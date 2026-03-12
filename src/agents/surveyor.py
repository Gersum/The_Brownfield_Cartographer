"""Surveyor Agent — Static Structure Analyst.

Performs deep static analysis of the codebase using tree-sitter.
Builds the structural skeleton: module graph, PageRank, git velocity,
dead code candidates.
"""

from __future__ import annotations

import json
import re
import subprocess
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from src.analyzers.tree_sitter_analyzer import (
    TreeSitterAnalyzer,
    discover_files,
    resolve_python_import,
)
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.edges import EdgeType
from src.models.nodes import Language, ModuleNode

console = Console()


class SurveyorAgent:
    """Surveys the codebase to build the structural module graph."""

    def __init__(self, repo_path: str | Path):
        self.repo_path = Path(repo_path).resolve()
        self.analyzer = TreeSitterAnalyzer()
        self.graph = KnowledgeGraph(str(self.repo_path))
        self.modules: dict[str, ModuleNode] = {}
        self.trace_log: list[dict] = []

    def run(self) -> KnowledgeGraph:
        """Execute the full Surveyor analysis pipeline."""
        console.print(f"\n[bold cyan]🔭 Surveyor Agent[/bold cyan] — Analyzing {self.repo_path}")

        # Step 1: Discover and analyze files
        self._analyze_all_files()

        # Step 2: Build module import graph
        self._build_import_graph()

        # Step 3: Extract git velocity
        self._extract_git_velocity()

        # Step 4: Run PageRank
        self._run_pagerank()

        # Step 5: Detect dead code candidates
        self._detect_dead_code()

        # Step 6: Detect circular dependencies
        sccs = self.graph.strongly_connected_components()
        if sccs:
            console.print(f"  ⚠️  Found {len(sccs)} circular dependency group(s)")
            for scc in sccs:
                console.print(f"     → {', '.join(scc)}")

        summary = self.graph.summary()
        console.print(f"  ✅ Module graph: {summary['node_count']} nodes, {summary['edge_count']} edges")

        return self.graph

    def save(self, output_dir: str | Path) -> Path:
        """Save the module graph to JSON."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "module_graph.json"
        self.graph.save(output_path, graph_type="module_graph")
        console.print(f"  💾 Saved to {output_path}")
        return output_path

    # ── Internal methods ────────────────────────────────────────────

    def _analyze_all_files(self) -> None:
        """Discover and analyze all source files."""
        files = discover_files(self.repo_path)
        console.print(f"  📂 Discovered {len(files)} source files")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("  Analyzing files...", total=len(files))

            for fpath in files:
                try:
                    module = self.analyzer.analyze_module(fpath, self.repo_path)
                    if module:
                        self.modules[module.id] = module
                        self.graph.add_node(module)
                        self._log("analyze_module", module.id, "success")
                except Exception as e:
                    rel = str(fpath.relative_to(self.repo_path))
                    self._log("analyze_module", rel, f"error: {e}")
                progress.advance(task)

        console.print(f"  🧩 Analyzed {len(self.modules)} modules")

    def _build_import_graph(self) -> None:
        """Build edges between modules based on import relationships."""
        edge_count = 0
        import_counts: Counter = Counter()

        # Python imports
        for mod_id, module in self.modules.items():
            if module.language != Language.PYTHON:
                continue

            for imp in module.imports:
                resolved = resolve_python_import(imp, mod_id, self.repo_path)
                if resolved and resolved in self.modules:
                    import_counts[(mod_id, resolved)] += 1

        for (src, tgt), count in import_counts.items():
            self.graph.add_edge_simple(
                src, tgt,
                edge_type=EdgeType.IMPORTS,
                weight=float(count),
            )
            edge_count += 1

        # dbt SQL ref() edges — resolve model references
        sql_ref_count = self._build_sql_ref_edges()
        edge_count += sql_ref_count

        console.print(f"  🔗 Built {edge_count} import edges")

    def _build_sql_ref_edges(self) -> int:
        """Build edges between SQL files based on dbt ref() calls."""
        # Build a map of model names to their file paths
        model_to_path: dict[str, str] = {}
        for mod_id, module in self.modules.items():
            if module.language == Language.SQL:
                model_name = Path(mod_id).stem
                model_to_path[model_name] = mod_id

        edge_count = 0
        for mod_id, module in self.modules.items():
            if module.language != Language.SQL:
                continue
            try:
                content = (self.repo_path / mod_id).read_text(encoding="utf-8", errors="replace")
                # Extract ref('model_name') calls
                refs = re.findall(r"ref\s*\(\s*['\"](\w+)['\"]\s*\)", content)
                for ref_name in refs:
                    if ref_name in model_to_path:
                        target_path = model_to_path[ref_name]
                        if target_path != mod_id:
                            self.graph.add_edge_simple(
                                mod_id, target_path,
                                edge_type=EdgeType.IMPORTS,
                                weight=1.0,
                                metadata={"ref_type": "dbt_ref", "ref_name": ref_name},
                            )
                            edge_count += 1
            except Exception:
                pass

        return edge_count

    def _extract_git_velocity(self) -> None:
        """Extract git change frequency for each file."""
        try:
            result = subprocess.run(
                ["git", "log", "--format=", "--name-only", "--since=30.days.ago"],
                capture_output=True, text=True, cwd=self.repo_path,
                timeout=30,
            )
            if result.returncode != 0:
                console.print("  ⚠️  Git velocity: not a git repo or no history")
                return

            file_counts: Counter = Counter()
            for line in result.stdout.strip().splitlines():
                line = line.strip()
                if line:
                    file_counts[line] += 1

            updated = 0
            for mod_id, module in self.modules.items():
                velocity = file_counts.get(mod_id, 0)
                if velocity > 0:
                    module.change_velocity_30d = velocity
                    
                    # Fetch recent commit messages for this file (limit to last 5)
                    try:
                        commit_result = subprocess.run(
                            ["git", "log", "-n", "5", "--format=%s", "--", mod_id],
                            cwd=self.repo_path,
                            capture_output=True,
                            text=True,
                            check=True
                        )
                        module.commit_summaries = [s.strip() for s in commit_result.stdout.strip().splitlines() if s.strip()]
                    except Exception:
                        module.commit_summaries = []

                    # Update the graph node data
                    if mod_id in self.graph.graph:
                        self.graph.graph.nodes[mod_id]["change_velocity_30d"] = velocity
                        self.graph.graph.nodes[mod_id]["commit_summaries"] = module.commit_summaries
                    updated += 1

            # Identify high-velocity core (Pareto 80/20)
            sorted_files = file_counts.most_common()
            total_changes = sum(file_counts.values())
            
            if total_changes > 0:
                running_sum = 0
                core_files = []
                for fpath, count in sorted_files:
                    running_sum += count
                    core_files.append(fpath)
                    if running_sum >= 0.8 * total_changes:
                        break
                
                core_count = len(core_files)
                total_files = len([f for f in self.repo_path.rglob("*") if f.is_file()])
                core_pct = (core_count / total_files * 100) if total_files > 0 else 0
                
                console.print(f"  📊 Git velocity: {updated} files with recent changes")
                console.print(f"     🔥 High-velocity core: {core_count} files ({core_pct:.1f}%) responsible for 80% of changes")
            else:
                console.print(f"  📊 Git velocity: {updated} files with recent changes")

        except (subprocess.TimeoutExpired, FileNotFoundError):
            console.print("  ⚠️  Git velocity: git not available")

    def _run_pagerank(self) -> None:
        """Run PageRank to identify architectural hubs."""
        if self.graph.graph.number_of_nodes() == 0:
            return

        try:
            pr = self.graph.pagerank()
            if pr:
                sorted_pr = sorted(pr.items(), key=lambda x: x[1], reverse=True)
                top = sorted_pr[:5]
                console.print("  🏆 Top 5 critical modules (PageRank):")
                for path, score in top:
                    console.print(f"     {score:.4f} — {path}")
        except Exception as e:
            console.print(f"  ⚠️  PageRank failed: {e}")

    def _detect_dead_code(self) -> None:
        """Identify modules that are never imported (dead code candidates)."""
        # Get all target nodes from IMPORTS edges
        imported_modules = set()
        for u, v, data in self.graph.graph.edges(data=True):
            if data.get("edge_type") == EdgeType.IMPORTS.value:
                imported_modules.add(v)

        dead_candidates = []
        for mod_id, module in self.modules.items():
            if module.language != Language.PYTHON:
                continue
            if mod_id not in imported_modules and not mod_id.endswith("__init__.py"):
                if module.public_functions or module.public_classes:
                    module.is_dead_code_candidate = True
                    if mod_id in self.graph.graph:
                        self.graph.graph.nodes[mod_id]["is_dead_code_candidate"] = True
                    dead_candidates.append(mod_id)

        if dead_candidates:
            console.print(f"  💀 {len(dead_candidates)} potential dead code files")

    def _log(self, action: str, target: str, result: str) -> None:
        """Log an analysis action for tracing."""
        self.trace_log.append({
            "timestamp": datetime.now().isoformat(),
            "agent": "surveyor",
            "action": action,
            "target": target,
            "result": result,
        })
