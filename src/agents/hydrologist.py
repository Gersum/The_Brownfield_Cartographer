"""Hydrologist Agent — Data Flow & Lineage Analyst.

Constructs the data lineage DAG by analyzing data sources,
transformations, and sinks across Python, SQL, and YAML.
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from rich.console import Console

from src.analyzers.dag_config_parser import DAGConfigParser
from src.analyzers.sql_lineage import SQLLineageAnalyzer
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.edges import EdgeType
from src.models.nodes import DatasetNode, StorageType, TransformationNode

console = Console()


class HydrologistAgent:
    """Traces data flow and builds the lineage graph."""

    def __init__(self, repo_path: str | Path, module_graph: Optional[KnowledgeGraph] = None):
        self.repo_path = Path(repo_path).resolve()
        self.module_graph = module_graph
        self.lineage_graph = KnowledgeGraph(str(self.repo_path))
        self.sql_analyzer = SQLLineageAnalyzer()
        self.dag_parser = DAGConfigParser()
        self.trace_log: list[dict] = []

    def run(self) -> KnowledgeGraph:
        """Execute the full Hydrologist analysis pipeline."""
        console.print(f"\n[bold blue]💧 Hydrologist Agent[/bold blue] — Tracing data flows in {self.repo_path}")

        # Step 1: Analyze SQL files
        self._analyze_sql_files()

        # Step 2: Parse DAG/config files
        self._parse_configs()

        # Step 3: Analyze Python data operations
        self._analyze_python_data_ops()

        # Step 4: Report results
        summary = self.lineage_graph.summary()
        console.print(f"  ✅ Lineage graph: {summary['node_count']} nodes, {summary['edge_count']} edges")

        sources = self.lineage_graph.find_sources()
        sinks = self.lineage_graph.find_sinks()
        console.print(f"  📥 Sources (entry points): {len(sources)}")
        if sources[:5]:
            for s in sources[:5]:
                console.print(f"     → {s}")
        console.print(f"  📤 Sinks (outputs): {len(sinks)}")
        if sinks[:5]:
            for s in sinks[:5]:
                console.print(f"     → {s}")

        return self.lineage_graph

    def save(self, output_dir: str | Path) -> Path:
        """Save the lineage graph to JSON."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "lineage_graph.json"
        self.lineage_graph.save(output_path, graph_type="lineage_graph")
        console.print(f"  💾 Saved to {output_path}")
        return output_path

    def blast_radius(self, node_id: str) -> list[str]:
        """Get everything downstream of a node."""
        return self.lineage_graph.blast_radius(node_id)

    def find_sources(self) -> list[str]:
        """Find data source entry points."""
        return self.lineage_graph.find_sources()

    def find_sinks(self) -> list[str]:
        """Find data output endpoints."""
        return self.lineage_graph.find_sinks()

    def trace_upstream(self, node_id: str) -> list[str]:
        """Trace all upstream dependencies of a dataset."""
        return self.lineage_graph.upstream_trace(node_id)

    # ── Internal methods ────────────────────────────────────────────

    def _analyze_sql_files(self) -> None:
        """Find and analyze all SQL files."""
        sql_files = list(self.repo_path.rglob("*.sql"))
        sql_files = [f for f in sql_files if not self._is_skip(f)]
        console.print(f"  🗃️  Found {len(sql_files)} SQL files")

        for sql_file in sql_files:
            try:
                transformations = self.sql_analyzer.analyze_file(sql_file)
                rel_path = str(sql_file.relative_to(self.repo_path))

                for t in transformations:
                    # Add dataset nodes
                    for ds in t.source_datasets:
                        self._ensure_dataset(ds)
                    for ds in t.target_datasets:
                        self._ensure_dataset(ds)

                    # Add transformation node
                    self.lineage_graph.add_node(t)

                    # Add CONSUMES edges (transformation ← source datasets)
                    for ds in t.source_datasets:
                        self.lineage_graph.add_edge_simple(
                            ds, t.id,
                            edge_type=EdgeType.CONSUMES,
                            source_file=rel_path,
                            line_range=t.line_range,
                            transformation_type=t.transformation_type,
                        )

                    # Add PRODUCES edges (transformation → target datasets)
                    for ds in t.target_datasets:
                        self.lineage_graph.add_edge_simple(
                            t.id, ds,
                            edge_type=EdgeType.PRODUCES,
                            source_file=rel_path,
                            line_range=t.line_range,
                            transformation_type=t.transformation_type,
                        )

                self._log("analyze_sql", rel_path, f"found {len(transformations)} transformations")

            except Exception as e:
                self._log("analyze_sql", str(sql_file), f"error: {e}")

    def _parse_configs(self) -> None:
        """Parse dbt and Airflow configuration files."""
        try:
            config_results = self.dag_parser.parse_directory(self.repo_path)

            # Add datasets from configs
            for ds in config_results["datasets"]:
                self._ensure_dataset(ds.name, storage_type=ds.storage_type,
                                     is_source=ds.is_source_of_truth)
                self.lineage_graph.add_node(ds)

            # Add config edges
            for edge in config_results["config_edges"]:
                # Ensure nodes exist
                if edge.source not in self.lineage_graph.graph:
                    self.lineage_graph.graph.add_node(edge.source, node_type="config")
                if edge.target not in self.lineage_graph.graph:
                    self.lineage_graph.graph.add_node(edge.target, node_type="reference")
                self.lineage_graph.add_edge(edge)

            console.print(
                f"  ⚙️  Config parsing: {len(config_results['datasets'])} datasets, "
                f"{len(config_results['config_edges'])} config edges, "
                f"{len(config_results['dbt_models'])} dbt models"
            )

        except Exception as e:
            console.print(f"  ⚠️  Config parsing failed: {e}")

    def _analyze_python_data_ops(self) -> None:
        """Analyze Python files and Notebooks for data read/write operations."""
        py_files = list(self.repo_path.rglob("*.py"))
        py_files = [f for f in py_files if not self._is_skip(f)]

        nb_files = list(self.repo_path.rglob("*.ipynb"))
        nb_files = [f for f in nb_files if not self._is_skip(f)]

        data_ops_found = 0
        
        # Process Python files
        for py_file in py_files:
            try:
                content = py_file.read_text(encoding="utf-8", errors="replace")
                rel_path = str(py_file.relative_to(self.repo_path))
                ops = self._extract_python_data_ops(content, rel_path)
                data_ops_found += len(ops)
            except Exception:
                pass

        # Process Notebooks
        if nb_files:
            from src.analyzers.notebook_analyzer import NotebookAnalyzer
            nb_analyzer = NotebookAnalyzer()
            for nb_file in nb_files:
                try:
                    rel_path = str(nb_file.relative_to(self.repo_path))
                    content = nb_analyzer.analyze(nb_file)
                    if content:
                        ops = self._extract_python_data_ops(content, rel_path)
                        data_ops_found += len(ops)
                except Exception:
                    pass

        if data_ops_found > 0:
            console.print(f"  🐍 Found {data_ops_found} Python data operations")

    def _extract_python_data_ops(self, content: str, source_file: str) -> list[dict]:
        """Extract data read/write operations from Python code."""
        ops = []

        # Pandas read operations
        read_patterns = [
            (r"pd\.read_csv\s*\(\s*['\"]([^'\"]+)['\"]", "file", "read"),
            (r"pd\.read_excel\s*\(\s*['\"]([^'\"]+)['\"]", "file", "read"),
            (r"pd\.read_sql\s*\(\s*['\"]([^'\"]+)['\"]", "table", "read"),
            (r"pd\.read_parquet\s*\(\s*['\"]([^'\"]+)['\"]", "file", "read"),
            (r"pd\.read_json\s*\(\s*['\"]([^'\"]+)['\"]", "file", "read"),
        ]

        # Write operations
        write_patterns = [
            (r"\.to_csv\s*\(\s*['\"]([^'\"]+)['\"]", "file", "write"),
            (r"\.to_excel\s*\(\s*['\"]([^'\"]+)['\"]", "file", "write"),
            (r"\.to_sql\s*\(\s*['\"]([^'\"]+)['\"]", "table", "write"),
            (r"\.to_parquet\s*\(\s*['\"]([^'\"]+)['\"]", "file", "write"),
            (r"\.to_json\s*\(\s*['\"]([^'\"]+)['\"]", "file", "write"),
        ]

        # SQLAlchemy / DB operations
        db_patterns = [
            (r"execute\s*\(\s*['\"]([^'\"]*(?:SELECT|INSERT|UPDATE|DELETE)[^'\"]*)['\"]",
             "table", "query"),
        ]

        # PySpark operations
        spark_patterns = [
            (r"spark\.read\.(?:csv|parquet|json|table)\s*\(\s*['\"]([^'\"]+)['\"]",
             "table", "read"),
            (r"\.write\.(?:csv|parquet|json|mode\([^)]+\)\.)?saveAsTable\s*\(\s*['\"]([^'\"]+)['\"]",
             "table", "write"),
        ]

        all_patterns = read_patterns + write_patterns + db_patterns + spark_patterns

        # Find dynamic references (variables/f-strings) that we can't resolve
        # e.g. pd.read_csv(filename) or pd.read_csv(f"data_{date}.csv")
        dynamic_pattern = r"(?:pd\.read_|spark\.read\.|\.to_)(?:csv|excel|sql|parquet|json|table)\s*\(\s*([^'\"].*?)\s*\)"

        for pattern, storage, direction in all_patterns:
            for match in re.finditer(pattern, content, re.IGNORECASE):
                dataset_name = match.group(1)
                line_no = content[:match.start()].count("\n") + 1

                storage_type = StorageType.FILE if storage == "file" else StorageType.TABLE
                self._ensure_dataset(dataset_name, storage_type=storage_type)

                transform_id = f"{source_file}:{line_no}:{direction}"
                t = TransformationNode(
                    id=transform_id,
                    source_datasets=[dataset_name] if direction == "read" else [],
                    target_datasets=[dataset_name] if direction == "write" else [],
                    transformation_type=f"python_{direction}",
                    source_file=source_file,
                    line_range=(line_no, line_no),
                )

                self.lineage_graph.add_node(t)
                self.lineage_graph.add_edge_simple(
                    dataset_name if direction == "read" else transform_id,
                    transform_id if direction == "read" else dataset_name,
                    edge_type=EdgeType.CONSUMES if direction == "read" else EdgeType.PRODUCES,
                    source_file=source_file,
                    line_range=(line_no, line_no),
                    transformation_type=f"python_{direction}",
                )

                ops.append({
                    "dataset": dataset_name,
                    "direction": direction,
                    "file": source_file,
                    "line": line_no,
                })

        for match in re.finditer(dynamic_pattern, content, re.IGNORECASE):
            expr = match.group(1)
            line_no = content[:match.start()].count("\n") + 1
            self._log("python_lineage", f"{source_file}:{line_no}", 
                      f"dynamic reference, cannot resolve: {expr}")

        return ops

    def _ensure_dataset(self, name: str, storage_type: StorageType = StorageType.TABLE,
                        is_source: bool = False) -> None:
        """Ensure a dataset node exists in the lineage graph."""
        if name not in self.lineage_graph.graph:
            ds = DatasetNode(
                id=name,
                name=name,
                storage_type=storage_type,
                is_source_of_truth=is_source,
            )
            self.lineage_graph.add_node(ds)

    def _is_skip(self, path: Path) -> bool:
        """Check if a path should be skipped."""
        skip_parts = {".git", "__pycache__", "node_modules", ".venv", "venv",
                      ".cartography", ".tox", ".mypy_cache", ".eggs"}
        return any(part in skip_parts for part in path.parts)

    def _log(self, action: str, target: str, result: str) -> None:
        """Log an analysis action for tracing."""
        self.trace_log.append({
            "timestamp": datetime.now().isoformat(),
            "agent": "hydrologist",
            "action": action,
            "target": target,
            "result": result,
            "analysis_method": "static",
            "evidence_sources": [target],
            "confidence": 0.9,
        })
