"""Archivist Agent — Living Context Maintainer.

Produces and maintains the system's outputs as living artifacts:
- SYSTEM_MAP.md (Mermaid diagrams)
- LINEAGE_MAP.md (Mermaid diagrams)
- dashboard.html (Interactive Cytoscape.js dashboard)
- CODEBASE.md (Context injection file)
- onboarding_brief.md (FDE Day-One answers)
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import networkx as nx
from rich.console import Console

from src.graph.knowledge_graph import KnowledgeGraph
from src.graph.visualizer import Visualizer
from src.models.edges import EdgeType

console = Console()


class ArchivistAgent:
    """Maintains codebase artifacts and visualizations."""

    def __init__(self, repo_path: str | Path, 
                 module_graph: KnowledgeGraph, 
                 lineage_graph: KnowledgeGraph):
        self.repo_path = Path(repo_path).resolve()
        self.module_graph = module_graph
        self.lineage_graph = lineage_graph
        self.output_dir = self.repo_path / ".cartography"
        self.trace_log: list[dict] = []

    def run(self, extra_context: Optional[dict] = None) -> dict[str, Path]:
        """Generate all artifacts."""
        console.print(f"\n[bold green]📁 Archivist Agent[/bold green] — Generating artifacts in {self.output_dir}")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        artifacts = {}

        # 1. System Map (Mermaid)
        system_map_path = self.generate_system_map()
        artifacts["system_map"] = system_map_path
        self._log("generate_artifact", str(system_map_path), "success", "static", ["module_graph.json"], 0.95)

        # 2. Lineage Map (Mermaid)
        lineage_map_path = self.generate_lineage_map()
        artifacts["lineage_map"] = lineage_map_path
        self._log("generate_artifact", str(lineage_map_path), "success", "static", ["lineage_graph.json"], 0.95)

        # 3. ONBOARDING_BRIEF.md
        if extra_context and "onboarding_brief" in extra_context:
            brief_path = self.output_dir / "onboarding_brief.md"
            brief_path.write_text(f"# FDE Onboarding Brief\n\n{extra_context['onboarding_brief']}")
            artifacts["onboarding_brief"] = brief_path
            console.print(f"  📋 Onboarding brief generated: {brief_path}")
            self._log("generate_artifact", str(brief_path), "success", "llm", ["semanticist.answer_questions"], 0.8)

        # 4. CODEBASE.md (Living Context)
        codebase_md_path = self.generate_codebase_md()
        artifacts["codebase_md"] = codebase_md_path
        self._log("generate_artifact", str(codebase_md_path), "success", "static+llm", ["module_graph.json", "lineage_graph.json"], 0.9)

        # 5. Interactive Dashboard (Cytoscape)
        dashboard_path = self.generate_dashboard()
        artifacts["dashboard"] = dashboard_path
        self._log("generate_artifact", str(dashboard_path), "success", "static", ["module_graph.json", "lineage_graph.json"], 0.92)

        # 6. Premium Visualizations (Pyvis & Matplotlib)
        premium_viz = self.generate_premium_visualizations()
        artifacts.update(premium_viz)

        return artifacts

    def generate_premium_visualizations(self) -> dict[str, Path]:
        """Generate premium Python-based visualizations."""
        viz = Visualizer(self.module_graph.graph, self.output_dir)
        
        premium_artifacts = {}
        
        # Interactive Network (Pyvis)
        net_path = viz.generate_interactive_network("network_map.html")
        if net_path:
            premium_artifacts["interactive_network"] = Path(net_path)
            
        # Static Architecture (Matplotlib)
        static_path = viz.generate_static_map("system_architecture.png")
        if static_path:
            premium_artifacts["static_architecture"] = Path(static_path)
            
        return premium_artifacts

    def generate_codebase_md(self) -> Path:
        """Generate a living context file for AI agents."""
        hubs = self.module_graph.pagerank()
        top_hubs = sorted(hubs.items(), key=lambda x: x[1], reverse=True)[:5]

        lineage_sources = self.lineage_graph.find_sources()[:10]
        lineage_sinks = self.lineage_graph.find_sinks()[:10]

        # Critical path approximation: top hub to nearest sinks via shortest paths when possible.
        critical_paths: list[str] = []
        Gm = self.module_graph.graph
        for hub, _ in top_hubs[:3]:
            try:
                for sink in lineage_sinks[:3]:
                    if hub in Gm and sink in Gm:
                        try:
                            path = nx.shortest_path(Gm, source=hub, target=sink)
                            if path and len(path) > 1:
                                critical_paths.append(" -> ".join(path))
                        except Exception:
                            continue
            except Exception:
                continue

        # Known debt from circular dependencies + doc drift
        circular = self.module_graph.strongly_connected_components()[:5]
        drifted = [
            n["id"] for n in self.module_graph.get_nodes_by_type("module")
            if n.get("documentation_drift")
        ][:10]

        # High velocity files
        velocity_nodes = sorted(
            self.module_graph.get_nodes_by_type("module"),
            key=lambda n: n.get("change_velocity_30d", 0),
            reverse=True,
        )[:10]
        
        lines = [
            "# CODEBASE.md — System Context",
            "",
            "## Architecture Overview",
            "This repository is analyzed by The Brownfield Cartographer. It contains a mix of Python and SQL.",
            "",
            "## Critical Architectural Hubs",
        ]
        
        for path, pr in top_hubs:
            node = self.module_graph.get_node(path)
            purpose = node.get("purpose_statement", "No purpose statement generated.")
            lines.append(f"- **{path}** (PageRank: {pr:.4f})")
            lines.append(f"  - *Purpose*: {purpose}")

        lines.append("")
        lines.append("## Critical Path")
        if critical_paths:
            for path in critical_paths[:5]:
                lines.append(f"- {path}")
        else:
            lines.append("- Critical path is approximated by top hubs and lineage hotspots; no direct merged shortest-path found.")
            
        lines.append("")
        lines.append("## Data Sources & Sinks")
        sources = lineage_sources
        sinks = lineage_sinks
        
        lines.append("### Entry Points")
        for s in sources:
            lines.append(f"- {s}")
            
        lines.append("")
        lines.append("### Terminal Outputs")
        for s in sinks:
            lines.append(f"- {s}")

        lines.append("")
        lines.append("## Known Debt")
        if circular:
            lines.append("### Circular Dependencies")
            for scc in circular:
                lines.append(f"- {' <-> '.join(scc)}")
        if drifted:
            lines.append("### Documentation Drift Hotspots")
            for mod in drifted:
                lines.append(f"- {mod}")
        if not circular and not drifted:
            lines.append("- No major structural debt detected in this snapshot.")

        lines.append("")
        lines.append("## High-Velocity Files")
        if velocity_nodes:
            for n in velocity_nodes:
                lines.append(f"- {n.get('id')} (change_velocity_30d={n.get('change_velocity_30d', 0)})")
        else:
            lines.append("- No velocity data available.")

        lines.append("")
        lines.append("## Module Purpose Index")
        for n in self.module_graph.get_nodes_by_type("module")[:200]:
            purpose = n.get("purpose_statement") or "Purpose not generated"
            lines.append(f"- {n.get('id')}: {purpose}")
            
        output_path = self.output_dir / "CODEBASE.md"
        output_path.write_text("\n".join(lines))
        console.print(f"  📝 CODEBASE.md generated: {output_path}")
        return output_path

    def _log(
        self,
        action: str,
        target: str,
        result: str,
        analysis_method: str,
        evidence_sources: list[str],
        confidence: float,
    ) -> None:
        self.trace_log.append({
            "timestamp": datetime.now().isoformat(),
            "agent": "archivist",
            "action": action,
            "target": target,
            "result": result,
            "analysis_method": analysis_method,
            "evidence_sources": evidence_sources,
            "confidence": confidence,
        })

    def generate_system_map(self) -> Path:
        """Generate a Mermaid diagram of the module import graph."""
        nodes = self.module_graph.get_nodes_by_type("module")
        pr = self.module_graph.pagerank()

        # Focus on the most important modules for readability
        MAX_NODES = 80
        if len(nodes) > MAX_NODES:
            sorted_nodes = sorted(
                nodes,
                key=lambda n: pr.get(n["id"], 0),
                reverse=True,
            )
            selected_ids = {n["id"] for n in sorted_nodes[:MAX_NODES]}
        else:
            selected_ids = {n["id"] for n in nodes}

        all_edges = self.module_graph.get_edges(EdgeType.IMPORTS)
        edges = [
            e for e in all_edges
            if e["source"] in selected_ids and e["target"] in selected_ids
        ]

        mermaid = ["graph TD"]

        # Add nodes with style based on PageRank or language
        for node in nodes:
            node_id = node["id"]
            if node_id not in selected_ids:
                continue
            # Clean node ID for Mermaid (no dots or slashes in IDs)
            safe_id = node_id.replace("/", "_").replace(".", "_")
            short_label = Path(node_id).name

            # Simple PageRank-based sizing (Mermaid doesn't support well, but we can highlight)
            if pr.get(node_id, 0) > 0.15:
                mermaid.append(f'    {safe_id}(({short_label})):::critical')
            else:
                mermaid.append(f'    {safe_id}["{short_label}"]')

        for edge in edges:
            src = edge["source"].replace("/", "_").replace(".", "_")
            tgt = edge["target"].replace("/", "_").replace(".", "_")
            mermaid.append(f"    {src} --> {tgt}")

        mermaid.append("")
        mermaid.append("    classDef critical fill:#f96,stroke:#333,stroke-width:4px;")

        total_nodes = len(nodes)
        visible_nodes = len(selected_ids)
        coverage_note = ""
        if visible_nodes < total_nodes:
            coverage_note = (
                f"\nThis view shows the top {visible_nodes} of {total_nodes} "
                "modules by structural importance (PageRank)."
            )

        content = f"""# System Map: Module Dependencies

This map represents the structural skeleton of the codebase. Nodes highlighted in orange are identified as critical architectural hubs (high PageRank).
{coverage_note}

```mermaid
{chr(10).join(mermaid)}
```
"""
        output_path = self.output_dir / "SYSTEM_MAP.md"
        output_path.write_text(content)
        console.print(f"  🗺️  System map generated: {output_path}")
        return output_path

    def generate_lineage_map(self) -> Path:
        """Generate a Mermaid diagram of the data lineage graph."""
        # Focus on datasets and transformations
        datasets = self.lineage_graph.get_nodes_by_type("dataset")
        transformations = self.lineage_graph.get_nodes_by_type("transformation")

        # For large graphs, focus on key sources/sinks and their neighborhoods
        G = self.lineage_graph.graph
        all_node_ids = set(G.nodes())
        important_nodes: set[str] = set()

        sources = self.lineage_graph.find_sources()[:20]
        sinks = self.lineage_graph.find_sinks()[:20]
        important_nodes.update(sources)
        important_nodes.update(sinks)

        for nid in list(important_nodes):
            for pred in G.predecessors(nid):
                important_nodes.add(pred)
            for succ in G.successors(nid):
                important_nodes.add(succ)

        MAX_NODES = 150
        if not important_nodes or len(important_nodes) > MAX_NODES:
            # Fallback: truncate to a reasonable subset
            important_nodes = set(list(all_node_ids)[:MAX_NODES])

        datasets = [ds for ds in datasets if ds["id"] in important_nodes]
        transformations = [t for t in transformations if t["id"] in important_nodes]
        dataset_ids = {ds["id"] for ds in datasets}

        mermaid = ["graph LR"]

        # Add datasets
        for ds in datasets:
            safe_id = f"ds_{ds['id'].replace('/', '_').replace('.', '_')}"
            label = ds['name']
            if ds.get('is_source_of_truth'):
                mermaid.append(f'    {safe_id}[("{label}")]:::source')
            else:
                mermaid.append(f'    {safe_id}["{label}"]')
                
        # Add transformations and edges
        for t in transformations:
            safe_t_id = f"tx_{t['id'].replace(':', '_').replace('/', '_').replace('.', '_')}"
            label = t.get('transformation_type', 'transform')
            mermaid.append(f'    {safe_t_id}{{{{"{label}"}}}}:::transform')
            
            # Connect sources to transform
            for src in t.get('source_datasets', []):
                if src not in dataset_ids:
                    continue
                safe_src = f"ds_{src.replace('/', '_').replace('.', '_')}"
                mermaid.append(f"    {safe_src} --> {safe_t_id}")
                
            # Connect transform to targets
            for tgt in t.get('target_datasets', []):
                if tgt not in dataset_ids:
                    continue
                safe_tgt = f"ds_{tgt.replace('/', '_').replace('.', '_')}"
                mermaid.append(f"    {safe_t_id} --> {safe_tgt}")

        mermaid.append("")
        mermaid.append("    classDef source fill:#dfd,stroke:#333,stroke-width:2px;")
        mermaid.append("    classDef transform fill:#f9f,stroke:#333,stroke-dasharray: 5 5;")

        total_nodes = len(self.lineage_graph.graph.nodes())
        visible_nodes = len(datasets) + len(transformations)
        coverage_note = ""
        if visible_nodes < total_nodes:
            coverage_note = (
                f"\nThis view focuses on key sources/sinks and nearby nodes "
                f"({visible_nodes} of {total_nodes} total lineage nodes)."
            )

        content = f"""# Data Lineage Map

This map traces the flow of data from sources to output datasets.
{coverage_note}

```mermaid
{chr(10).join(mermaid)}
```
"""
        output_path = self.output_dir / "LINEAGE_MAP.md"
        output_path.write_text(content)
        console.print(f"  💧 Lineage map generated: {output_path}")
        return output_path

    def generate_dashboard(self) -> Path:
        """Generate a standalone interactive web dashboard."""
        template_path = Path(__file__).parent.parent / "dashboard" / "template.html"
        if not template_path.exists():
            console.print("  ⚠️  Dashboard template not found. Skipping.")
            return self.output_dir / "dashboard_skipped"

        template = template_path.read_text(encoding="utf-8")
        
        # Prepare data
        module_data = self.module_graph.to_json()
        lineage_data = self.lineage_graph.to_json()
        
        # Inject
        html = template.replace("__MODULE_DATA__", json.dumps(module_data))
        html = html.replace("__LINEAGE_DATA__", json.dumps(lineage_data))
        
        output_path = self.output_dir / "dashboard.html"
        output_path.write_text(html, encoding="utf-8")
        console.print(f"  ✨ Interactive dashboard generated: {output_path}")
        return output_path
