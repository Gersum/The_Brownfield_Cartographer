"""Archivist Agent — Living Context Maintainer.

Produces and maintains the system's outputs as living artifacts:
- SYSTEM_MAP.md (Mermaid diagrams)
- CODEBASE.md (Context injection file)
- onboarding_brief.md (FDE Day-One answers)
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from rich.console import Console

from src.graph.knowledge_graph import KnowledgeGraph
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

    def run(self) -> dict[str, Path]:
        """Generate all artifacts."""
        console.print(f"\n[bold green]📁 Archivist Agent[/bold green] — Generating artifacts in {self.output_dir}")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        artifacts = {}

        # 1. System Map (Mermaid)
        system_map_path = self.generate_system_map()
        artifacts["system_map"] = system_map_path

        # 2. Lineage Map (Mermaid)
        lineage_map_path = self.generate_lineage_map()
        artifacts["lineage_map"] = lineage_map_path

        # 3. CODEBASE.md (Stub for interim)
        # 4. onboarding_brief.md (Stub for interim)

        return artifacts

    def generate_system_map(self) -> Path:
        """Generate a Mermaid diagram of the module import graph."""
        nodes = self.module_graph.get_nodes_by_type("module")
        edges = self.module_graph.get_edges(EdgeType.IMPORTS)

        mermaid = ["graph TD"]
        
        # Add nodes with style based on PageRank or language
        pr = self.module_graph.pagerank()
        
        for node in nodes:
            node_id = node["id"]
            # Clean node ID for Mermaid (no dots or slashes in IDs)
            safe_id = node_id.replace("/", "_").replace(".", "_")
            label = f'"{node_id}"'
            
            # Simple PageRank-based sizing (Mermaid doesn't support well, but we can highlight)
            if pr.get(node_id, 0) > 0.15:
                mermaid.append(f'    {safe_id}(({label})):::critical')
            else:
                mermaid.append(f'    {safe_id}["{label}"]')

        for edge in edges:
            src = edge["source"].replace("/", "_").replace(".", "_")
            tgt = edge["target"].replace("/", "_").replace(".", "_")
            mermaid.append(f"    {src} --> {tgt}")

        mermaid.append("")
        mermaid.append("    classDef critical fill:#f96,stroke:#333,stroke-width:4px;")
        
        content = f"""# System Map: Module Dependencies

This map represents the structural skeleton of the codebase. Nodes highlighted in orange are identified as critical architectural hubs (high PageRank).

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
                safe_src = f"ds_{src.replace('/', '_').replace('.', '_')}"
                mermaid.append(f"    {safe_src} --> {safe_t_id}")
                
            # Connect transform to targets
            for tgt in t.get('target_datasets', []):
                safe_tgt = f"ds_{tgt.replace('/', '_').replace('.', '_')}"
                mermaid.append(f"    {safe_t_id} --> {safe_tgt}")

        mermaid.append("")
        mermaid.append("    classDef source fill:#dfd,stroke:#333,stroke-width:2px;")
        mermaid.append("    classDef transform fill:#f9f,stroke:#333,stroke-dasharray: 5 5;")
        
        content = f"""# Data Lineage Map

This map traces the flow of data from sources to output datasets.

```mermaid
{chr(10).join(mermaid)}
```
"""
        output_path = self.output_dir / "LINEAGE_MAP.md"
        output_path.write_text(content)
        console.print(f"  💧 Lineage map generated: {output_path}")
        return output_path
