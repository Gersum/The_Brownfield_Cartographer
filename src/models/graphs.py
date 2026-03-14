"""Graph model wrappers for serialization."""

from __future__ import annotations

from pydantic import BaseModel, Field


class GraphMetadata(BaseModel):
    """Metadata about a serialized graph."""
    repo_path: str = ""
    analysis_timestamp: str = ""
    node_count: int = 0
    edge_count: int = 0
    graph_type: str = ""  # "module_graph" or "lineage_graph"
    git_commit: str = ""
