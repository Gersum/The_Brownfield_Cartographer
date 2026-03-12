"""Pydantic schemas for knowledge graph edge types."""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class EdgeType(str, Enum):
    IMPORTS = "IMPORTS"
    PRODUCES = "PRODUCES"
    CONSUMES = "CONSUMES"
    CALLS = "CALLS"
    CONFIGURES = "CONFIGURES"


class GraphEdge(BaseModel):
    """Represents an edge in the knowledge graph."""
    source: str = Field(..., description="Source node ID")
    target: str = Field(..., description="Target node ID")
    edge_type: EdgeType
    weight: float = Field(1.0, description="Edge weight (e.g. import count)")
    metadata: dict = Field(default_factory=dict, description="Additional edge metadata")

    # Specific metadata fields
    transformation_type: Optional[str] = None
    source_file: Optional[str] = None
    line_range: Optional[tuple[int, int]] = None
