"""Pydantic schemas for knowledge graph node types."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class StorageType(str, Enum):
    TABLE = "table"
    FILE = "file"
    STREAM = "stream"
    API = "api"


class Language(str, Enum):
    PYTHON = "python"
    SQL = "sql"
    YAML = "yaml"
    JAVASCRIPT = "javascript"
    TYPESCRIPT = "typescript"
    UNKNOWN = "unknown"


class ModuleNode(BaseModel):
    """Represents a code module (file) in the codebase."""
    id: str = Field(..., description="Unique identifier, typically the relative file path")
    path: str = Field(..., description="Relative path from repo root")
    language: Language = Language.UNKNOWN
    purpose_statement: Optional[str] = Field(None, description="LLM-generated purpose")
    domain_cluster: Optional[str] = Field(None, description="Inferred domain name")
    complexity_score: float = Field(0.0, description="Cyclomatic complexity estimate")
    lines_of_code: int = Field(0, description="Total lines of code")
    comment_ratio: float = Field(0.0, description="Ratio of comment lines to total lines")
    change_velocity_30d: int = Field(0, description="Number of commits touching this file in last 30 days")
    commit_summaries: list[str] = Field(default_factory=list, description="Recent commit messages for this file")
    is_dead_code_candidate: bool = Field(False, description="Exported but never imported")
    last_modified: Optional[datetime] = None
    imports: list[str] = Field(default_factory=list, description="List of resolved import paths")
    public_functions: list[str] = Field(default_factory=list, description="Public function names")
    public_classes: list[str] = Field(default_factory=list, description="Public class names")
    node_type: str = "module"


class DatasetNode(BaseModel):
    """Represents a data table, file, stream, or API endpoint."""
    id: str = Field(..., description="Unique identifier for the dataset")
    name: str = Field(..., description="Dataset name (table name, file path, etc.)")
    storage_type: StorageType = StorageType.TABLE
    schema_snapshot: Optional[dict] = None
    freshness_sla: Optional[str] = None
    owner: Optional[str] = None
    is_source_of_truth: bool = False
    node_type: str = "dataset"


class FunctionNode(BaseModel):
    """Represents a function or method in the codebase."""
    id: str = Field(..., description="Unique identifier (qualified name)")
    qualified_name: str = Field(..., description="Fully qualified name: module.class.function")
    parent_module: str = Field(..., description="Module path this function belongs to")
    signature: str = Field("", description="Function signature")
    purpose_statement: Optional[str] = None
    call_count_within_repo: int = Field(0, description="How often this function is called in the repo")
    is_public_api: bool = Field(True, description="Whether this is a public function")
    node_type: str = "function"


class TransformationNode(BaseModel):
    """Represents a data transformation (SQL query, Python transform, etc.)."""
    id: str = Field(..., description="Unique identifier for the transformation")
    source_datasets: list[str] = Field(default_factory=list, description="Input dataset IDs")
    target_datasets: list[str] = Field(default_factory=list, description="Output dataset IDs")
    transformation_type: str = Field("unknown", description="e.g. sql_query, python_transform, dbt_model")
    source_file: str = Field("", description="File where this transformation is defined")
    line_range: tuple[int, int] = Field((0, 0), description="Start and end line numbers")
    sql_query_if_applicable: Optional[str] = None
    node_type: str = "transformation"
