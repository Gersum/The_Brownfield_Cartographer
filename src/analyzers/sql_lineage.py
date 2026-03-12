"""SQL lineage extraction using sqlglot.

Parses SQL files and dbt model files to extract the full table
dependency graph from SELECT/FROM/JOIN/WITH chains.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import sqlglot
from sqlglot import exp

from src.models.nodes import DatasetNode, StorageType, TransformationNode


class SQLLineageAnalyzer:
    """Extract table dependencies from SQL files using sqlglot."""

    # Dialects to try in order
    DIALECTS = ["postgres", "bigquery", "snowflake", "duckdb", None]

    def analyze_file(self, file_path: str | Path) -> list[TransformationNode]:
        """Analyze a SQL file and return transformation nodes.
        
        Each SQL statement that reads/writes tables produces a TransformationNode.
        """
        file_path = Path(file_path)
        try:
            content = file_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return []

        return self.analyze_sql(content, str(file_path))

    def analyze_sql(self, sql: str, source_file: str = "",
                    dialect: str | None = None) -> list[TransformationNode]:
        """Analyze raw SQL content and extract transformations."""
        # Strip dbt-specific Jinja
        cleaned = self._strip_jinja(sql)

        # Extract dbt ref() and source() calls before cleaning
        dbt_refs = self._extract_dbt_refs(sql)
        dbt_sources = self._extract_dbt_sources(sql)

        transformations = []

        # Try to parse with sqlglot
        statements = self._parse_sql(cleaned, dialect)

        for i, stmt in enumerate(statements):
            source_tables = set()
            target_tables = set()

            # Extract source tables (FROM, JOIN, subqueries)
            for table in stmt.find_all(exp.Table):
                table_name = self._get_table_name(table)
                if table_name:
                    source_tables.add(table_name)

            # Extract target tables (INSERT, CREATE, MERGE)
            if isinstance(stmt, (exp.Insert, exp.Create, exp.Merge)):
                target = stmt.find(exp.Table)
                if target:
                    target_name = self._get_table_name(target)
                    if target_name:
                        target_tables.add(target_name)
                        source_tables.discard(target_name)

            # For SELECT without INSERT/CREATE, the model name is the target
            if isinstance(stmt, exp.Select) and not target_tables:
                # In dbt, the filename IS the model name
                if source_file:
                    model_name = Path(source_file).stem
                    target_tables.add(model_name)

            # Add dbt refs as sources
            source_tables.update(dbt_refs)
            source_tables.update(dbt_sources)

            # Only create transformation if we found something
            if source_tables or target_tables:
                lines = cleaned[:stmt.meta.get("start", 0)].count("\n") if hasattr(stmt, "meta") else 0
                transformations.append(TransformationNode(
                    id=f"{source_file}:{i}",
                    source_datasets=sorted(source_tables),
                    target_datasets=sorted(target_tables),
                    transformation_type=self._infer_type(stmt, source_file),
                    source_file=source_file,
                    line_range=(max(1, lines), lines + cleaned.count("\n")),
                    sql_query_if_applicable=cleaned[:500] if len(cleaned) <= 500 else cleaned[:500] + "...",
                ))

        return transformations

    def extract_table_names(self, sql: str, dialect: str | None = None) -> dict[str, set[str]]:
        """Quick extraction of source and target tables from SQL.
        
        Returns {"sources": set, "targets": set}.
        """
        cleaned = self._strip_jinja(sql)
        sources = set()
        targets = set()

        # Add dbt refs/sources
        sources.update(self._extract_dbt_refs(sql))
        sources.update(self._extract_dbt_sources(sql))

        for stmt in self._parse_sql(cleaned, dialect):
            for table in stmt.find_all(exp.Table):
                name = self._get_table_name(table)
                if name:
                    sources.add(name)

            if isinstance(stmt, (exp.Insert, exp.Create, exp.Merge)):
                target = stmt.find(exp.Table)
                if target:
                    name = self._get_table_name(target)
                    if name:
                        targets.add(name)
                        sources.discard(name)

        return {"sources": sources, "targets": targets}

    def _parse_sql(self, sql: str, dialect: str | None = None) -> list:
        """Parse SQL, trying multiple dialects if needed."""
        if dialect:
            dialects = [dialect]
        else:
            dialects = self.DIALECTS

        for d in dialects:
            try:
                stmts = list(sqlglot.parse(sql, dialect=d, error_level=sqlglot.ErrorLevel.IGNORE))
                if stmts:
                    return [s for s in stmts if s is not None]
            except Exception:
                continue

        return []

    def _get_table_name(self, table_node: exp.Table) -> Optional[str]:
        """Extract fully qualified table name from a Table expression."""
        parts = []
        if table_node.catalog:
            parts.append(table_node.catalog)
        if table_node.db:
            parts.append(table_node.db)
        if table_node.name:
            parts.append(table_node.name)

        name = ".".join(parts) if parts else None

        # Skip CTEs and subqueries with generic names
        if name and name.lower() not in ("dual", "information_schema"):
            return name
        return None

    def _strip_jinja(self, sql: str) -> str:
        """Remove Jinja2 template syntax (dbt) from SQL."""
        # Replace {{ ref('xxx') }} with table name
        sql = re.sub(r"\{\{\s*ref\s*\(\s*['\"](\w+)['\"]\s*\)\s*\}\}", r"\1", sql)
        # Replace {{ source('schema', 'table') }} with table name
        sql = re.sub(r"\{\{\s*source\s*\(\s*['\"](\w+)['\"],\s*['\"](\w+)['\"]\s*\)\s*\}\}", r"\1.\2", sql)
        # Replace {{ config(...) }} with empty string
        sql = re.sub(r"\{\{.*?\}\}", "", sql, flags=re.DOTALL)
        # Replace {% ... %} blocks
        sql = re.sub(r"\{%.*?%\}", "", sql, flags=re.DOTALL)
        # Replace {# ... #} comments
        sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.DOTALL)
        return sql

    def _extract_dbt_refs(self, sql: str) -> set[str]:
        """Extract dbt ref() references from raw SQL (before Jinja stripping)."""
        refs = set()
        for match in re.finditer(r"ref\s*\(\s*['\"](\w+)['\"]\s*\)", sql):
            refs.add(match.group(1))
        return refs

    def _extract_dbt_sources(self, sql: str) -> set[str]:
        """Extract dbt source() references from raw SQL."""
        sources = set()
        for match in re.finditer(r"source\s*\(\s*['\"](\w+)['\"],\s*['\"](\w+)['\"]\s*\)", sql):
            sources.add(f"{match.group(1)}.{match.group(2)}")
        return sources

    def _infer_type(self, stmt, source_file: str) -> str:
        """Infer the transformation type."""
        if source_file.endswith(".sql"):
            if "models" in source_file:
                return "dbt_model"
            return "sql_query"
        return "sql_transformation"
