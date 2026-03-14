"""Airflow DAG and dbt YAML configuration parser.

Extracts pipeline topology and configuration dependencies from:
- Airflow DAG Python files
- dbt schema.yml / sources.yml
- dbt project.yml
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any, Optional

import yaml

from src.models.edges import EdgeType, GraphEdge
from src.models.nodes import DatasetNode, ModuleNode, StorageType, TransformationNode


class DAGConfigParser:
    """Parse Airflow DAGs and dbt configs to extract pipeline topology."""

    def parse_directory(self, repo_root: str | Path) -> dict:
        """Parse all config files in a repo and return structured results.
        
        Returns dict with keys:
        - "datasets": list of DatasetNode
        - "transformations": list of TransformationNode
        - "config_edges": list of GraphEdge
        - "dbt_models": dict of model_name → metadata
        """
        repo_root = Path(repo_root)
        results = {
            "datasets": [],
            "transformations": [],
            "config_edges": [],
            "dbt_models": {},
        }

        # Find and parse dbt project files
        for yml_file in repo_root.rglob("*.yml"):
            if self._is_skip_path(yml_file):
                continue
            try:
                self._parse_yaml_file(yml_file, repo_root, results)
            except Exception as e:
                print(f"[WARN] Failed to parse {yml_file}: {e}")

        for yaml_file in repo_root.rglob("*.yaml"):
            if self._is_skip_path(yaml_file):
                continue
            try:
                self._parse_yaml_file(yaml_file, repo_root, results)
            except Exception as e:
                print(f"[WARN] Failed to parse {yaml_file}: {e}")

        # Find and parse Airflow DAG files
        for py_file in repo_root.rglob("*.py"):
            if self._is_skip_path(py_file):
                continue
            try:
                content = py_file.read_text(encoding="utf-8", errors="replace")
                if self._is_airflow_dag(content):
                    self._parse_airflow_dag(py_file, repo_root, content, results)
            except Exception as e:
                pass  # Skip unparseable files

        return results

    def _is_skip_path(self, path: Path) -> bool:
        """Check if a path should be skipped."""
        skip_parts = {".git", "__pycache__", "node_modules", ".venv", "venv",
                      ".cartography", ".tox", ".mypy_cache"}
        return any(part in skip_parts for part in path.parts)

    def _parse_yaml_file(self, yml_path: Path, repo_root: Path, results: dict) -> None:
        """Parse a YAML file for dbt or Airflow configuration."""
        content = yml_path.read_text(encoding="utf-8", errors="replace")
        try:
            data = yaml.safe_load(content)
        except yaml.YAMLError:
            return

        if not isinstance(data, dict):
            return

        rel_path = str(yml_path.relative_to(repo_root))

        # dbt schema.yml — has 'models' key
        if "models" in data:
            self._parse_dbt_schema(data, rel_path, results)

        # dbt sources.yml — has 'sources' key
        if "sources" in data:
            self._parse_dbt_sources(data, rel_path, results)

        # dbt project.yml — has 'name' and 'version' and 'profile'
        if "name" in data and "profile" in data:
            self._parse_dbt_project(data, rel_path, results)

    def _parse_dbt_schema(self, data: dict, rel_path: str, results: dict) -> None:
        """Parse dbt schema.yml to extract model metadata."""
        models = data.get("models", [])
        if not isinstance(models, list):
            return

        for model in models:
            if not isinstance(model, dict):
                continue
            name = model.get("name", "")
            if not name:
                continue

            results["dbt_models"][name] = {
                "description": model.get("description", ""),
                "columns": model.get("columns", []),
                "config": model.get("config", {}),
                "source_file": rel_path,
            }

            # Add config edge from YAML to model
            results["config_edges"].append(GraphEdge(
                source=rel_path,
                target=name,
                edge_type=EdgeType.CONFIGURES,
                metadata={"config_type": "dbt_schema"},
                transformation_type="config_parse",
                source_file=rel_path,
                line_range=(1, 1),
            ))

    def _parse_dbt_sources(self, data: dict, rel_path: str, results: dict) -> None:
        """Parse dbt sources.yml to extract source tables."""
        sources = data.get("sources", [])
        if not isinstance(sources, list):
            return

        for source in sources:
            if not isinstance(source, dict):
                continue
            source_name = source.get("name", "")
            tables = source.get("tables", [])

            for table in tables:
                if not isinstance(table, dict):
                    continue
                table_name = table.get("name", "")
                if not table_name:
                    continue

                full_name = f"{source_name}.{table_name}" if source_name else table_name

                results["datasets"].append(DatasetNode(
                    id=full_name,
                    name=full_name,
                    storage_type=StorageType.TABLE,
                    is_source_of_truth=True,
                    schema_snapshot={
                        "columns": [c.get("name", "") for c in table.get("columns", [])
                                    if isinstance(c, dict)]
                    } if table.get("columns") else None,
                ))

                results["config_edges"].append(GraphEdge(
                    source=rel_path,
                    target=full_name,
                    edge_type=EdgeType.CONFIGURES,
                    metadata={"config_type": "dbt_source"},
                    transformation_type="config_parse",
                    source_file=rel_path,
                    line_range=(1, 1),
                ))

    def _parse_dbt_project(self, data: dict, rel_path: str, results: dict) -> None:
        """Parse dbt_project.yml for project-level configuration."""
        project_name = data.get("name", "")
        # Store project config for reference
        results["config_edges"].append(GraphEdge(
            source=rel_path,
            target=f"project:{project_name}",
            edge_type=EdgeType.CONFIGURES,
            metadata={"config_type": "dbt_project", "project_name": project_name},
            transformation_type="config_parse",
            source_file=rel_path,
            line_range=(1, 1),
        ))

    def _is_airflow_dag(self, content: str) -> bool:
        """Heuristic: check if a Python file defines an Airflow DAG."""
        return ("DAG(" in content or "with DAG" in content) and "airflow" in content.lower()

    def _parse_airflow_dag(self, py_path: Path, repo_root: Path,
                           content: str, results: dict) -> None:
        """Parse an Airflow DAG file to extract task dependencies."""
        rel_path = str(py_path.relative_to(repo_root))

        # Extract DAG name from DAG() constructor
        dag_name_match = re.search(r"DAG\s*\(\s*['\"]([^'\"]+)['\"]", content)
        # Or from @dag(dag_id="...")
        if not dag_name_match:
            dag_name_match = re.search(r"@dag\s*\(\s*dag_id\s*=\s*['\"]([^'\"]+)['\"]", content)
        
        dag_name = dag_name_match.group(1) if dag_name_match else py_path.stem

        # Extract operator instances (task_id assignments)
        tasks = {}
        # Pattern 1: var = Operator(task_id="name")
        for match in re.finditer(
            r"(\w+)\s*=\s*(\w+(?:Operator|Sensor|Task))\s*\(\s*task_id\s*=\s*['\"]([^'\"]+)['\"]",
            content
        ):
            var_name, operator_type, task_id = match.groups()
            tasks[var_name] = {
                "task_id": task_id,
                "operator": operator_type,
                "dag": dag_name,
            }
        
        # Pattern 2: @task(task_id="name") def funcname():
        for match in re.finditer(
            r"@task(?:\(task_id\s*=\s*['\"]([^'\"]+)['\"]\))?\s+def\s+(\w+)",
            content
        ):
            task_id_attr, func_name = match.groups()
            task_id = task_id_attr if task_id_attr else func_name
            tasks[func_name] = {
                "task_id": task_id,
                "operator": "TaskFlow",
                "dag": dag_name,
            }

        # Extract task dependencies (>> and << operators)
        # This regex handles both var names and function calls used as tasks
        for match in re.finditer(r"(\w+)(?:\([^)]*\))?\s*>>\s*(\w+)", content):
            upstream, downstream = match.groups()
            if upstream in tasks and downstream in tasks:
                results["config_edges"].append(GraphEdge(
                    source=f"task:{tasks[upstream]['task_id']}",
                    target=f"task:{tasks[downstream]['task_id']}",
                    edge_type=EdgeType.CONFIGURES,
                    metadata={
                        "dag": dag_name,
                        "source_file": rel_path,
                        "relationship": "task_dependency",
                    },
                    transformation_type="airflow_task_dependency",
                    source_file=rel_path,
                    line_range=(1, 1),
                ))

        # Extract list-based dependencies: [task1, task2] >> task3
        for match in re.finditer(r"\[([^\]]+)\]\s*>>\s*(\w+)", content):
            upstream_list, downstream = match.groups()
            upstream_vars = [v.strip() for v in upstream_list.split(",")]
            for uv in upstream_vars:
                if uv in tasks and downstream in tasks:
                    results["config_edges"].append(GraphEdge(
                        source=f"task:{tasks[uv]['task_id']}",
                        target=f"task:{tasks[downstream]['task_id']}",
                        edge_type=EdgeType.CONFIGURES,
                        metadata={
                            "dag": dag_name,
                            "source_file": rel_path,
                            "relationship": "task_dependency",
                        },
                        transformation_type="airflow_task_dependency",
                        source_file=rel_path,
                        line_range=(1, 1),
                    ))

        # Add the DAG config edge
        results["config_edges"].append(GraphEdge(
            source=rel_path,
            target=f"dag:{dag_name}",
            edge_type=EdgeType.CONFIGURES,
            metadata={"config_type": "airflow_dag"},
            transformation_type="airflow_dag_config",
            source_file=rel_path,
            line_range=(1, 1),
        ))


# Import yaml safely
try:
    import yaml
except ImportError:
    import json as yaml  # Fallback (won't work for YAML, but avoids crash)
