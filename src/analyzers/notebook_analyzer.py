"""Notebook Analyzer — Extracts code from Jupyter Notebooks for analysis.

Parses .ipynb files (JSON) to extract Python source code and data flow patterns.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from rich.console import Console

console = Console()


class NotebookAnalyzer:
    """Analyzes Jupyter Notebooks (.ipynb) for structure and data lineage."""

    def analyze(self, file_path: str | Path) -> Optional[str]:
        """Extract all Python code from a Jupyter Notebook file."""
        file_path = Path(file_path)
        if not file_path.exists():
            return None

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            code_cells = []
            for cell in data.get("cells", []):
                if cell.get("cell_type") == "code":
                    source = cell.get("source", [])
                    if isinstance(source, list):
                        code_cells.append("".join(source))
                    else:
                        code_cells.append(str(source))
            
            return "\n\n".join(code_cells)
        except Exception as e:
            console.print(f"[WARN] Failed to parse notebook {file_path}: {e}")
            return None
