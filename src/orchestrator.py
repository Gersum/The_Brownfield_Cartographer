"""Orchestrator — wires agents in sequence, serializes outputs.

For the interim submission: Surveyor → Hydrologist.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.panel import Panel

from src.agents.hydrologist import HydrologistAgent
from src.agents.surveyor import SurveyorAgent
from src.graph.knowledge_graph import KnowledgeGraph

console = Console()


class Orchestrator:
    """Runs the full Cartographer analysis pipeline."""

    def __init__(self, repo_path: str | Path, output_dir: Optional[str | Path] = None):
        self.repo_path = Path(repo_path).resolve()
        if output_dir:
            self.output_dir = Path(output_dir).resolve()
        else:
            self.output_dir = self.repo_path / ".cartography"
        self.trace_log: list[dict] = []

    def run(self) -> dict:
        """Execute the full analysis pipeline."""
        start_time = datetime.now()
        console.print(Panel.fit(
            f"[bold green]🗺️  The Brownfield Cartographer[/bold green]\n"
            f"Target: {self.repo_path}\n"
            f"Output: {self.output_dir}",
            title="Analysis Pipeline",
        ))

        self.output_dir.mkdir(parents=True, exist_ok=True)

        results = {}

        # Phase 1: Surveyor — Static Structure
        self._log("pipeline", "start", "surveyor")
        surveyor = SurveyorAgent(self.repo_path)
        module_graph = surveyor.run()
        surveyor.save(self.output_dir)
        results["module_graph"] = module_graph
        results["surveyor_trace"] = surveyor.trace_log
        self._log("pipeline", "complete", "surveyor")

        # Phase 2: Hydrologist — Data Lineage
        self._log("pipeline", "start", "hydrologist")
        hydrologist = HydrologistAgent(self.repo_path, module_graph=module_graph)
        lineage_graph = hydrologist.run()
        hydrologist.save(self.output_dir)
        results["lineage_graph"] = lineage_graph
        results["hydrologist_trace"] = hydrologist.trace_log
        self._log("pipeline", "complete", "hydrologist")

        # Phase 3: Archivist — Visualizations & Artifacts
        self._log("pipeline", "start", "archivist")
        from src.agents.archivist import ArchivistAgent
        archivist = ArchivistAgent(self.repo_path, module_graph, lineage_graph)
        artifacts = archivist.run()
        results["artifacts"] = artifacts
        self._log("pipeline", "complete", "archivist")
        results["artifacts"] = artifacts
        self._log("pipeline", "complete", "archivist")

        # Save combined trace log
        all_traces = (
            surveyor.trace_log
            + hydrologist.trace_log
            + self.trace_log
        )
        self._save_trace_log(all_traces)

        # Summary
        elapsed = (datetime.now() - start_time).total_seconds()
        console.print(Panel.fit(
            f"[bold green]✅ Analysis Complete[/bold green]\n"
            f"Time: {elapsed:.1f}s\n"
            f"Module graph: {module_graph.summary()['node_count']} nodes, "
            f"{module_graph.summary()['edge_count']} edges\n"
            f"Lineage graph: {lineage_graph.summary()['node_count']} nodes, "
            f"{lineage_graph.summary()['edge_count']} edges\n"
            f"Artifacts: {len(artifacts)} generated\n"
            f"Output: {self.output_dir}",
            title="Pipeline Summary",
        ))

        return results

    def _save_trace_log(self, traces: list[dict]) -> None:
        """Save the combined trace log as JSONL."""
        trace_path = self.output_dir / "cartography_trace.jsonl"
        with open(trace_path, "w") as f:
            for entry in traces:
                f.write(json.dumps(entry, default=str) + "\n")
        console.print(f"  📋 Trace log: {trace_path}")

    def _log(self, action: str, target: str, result: str) -> None:
        """Log a pipeline action."""
        self.trace_log.append({
            "timestamp": datetime.now().isoformat(),
            "agent": "orchestrator",
            "action": action,
            "target": target,
            "result": result,
        })
