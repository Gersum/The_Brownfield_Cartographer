"""CLI entry point for The Brownfield Cartographer.

Usage:
    python -m src.cli analyze <repo_path> [--output <dir>]
    python -m src.cli analyze <github_url> [--output <dir>]
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import click
from rich.console import Console

console = Console()


@click.group()
@click.version_option(version="0.1.0")
def main():
    """🗺️ The Brownfield Cartographer — Codebase Intelligence System"""
    pass


@main.command()
@click.argument("repo_path")
@click.option("--output", "-o", default=None,
              help="Output directory (default: <repo>/.cartography)")
@click.option("--incremental", "-i", is_flag=True,
              help="Use incremental mode (only analyze files changed since last run via git diff)")
def analyze(repo_path: str, output: str | None, incremental: bool):
    """Analyze a codebase and generate the cartography artifacts.
    
    REPO_PATH can be a local directory or a GitHub URL.
    """
    from src.orchestrator import Orchestrator

    # Handle GitHub URLs
    actual_path = _resolve_repo_path(repo_path)
    if actual_path is None:
        console.print("[red]Error: Could not resolve repo path[/red]")
        sys.exit(1)

    # Run the analysis
    orchestrator = Orchestrator(actual_path, output_dir=output, incremental=incremental)
    try:
        results = orchestrator.run()
    except Exception as e:
        console.print(f"[red]Error during analysis: {e}[/red]")
        import traceback
        traceback.print_exc()
        sys.exit(1)


@main.command()
@click.argument("repo_path")
@click.option("--output", "-o", default=None,
              help="Output directory (default: <repo>/.cartography)")
def visualize(repo_path: str, output: str | None):
    """Re-generate premium visualizations for an analyzed codebase."""
    from src.graph.knowledge_graph import KnowledgeGraph
    from src.agents.archivist import ArchivistAgent
    
    actual_path = _resolve_repo_path(repo_path)
    if actual_path is None:
        console.print("[red]Error: Could not resolve repo path[/red]")
        sys.exit(1)
        
    output_dir = Path(output) if output else Path(actual_path) / ".cartography"
    
    # Load existing graphs
    module_graph_path = output_dir / "module_graph.json"
    lineage_graph_path = output_dir / "lineage_graph.json"
    
    if not module_graph_path.exists():
        console.print(f"[red]Error: Graph not found at {module_graph_path}. Run 'analyze' first.[/red]")
        sys.exit(1)
        
    console.print(f"[cyan]Loading graphs from {output_dir}...[/cyan]")
    module_graph = KnowledgeGraph.load(module_graph_path)
    lineage_graph = KnowledgeGraph.load(lineage_graph_path)
    
    archivist = ArchivistAgent(actual_path, module_graph, lineage_graph)
    archivist.generate_premium_visualizations()
    console.print("[green]Premium visualizations updated.[/green]")


@main.command()
@click.argument("repo_path")
def query(repo_path: str):
    """Launch interactive query mode (Navigator agent).
    
    Requires a previously analyzed codebase.
    """
    from src.agents.navigator import NavigatorAgent
    
    actual_path = _resolve_repo_path(repo_path)
    if actual_path is None:
        console.print("[red]Error: Could not resolve repo path[/red]")
        sys.exit(1)
        
    navigator = NavigatorAgent(actual_path)
    navigator.run_interactive()


@main.command()
@click.argument("repo_path")
@click.option(
    "-q",
    "--question",
    required=True,
    help="Free-form architecture question for the Semanticist agent.",
)
def semantic_ask(repo_path: str, question: str):
    """Ask the Semanticist agent a free-form question about an analyzed repo.

    Requires that `analyze` has already been run so the module graph exists.
    """
    from src.graph.knowledge_graph import KnowledgeGraph
    from src.agents.semanticist import SemanticistAgent

    actual_path = _resolve_repo_path(repo_path)
    if actual_path is None:
        console.print("[red]Error: Could not resolve repo path[/red]")
        sys.exit(1)

    output_dir = Path(actual_path) / ".cartography"
    module_graph_path = output_dir / "module_graph.json"

    if not module_graph_path.exists():
        console.print(
            f"[red]Error: Module graph not found at {module_graph_path}. "
            "Run 'analyze' first.[/red]"
        )
        sys.exit(1)

    console.print(f"[cyan]Loading module graph from {module_graph_path}...[/cyan]")
    module_graph = KnowledgeGraph.load(module_graph_path)

    semanticist = SemanticistAgent(str(actual_path), module_graph)
    answer = semanticist.ask(question)

    console.print("\n[bold magenta]🧠 Semanticist Answer[/bold magenta]\n")
    console.print(answer)


def _resolve_repo_path(repo_path: str) -> Path | None:
    """Resolve a repo path (local dir or GitHub URL) to a local path."""
    # Check if it's a local path
    local = Path(repo_path).resolve()
    if local.exists() and local.is_dir():
        return local

    # Check if it's a GitHub URL
    if repo_path.startswith("http") and ("github.com" in repo_path or "gitlab.com" in repo_path):
        return _clone_repo(repo_path)

    console.print(f"[red]Path does not exist: {repo_path}[/red]")
    return None


def _clone_repo(url: str) -> Path | None:
    """Clone a Git repository to a temporary directory."""
    # Extract repo name from URL
    repo_name = url.rstrip("/").split("/")[-1].replace(".git", "")
    clone_dir = Path("targets") / repo_name

    if clone_dir.exists():
        console.print(f"[yellow]Target already exists: {clone_dir}[/yellow]")
        return clone_dir.resolve()

    console.print(f"[cyan]Cloning {url}...[/cyan]")
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", url, str(clone_dir)],
            check=True, capture_output=True, text=True,
        )
        console.print(f"[green]Cloned to {clone_dir}[/green]")
        return clone_dir.resolve()
    except subprocess.CalledProcessError as e:
        console.print(f"[red]Clone failed: {e.stderr}[/red]")
        return None


if __name__ == "__main__":
    main()
