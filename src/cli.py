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
def analyze(repo_path: str, output: str | None):
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
    orchestrator = Orchestrator(actual_path, output_dir=output)
    try:
        results = orchestrator.run()
    except Exception as e:
        console.print(f"[red]Error during analysis: {e}[/red]")
        import traceback
        traceback.print_exc()
        sys.exit(1)


@main.command()
@click.argument("repo_path")
def query(repo_path: str):
    """Launch interactive query mode (Navigator agent).
    
    Requires a previously analyzed codebase.
    """
    console.print("[yellow]Query mode is coming in the final submission.[/yellow]")
    console.print("For now, use the JSON files in .cartography/ to explore the results.")


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
