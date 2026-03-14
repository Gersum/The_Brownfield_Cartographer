"""Navigator Agent — Interactive Query Interface.

Uses LangGraph to expose a conversational interface backed by tools that 
query the generated Knowledge Graph (Module Graph & Lineage Graph).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated, Literal

from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent
from rich.console import Console

from src.graph.knowledge_graph import KnowledgeGraph

console = Console()

# Global state for tools (since LangChain tools are decorated functions)
_module_graph: KnowledgeGraph | None = None
_lineage_graph: KnowledgeGraph | None = None


@tool
def find_implementation(concept_or_keyword: str) -> str:
    """Finds modules or public functions related to a specific concept or keyword."""
    if not _module_graph:
        return "Module graph not loaded."
        
    results = []
    # Search nodes
    for node in _module_graph.get_nodes_by_type("module"):
        # Check path and purpose
        path = node.get("id", "")
        purpose = node.get("purpose_statement", "")
        if concept_or_keyword.lower() in path.lower() or (purpose and concept_or_keyword.lower() in purpose.lower()):
            results.append(f"- Module: {path} (Purpose: {purpose})")
            
        # Check public functions/classes
        funcs = node.get("public_functions", [])
        classes = node.get("public_classes", [])
        for f in funcs:
            if concept_or_keyword.lower() in f.lower():
                results.append(f"  - Function: {f} in {path}")
        for c in classes:
            if concept_or_keyword.lower() in c.lower():
                results.append(f"  - Class: {c} in {path}")
                
    if not results:
        return f"No implementations found matching '{concept_or_keyword}'."
        
    return "Found the following implementations:\n" + "\n".join(results[:15])


@tool
def trace_lineage(dataset_id: str, direction: str = "upstream") -> str:
    """Traces data lineage from a dataset. Use direction='upstream' for what feeds it, or 'downstream' for what it feeds."""
    if not _lineage_graph:
        return "Lineage graph not loaded."

    node = _lineage_graph.get_node(dataset_id)
    if not node:
        # Try to find by name loosely
        for n in _lineage_graph.get_nodes_by_type("dataset"):
            if dataset_id.lower() in n.get("name", "").lower():
                dataset_id = n["id"]
                break
        else:
            return f"Dataset '{dataset_id}' not found."

    edges = _lineage_graph.get_all_edges()
    related = []

    def _format_location(edge: dict, node_id: str) -> str:
        """Best-effort file:line citation for an edge or its node."""
        source_file = edge.get("source_file")
        line_info = None

        node_data = _lineage_graph.get_node(node_id)
        if node_data:
            line_range = node_data.get("line_range")
            if isinstance(line_range, (list, tuple)) and len(line_range) >= 1:
                try:
                    line_info = int(line_range[0])
                except Exception:
                    line_info = None

        if source_file and line_info is not None:
            return f"{source_file}:{line_info}"
        if source_file:
            return source_file
        if line_info is not None:
            return f"line {line_info}"
        return "location unknown"

    seen = set()

    if direction.lower() == "upstream":
        for e in edges:
            if e["target"] == dataset_id:
                src = e["source"]
                edge_type = e.get("edge_type", "TRANSFORM")
                loc = _format_location(e, src)
                key = (src, edge_type, loc, "up")
                if key in seen:
                    continue
                seen.add(key)
                related.append(
                    f"- Upstream source: {src} via {edge_type} [{loc}]"
                )
    else:  # downstream
        for e in edges:
            if e["source"] == dataset_id:
                tgt = e["target"]
                edge_type = e.get("edge_type", "TRANSFORM")
                loc = _format_location(e, tgt)
                key = (tgt, edge_type, loc, "down")
                if key in seen:
                    continue
                seen.add(key)
                related.append(
                    f"- Downstream target: {tgt} via {edge_type} [{loc}]"
                )

    if not related:
        return f"No {direction} dependencies found for '{dataset_id}'."

    return "\n".join(related)


@tool
def blast_radius(module_path: str) -> str:
    """Calculates the impact of changing a specific Python file or SQL module (what other modules import/depend on it)."""
    if not _module_graph:
        return "Module graph not loaded."
        
    node = _module_graph.get_node(module_path)
    if not node:
        return f"Module '{module_path}' not found. Are you sure you have the correct file path?"
        
    # Find all downstream dependents (edges where target is this module... wait, IMPORTS edge means source imports target)
    # If A imports B (A -> B), then changing B impacts A.
    # So we want edges where `target` == module_path (meaning `source` depends on it).
    impacted = []
    for e in _module_graph.get_all_edges():
        if e["target"] == module_path:
            impacted.append(e["source"])
            
    if not impacted:
        return f"Changing '{module_path}' has 0 known downstream dependents (safe to modify)."
        
    return f"Changing '{module_path}' impacts {len(impacted)} modules directly:\n" + "\n".join(f"- {m}" for m in impacted)


@tool
def explain_module(module_path: str) -> str:
    """Provides a detailed semantic explanation and context for a given module/file path."""
    if not _module_graph:
        return "Module graph not loaded."
        
    node = _module_graph.get_node(module_path)
    if not node:
        return f"Module '{module_path}' not found."
        
    details = [
        f"**Module**: {node.get('id')}",
        f"**Language**: {node.get('language')}",
        f"**Domain**: {node.get('domain_cluster', 'Unknown')}",
        f"**Purpose**: {node.get('purpose_statement', 'None')}",
        f"**Complexity Score**: {node.get('complexity_score')}",
        f"**Public API**: {len(node.get('public_functions', []))} functions, {len(node.get('public_classes', []))} classes",
        f"**Doc Drift Alarm**: {'Yes' if node.get('documentation_drift') else 'No'}"
    ]
    return "\n".join(details)


class NavigatorAgent:
    """Interactive conversational agent using LangGraph."""
    
    def __init__(self, repo_path: str | Path):
        self.repo_path = Path(repo_path)
        self.output_dir = self.repo_path / ".cartography"
        
        # Load the graphs
        global _module_graph, _lineage_graph
        try:
            mod_graph = KnowledgeGraph.load(self.output_dir / "module_graph.json")
            lin_graph = KnowledgeGraph.load(self.output_dir / "lineage_graph.json")
            _module_graph = mod_graph
            _lineage_graph = lin_graph
            self.loaded = True
        except Exception:
            self.loaded = False
            console.print("  ⚠️  [orange3]Could not load Cartography graphs. Ensure you run `analyze` first.[/orange3]")
            
    def run_interactive(self) -> None:
        """Start an interactive session with the user using a manual ReAct loop."""
        if not self.loaded:
            console.print("  ⚠️  [orange3]Knowledge graph not loaded. Run 'analyze' first.[/orange3]")
            return

        try:
            from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
            from langchain_ollama import ChatOllama
            import re
            
            # Use llama3.1 as requested
            llm = ChatOllama(model="llama3.1", temperature=0.1)
            
            system_prompt = """You are the Cartographer Navigator, an expert AI assistant.
You explore a codebase using a specific scratchpad format.
You have access to 4 tools to query the Knowledge Graph:
1. find_implementation(query: str): Search for files or functions by name.
2. trace_lineage(target: str): Trace upstream/downstream data dependencies for a module.
3. blast_radius(module_id: str): Find the downstream impact of changing a module.
4. explain_module(module_id: str): Get high-level purpose and LLM-generated documentation for a module.

Format:
Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [find_implementation, trace_lineage, blast_radius, explain_module]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat)
Thought: I now know the final answer
Final Answer: the final answer to the original question

Always use tools if you need to look up information.
"""
            
            console.print("\n🧭 [bold cyan]Navigator Agent[/bold cyan] — Interactive Session Started")
            console.print("Ask me anything about the codebase. Type 'exit' to quit.\n")
            
            tool_fns = {
                "find_implementation": find_implementation,
                "trace_lineage": trace_lineage,
                "blast_radius": blast_radius,
                "explain_module": explain_module
            }
            
            while True:
                try:
                    user_input = console.input("[bold green]Query > [/bold green]")
                    if user_input.lower() in ("exit", "quit", "q"):
                        break
                    
                    if not user_input.strip():
                        continue

                    messages = [SystemMessage(content=system_prompt), HumanMessage(content=user_input)]
                    
                    for step in range(5):
                        response = llm.invoke(messages)
                        content = response.content
                        
                        # Show the thought process to the user
                        for line in content.split("\n"):
                            if line.startswith("Thought:"):
                                console.print(f"[dim]{line}[/dim]")
                            elif line.startswith("Action:"):
                                console.print(f"🛠️  [yellow]{line}[/yellow]")
                            elif line.startswith("Action Input:"):
                                console.print(f"📥 [yellow]{line}[/yellow]")

                        messages.append(AIMessage(content=content))
                        
                        if "Final Answer:" in content:
                            answer = content.split("Final Answer:")[-1].strip()
                            console.print(f"\n✨ [bold]Navigator:[/bold] {answer}\n")
                            break
                        
                        # Parse Action and Action Input
                        action_match = re.search(r"Action:\s*(\w+)", content)
                        input_match = re.search(r"Action Input:\s*(.+)", content)
                        
                        if action_match and input_match:
                            action = action_match.group(1).strip()
                            action_input = input_match.group(1).strip().strip('"').strip("'")
                            
                            if action in tool_fns:
                                try:
                                    observation = tool_fns[action](action_input)
                                    # Limit observation size in prompt to avoid token overflow
                                    prompt_obs = observation[:2000] + "..." if len(observation) > 2000 else observation
                                    console.print(f"👁️  [blue]Observation: {observation[:150]}...[/blue]")
                                    messages.append(HumanMessage(content=f"Observation: {prompt_obs}"))
                                except Exception as te:
                                    messages.append(HumanMessage(content=f"Observation Error: {te}"))
                            else:
                                messages.append(HumanMessage(content=f"Observation: Unknown tool '{action}'"))
                        else:
                            # No action and no final answer? Just stop or ask to continue.
                            if "Thought:" in content and not action_match:
                                console.append(HumanMessage(content="Please provide an Action or Final Answer."))
                            else:
                                console.print(f"\n✨ [bold]Navigator:[/bold] {content}\n")
                                break
                                
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    console.print(f"  ❌ Error: {e}")
                    
        except ImportError:
            console.print("  ⚠️  [orange3]Missing dependencies. Install with 'pip install langchain-ollama'[/orange3]")
        except Exception as e:
            console.print(f"  ❌ Navigator failed: {e}")
