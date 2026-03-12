"""Semanticist Agent for LLM-powered codebase analysis.

Uses Gemini to generate purpose statements, detect documentation drift,
and cluster modules into domains.
"""

from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Any, Optional

import google.generativeai as genai
from rich.console import Console

from src.graph.knowledge_graph import KnowledgeGraph
from src.models.nodes import ModuleNode

console = Console()

class SemanticistAgent:
    """Agent that adds semantic layers to the knowledge graph using Gemini."""

    def __init__(self, repo_path: str, graph: KnowledgeGraph):
        self.repo_path = Path(repo_path)
        self.graph = graph
        self._setup_gemini()

    def _setup_gemini(self):
        """Initialize the Gemini client."""
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            console.print("  ⚠️  [orange3]GOOGLE_API_KEY not found. Semantic analysis will be skipped.[/orange3]")
            self.client = None
            return

        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel("gemini-2.0-flash")
        self.client = True

    def run(self) -> None:
        """Run the semantic analysis phase."""
        if not self.client:
            return

        console.print("\n🧠 [bold magenta]Semanticist Agent[/bold magenta] — Deepening understanding")
        
        # Get all modules that need purpose statements
        modules = self.graph.get_nodes_by_type("module")
        console.print(f"  📝 Generating purpose statements for {len(modules)} modules...")
        
        updated_count = 0
        for mod_data in modules:
            mod_id = mod_data["id"]
            if mod_data.get("purpose_statement"):
                continue
                
            purpose = self._generate_purpose(mod_id)
            if purpose:
                # Update node in KG
                mod_data["purpose_statement"] = purpose
                # Re-add node to update data blob
                node = ModuleNode(**mod_data)
                self.graph.add_node(node)
                updated_count += 1
                
        console.print(f"  ✅ Updated {updated_count} modules with semantic context")
        
        # Domain clustering (placeholder for real embeddings)
        self._cluster_domains()

    def _generate_purpose(self, mod_id: str) -> Optional[str]:
        """Generate a purpose statement for a module using Gemini."""
        try:
            full_path = self.repo_path / mod_id
            if not full_path.exists():
                return None
                
            code = full_path.read_text(encoding="utf-8", errors="replace")
            # Truncate if too long (simple budget)
            if len(code) > 20000:
                code = code[:20000] + "\n... [truncated]"

            # Include commit history if available
            mod_node = self.graph.get_node(mod_id)
            history = ""
            if mod_node and mod_node.get("commit_summaries"):
                history = "\nRecent changes:\n" + "\n".join(f"- {s}" for s in mod_node["commit_summaries"])

            prompt = f"""
Analyze the following code from file `{mod_id}`.
Provide a concise (1-2 sentence) summary of its core purpose in the system.
Avoid generic descriptions like "this is a python file". Focus on its role in data flow or system logic.
{history}

CODE:
```
{code}
```
"""
            response = self.model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            # console.print(f"  ❌ Error generating purpose for {mod_id}: {e}")
            return None

    def _cluster_domains(self):
        """Identify domain boundaries based on module paths and purposes."""
        # Simple path-based clustering as a fallback/baseline
        modules = self.graph.get_nodes_by_type("module")
        domains = {}
        
        for mod in modules:
            parts = mod["id"].split("/")
            if len(parts) > 1:
                domain = parts[0]
                if domain not in domains:
                    domains[domain] = 0
                domains[domain] += 1
                
                # Update node
                mod["domain_cluster"] = domain
                node = ModuleNode(**mod)
                self.graph.add_node(node)
        
        if domains:
            console.print("  🏷️  Inferred domains:")
            for dom, count in sorted(domains.items(), key=lambda x: x[1], reverse=True)[:5]:
                console.print(f"     • {dom} ({count} modules)")

    def answer_questions(self) -> dict[str, str]:
        """Answer the Five FDE Day-One Questions using the knowledge graph context."""
        if not self.client:
            return {}

        # Prepare context (top critical hubs and sources)
        hubs = self.graph.pagerank()
        top_hubs = sorted(hubs.items(), key=lambda x: x[1], reverse=True)[:10]
        
        context = {
            "critical_hubs": [h[0] for h in top_hubs],
            "sources": self.graph.find_sources()[:10],
            "sinks": self.graph.find_sinks()[:10],
            "module_summaries": {}
        }
        
        # Add some purpose statements for context
        for mod_id in context["critical_hubs"]:
            node = self.graph.get_node(mod_id)
            if node and node.get("purpose_statement"):
                context["module_summaries"][mod_id] = node["purpose_statement"]

        prompt = f"""
You are an expert FDE (Forward Deployed Engineer) performing a Day-One reconnaissance of a new codebase.
Based on the following system metadata and architectural hubs, answer the Five FDE Day-One Questions.

CONTEXT:
{json.dumps(context, indent=2)}

QUESTIONS:
1. What is the core business purpose of this codebase?
2. What are the key data sources and where do they enter?
3. What are the critical paths for data transformation?
4. Where is the most complex or 'messy' part of the system (architectural debt)?
5. How would a new engineer run or test a change to a specific pipeline?

Provide evidence-based answers. If unsure, state what further investigation is needed.
"""
        try:
            response = self.model.generate_content(prompt)
            return {"onboarding_brief": response.text.strip()}
        except Exception:
            return {}
