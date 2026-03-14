"""Semanticist Agent for LLM-powered codebase analysis.

Uses Gemini to generate purpose statements, detect documentation drift,
and cluster modules into domains.
"""

from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Any, Optional

import tiktoken
from sklearn.feature_extraction.text import TfidfVectorizer
from langchain_ollama import ChatOllama
from sklearn.cluster import KMeans
from rich.console import Console

from src.graph.knowledge_graph import KnowledgeGraph
from src.models.nodes import ModuleNode

console = Console()


class ContextWindowBudget:
    """Tracks token usage for LLM calls based on tiktoken estimation."""
    def __init__(self, max_tokens_per_minute: int = 1_000_000):
        self._tokenizer = tiktoken.get_encoding("cl100k_base")
        self.tokens_used_this_run = 0
        self.max_tokens = max_tokens_per_minute

    def estimate_tokens(self, text: str) -> int:
        return len(self._tokenizer.encode(text))

    def consume(self, count: int) -> bool:
        """Consumes tokens. Returns False if budget exceeded."""
        if self.tokens_used_this_run + count > self.max_tokens:
            return False
        self.tokens_used_this_run += count
        return True


class SemanticistAgent:
    """Agent that adds semantic layers to the knowledge graph using Gemini."""

    def __init__(self, repo_path: str, graph: KnowledgeGraph):
        self.repo_path = Path(repo_path)
        self.graph = graph
        self.budget = ContextWindowBudget()
        self._setup_llm()

    def _setup_llm(self):
        """Initialize the Ollama client."""
        try:
            self.flash_model = ChatOllama(model="llama3.1", temperature=0.1)
            self.pro_model = ChatOllama(model="llama3.1", temperature=0.2)
            self.client = True
        except Exception as e:
            console.print(f"  ⚠️  [orange3]Failed to initialize Ollama: {e}[/orange3]")
            self.client = None

    def run(self) -> None:
        """Run the semantic analysis phase."""
        if not self.client:
            return

        console.print("\n🧠 [bold magenta]Semanticist Agent[/bold magenta] — Deepening understanding")
        
        # 1. Generate Purpose Statements & Detect Documentation Drift
        modules = self.graph.get_nodes_by_type("module")
        # Prioritize by PageRank for high-impact insights first
        pagerank = self.graph.pagerank()
        modules.sort(key=lambda m: pagerank.get(m["id"], 0), reverse=True)
        
        # In huge codebases, cap at 50 modules for "Day Zero" analysis
        if len(modules) > 50:
            console.print(f"  🏢 Large codebase detected ({len(modules)} modules). Focusing on top 50 critical hubs for Day Zero.")
            modules = modules[:50]
        
        console.print(f"  📝 Analyzing {len(modules)} modules for purpose and doc drift...")
        
        updated_count = 0
        drift_count = 0
        
        for i, mod_data in enumerate(modules):
            mod_id = mod_data["id"]
            if mod_data.get("purpose_statement"):
                continue
                
            purpose, doc_drift = self._generate_purpose(mod_id)
            if purpose:
                mod_data["purpose_statement"] = purpose
                if doc_drift:
                    mod_data["documentation_drift"] = True
                    drift_count += 1
                
                node = ModuleNode(**mod_data)
                self.graph.add_node(node)
                updated_count += 1
                
            # Periodic saving
            if updated_count > 0 and updated_count % 10 == 0:
                self.graph.save(self.repo_path / ".cartography" / "module_graph.json")

        # Final save for the semantic phase
        if updated_count > 0:
            self.graph.save(self.repo_path / ".cartography" / "module_graph.json")
                
        console.print(f"  ✅ Updated {updated_count} modules with semantic context")
        if drift_count > 0:
            console.print(f"  🚨 Detected documentation drift in {drift_count} modules")
        
        # 2. Domain Clustering
        self._cluster_domains()

    def _generate_purpose(self, mod_id: str) -> tuple[Optional[str], bool]:
        """Generate purpose statement and detect doc drift. Returns (purpose, has_drift)."""
        try:
            full_path = self.repo_path / mod_id
            if not full_path.exists() or full_path.is_dir():
                return None, False
                
            code = full_path.read_text(encoding="utf-8", errors="replace")
            
            # Efficiency: Skip very small files (usually empty __init__ or one-liners)
            if len(code.strip()) < 50:
                return "Utility/Initialization module.", False

            # Context Window Budgeting
            tokens = self.budget.estimate_tokens(code)
            if tokens > 30000:
                code = code[:100000] + "\n... [truncated]" # Roughly 25k tokens
                tokens = self.budget.estimate_tokens(code)
                
            if not self.budget.consume(tokens + 500): # +500 for prompt/response
                console.print(f"  ⚠️  Skipping {mod_id}: Context budget exceeded.")
                return None, False

            mod_node = self.graph.get_node(mod_id)
            history = ""
            if mod_node and mod_node.get("commit_summaries"):
                history = "\nRecent changes:\n" + "\n".join(f"- {s}" for s in mod_node["commit_summaries"])

            prompt = f"""
Analyze the following code from file `{mod_id}`.
Provide TWO things formatted as JSON:
1. `purpose_statement`: A concise (1-2 sentence) summary of its core purpose in the system. Focus on what it ACTUALLY does based on the code logic, not generic descriptions.
2. `documentation_drift`: A boolean (true/false) indicating if the code seems out of sync with its own comments/docstrings, or if the history indicates the code evolved significantly but docs didn't.

{history}

CODE:
```
{code}
```

Return ONLY valid JSON in this exact format:
{{
    "purpose_statement": "...",
    "documentation_drift": false
}}
"""
            # Use Flash for bulk processing
            response = self.flash_model.invoke(prompt)
            import re
            import json
            
            text = str(response.content).strip()
            
            # Ultra-robust JSON extraction: Find all balanced { } pairs and try parsing them
            def robust_json_extract(s):
                # Find all '{' positions
                starts = [m.start() for m in re.finditer(r'\{', s)]
                # Find all '}' positions in reverse (to try largest blocks first)
                ends = [m.start() for m in re.finditer(r'\}', s)]
                
                # Try all combinations from largest to smallest
                for start in starts:
                    for end in sorted(ends, reverse=True):
                        if end > start:
                            try:
                                candidate = s[start:end+1]
                                data = json.loads(candidate)
                                if "purpose_statement" in data:
                                    return data
                            except:
                                continue
                return None

            data = robust_json_extract(text)
            if data:
                return data.get("purpose_statement"), data.get("documentation_drift", False)
            
            # Fallback for very messy output (manual string cleaning)
            if 'text' in locals():
                console.print(f"  ❌ Failed to extract JSON for {mod_id}. Raw: {text[:100]}...")
            return None, False
        except Exception as e:
            console.print(f"  ❌ Error for {mod_id}: {e}")
            return None, False

    def _cluster_domains(self):
        """Identify domain boundaries by embedding purpose statements and clustering."""
        console.print("  🏷️  Clustering modules into domains...")
        modules = self.graph.get_nodes_by_type("module")
        
        # Collect nodes with purpose statements
        valid_mods = [m for m in modules if m.get("purpose_statement")]
        if len(valid_mods) < 3:
            return # Not enough data to cluster
            
        purposes = [m["purpose_statement"] for m in valid_mods]
        
        try:
            # Use TF-IDF + K-Means for local clustering
            vectorizer = TfidfVectorizer(stop_words='english', max_features=100)
            X = vectorizer.fit_transform(purposes)
            
            # Determine k (rough heuristic)
            k = max(2, min(5, len(valid_mods) // 3))
            kmeans = KMeans(n_clusters=k, random_state=42, n_init='auto')
            clusters = kmeans.fit_predict(X)
            
            # Extract top keywords per cluster to act as the Domain Label
            order_centroids = kmeans.cluster_centers_.argsort()[:, ::-1]
            terms = vectorizer.get_feature_names_out()
            
            cluster_labels = {}
            for i in range(k):
                # Pick top 2 keywords for the domain name
                top_terms = [terms[ind] for ind in order_centroids[i, :2]]
                cluster_labels[i] = f"Domain_{'-'.join(top_terms)}"
            
            # Update knowledge graph
            for mod, cluster_id in zip(valid_mods, clusters):
                mod["domain_cluster"] = cluster_labels[cluster_id]
                self.graph.add_node(ModuleNode(**mod))
                
            console.print("  ✅ Inferred domains from semantic clustering:")
            for label in set(cluster_labels.values()):
                count = list(clusters).count(list(cluster_labels.keys())[list(cluster_labels.values()).index(label)])
                console.print(f"     • {label} ({count} modules)")
                
        except Exception as e:
            console.print(f"  ⚠️  Clustering failed: {e}")

    def answer_questions(self) -> dict[str, str]:
        """Answer the Five FDE Day-One Questions using the Pro model."""
        if not self.client:
            return {}
            
        console.print("  🕵️  Synthesizing FDE Day-One Brief...")

        hubs = self.graph.pagerank()
        top_hubs = sorted(hubs.items(), key=lambda x: x[1], reverse=True)[:10]
        
        context = {
            "critical_hubs": [h[0] for h in top_hubs],
            "data_sources": self.graph.find_sources()[:10],
            "data_sinks": self.graph.find_sinks()[:10],
            "module_purposes": {}
        }
        
        for mod_id in context["critical_hubs"]:
            node = self.graph.get_node(mod_id)
            if node and node.get("purpose_statement"):
                context["module_purposes"][mod_id] = node["purpose_statement"]

        prompt = f"""
You are an expert Forward Deployed Engineer (FDE). Based on the Cartographer's analysis, answer the Five Day-One Questions.
Be highly specific. Cite exact file paths from the context. Do not invent information. 

CONTEXT:
{json.dumps(context, indent=2)}

THE FIVE QUESTIONS:
1. What is the core business purpose of this codebase?
2. What are the key data sources and where do they enter?
3. What are the critical paths for data transformation?
4. Where is the most complex or 'messy' part of the system (architectural debt)?
5. How would a new engineer run or test a change to a specific pipeline?

Format your response as a professional Markdown brief.
"""
        try:
            # Use Pro model for deep synthesis
            response = self.pro_model.invoke(prompt)
            return {"onboarding_brief": str(response.content).strip()}
        except Exception as e:
            console.print(f"  ⚠️  FDE Brief generation failed: {e}")
            return {}

    def ask(self, question: str) -> str:
        """Answer a free-form architecture question about this repo using semantic context.

        Requires that the module graph has already been populated (e.g. via Surveyor + Semanticist).
        """
        if not self.client:
            return "Semanticist LLM client is not available. Is Ollama with 'llama3.1' running?"

        question = question.strip()
        if not question:
            return "No question provided."

        hubs = self.graph.pagerank()
        top_hubs = sorted(hubs.items(), key=lambda x: x[1], reverse=True)[:10]

        context = {
            "critical_hubs": [h[0] for h in top_hubs],
            "data_sources": self.graph.find_sources()[:10],
            "data_sinks": self.graph.find_sinks()[:10],
            "module_purposes": {},
        }

        for mod_id in context["critical_hubs"]:
            node = self.graph.get_node(mod_id)
            if node and node.get("purpose_statement"):
                context["module_purposes"][mod_id] = node["purpose_statement"]

        prompt = f"""
You are an expert Forward Deployed Engineer (FDE) helping a teammate understand a codebase.
You have structured context from a static analysis + Semanticist pass.

CONTEXT (JSON):
{json.dumps(context, indent=2)}

USER QUESTION:
{question}

INSTRUCTIONS:
- Answer using ONLY the information in CONTEXT.
- Do NOT say you want to inspect code, open files, or run commands; you cannot access anything beyond this context.
- Be specific and concise, preferring clear declarative statements over hedging.
- When you reference a module or dataset, cite the exact path/id from the context.
- If something is not in the context, say clearly that it is "not in context" rather than guessing.
"""
        try:
            response = self.pro_model.invoke(prompt)
            return str(response.content).strip()
        except Exception as e:
            console.print(f"  ⚠️  Semanticist free-form question failed: {e}")
            return f"Semanticist error: {e}"
