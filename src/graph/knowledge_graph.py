"""Knowledge graph backed by SQLite with NetworkX for graph algorithms.

Uses SQLite as the persistent store for nodes and edges, with NetworkX
loaded on-demand for graph algorithms (PageRank, SCC, blast radius).
"""

from __future__ import annotations

import json
import sqlite3
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import networkx as nx
from pydantic import BaseModel

from src.models.edges import EdgeType, GraphEdge
from src.models.graphs import GraphMetadata
from src.models.nodes import DatasetNode, FunctionNode, ModuleNode, TransformationNode


_SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    node_type TEXT NOT NULL,
    data TEXT NOT NULL  -- JSON blob of all node attributes
);

CREATE TABLE IF NOT EXISTS edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    target TEXT NOT NULL,
    edge_type TEXT NOT NULL,
    weight REAL DEFAULT 1.0,
    data TEXT NOT NULL,  -- JSON blob of all edge attributes
    FOREIGN KEY (source) REFERENCES nodes(id),
    FOREIGN KEY (target) REFERENCES nodes(id)
);

CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source);
CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target);
CREATE INDEX IF NOT EXISTS idx_edges_type ON edges(edge_type);
CREATE INDEX IF NOT EXISTS idx_nodes_type ON nodes(node_type);

CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class KnowledgeGraph:
    """Central data store for codebase intelligence.

    Persists nodes and edges in SQLite. Builds a NetworkX DiGraph
    on demand for graph algorithms (PageRank, SCC, BFS).
    """

    def __init__(self, repo_path: str = "", db_path: str | Path | None = None):
        self.repo_path = repo_path
        self._created_at = datetime.now().isoformat()

        if db_path:
            self._db_path = str(db_path)
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        else:
            self._db_path = ":memory:"

        self._conn = sqlite3.connect(self._db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=OFF")  # Allow flexible insertion order
        self._conn.executescript(_SCHEMA)

        # Store metadata
        self._set_meta("repo_path", repo_path)
        self._set_meta("created_at", self._created_at)

        # Cached NetworkX graph (invalidated on writes)
        self._nx_cache: Optional[nx.DiGraph] = None

    # ── Node operations ─────────────────────────────────────────────

    def add_node(self, node: BaseModel) -> None:
        """Add a typed node to the graph."""
        data = node.model_dump(mode="json")
        node_id = data.pop("id")
        node_type = data.get("node_type", "unknown")
        self._conn.execute(
            "INSERT OR REPLACE INTO nodes (id, node_type, data) VALUES (?, ?, ?)",
            (node_id, node_type, json.dumps(data, default=str)),
        )
        self._conn.commit()
        self._nx_cache = None

    def get_node(self, node_id: str) -> Optional[dict]:
        """Retrieve node data by ID."""
        row = self._conn.execute(
            "SELECT id, data FROM nodes WHERE id = ?", (node_id,)
        ).fetchone()
        if row:
            data = json.loads(row["data"])
            data["id"] = row["id"]
            return data
        return None

    def get_nodes_by_type(self, node_type: str) -> list[dict]:
        """Get all nodes of a given type."""
        rows = self._conn.execute(
            "SELECT id, data FROM nodes WHERE node_type = ?", (node_type,)
        ).fetchall()
        results = []
        for row in rows:
            data = json.loads(row["data"])
            data["id"] = row["id"]
            results.append(data)
        return results

    def node_count(self) -> int:
        """Count total nodes."""
        return self._conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]

    def has_node(self, node_id: str) -> bool:
        """Check if a node exists."""
        row = self._conn.execute(
            "SELECT 1 FROM nodes WHERE id = ?", (node_id,)
        ).fetchone()
        return row is not None

    # ── Edge operations ─────────────────────────────────────────────

    def add_edge(self, edge: GraphEdge) -> None:
        """Add a typed edge to the graph."""
        data = edge.model_dump(mode="json", exclude={"source", "target"})
        self._conn.execute(
            "INSERT INTO edges (source, target, edge_type, weight, data) VALUES (?, ?, ?, ?, ?)",
            (edge.source, edge.target, edge.edge_type.value,
             edge.weight, json.dumps(data, default=str)),
        )
        self._conn.commit()
        self._nx_cache = None

    def add_edge_simple(self, source: str, target: str, edge_type: EdgeType,
                        weight: float = 1.0, **kwargs) -> None:
        """Convenience method to add an edge without creating a GraphEdge object."""
        data = {"edge_type": edge_type.value, "weight": weight, **kwargs}
        self._conn.execute(
            "INSERT INTO edges (source, target, edge_type, weight, data) VALUES (?, ?, ?, ?, ?)",
            (source, target, edge_type.value, weight, json.dumps(data, default=str)),
        )
        self._conn.commit()
        self._nx_cache = None

    def get_edges(self, edge_type: Optional[EdgeType] = None) -> list[dict]:
        """Get all edges, optionally filtered by type."""
        if edge_type:
            rows = self._conn.execute(
                "SELECT source, target, data FROM edges WHERE edge_type = ?",
                (edge_type.value,)
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT source, target, data FROM edges"
            ).fetchall()
        results = []
        for row in rows:
            d = json.loads(row["data"])
            d["source"] = row["source"]
            d["target"] = row["target"]
            results.append(d)
        return results

    def get_all_edges(self) -> list[dict]:
        """Get all edges with their attributes."""
        return self.query_edges("1=1")

    def edge_count(self) -> int:
        """Count total edges."""
        return self._conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]

    # ── Graph algorithms (via NetworkX) ─────────────────────────────

    def _build_nx(self) -> nx.DiGraph:
        """Build a NetworkX DiGraph from the SQLite data."""
        if self._nx_cache is not None:
            return self._nx_cache

        G = nx.DiGraph()
        for row in self._conn.execute("SELECT id, data FROM nodes").fetchall():
            G.add_node(row["id"], **json.loads(row["data"]))
        for row in self._conn.execute("SELECT source, target, weight, data FROM edges").fetchall():
            data = json.loads(row["data"])
            data.pop("weight", None)  # Avoid duplication
            G.add_edge(row["source"], row["target"],
                       weight=row["weight"], **data)
        self._nx_cache = G
        return G

    def pagerank(self, **kwargs) -> dict[str, float]:
        """Run PageRank to identify the most critical nodes."""
        G = self._build_nx()
        if len(G) == 0:
            return {}
        try:
            return nx.pagerank(G, **kwargs)
        except nx.PowerIterationFailedConvergence:
            return nx.pagerank(G, max_iter=500, **kwargs)

    def strongly_connected_components(self) -> list[list[str]]:
        """Find circular dependencies (SCCs with size > 1)."""
        G = self._build_nx()
        sccs = list(nx.strongly_connected_components(G))
        return [sorted(list(scc)) for scc in sccs if len(scc) > 1]

    def topological_sort(self) -> list[str]:
        """Topological ordering (empty list if cyclic)."""
        G = self._build_nx()
        try:
            return list(nx.topological_sort(G))
        except nx.NetworkXUnfeasible:
            return []

    def blast_radius(self, node_id: str) -> list[str]:
        """BFS downstream from a node to find all dependents."""
        G = self._build_nx()
        if node_id not in G:
            return []
        visited = set()
        queue = deque([node_id])
        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            for successor in G.successors(current):
                if successor not in visited:
                    queue.append(successor)
        visited.discard(node_id)
        return sorted(list(visited))

    def find_sources(self) -> list[str]:
        """Nodes with in-degree=0 (entry points / data sources)."""
        G = self._build_nx()
        return sorted([n for n in G.nodes() if G.in_degree(n) == 0])

    def find_sinks(self) -> list[str]:
        """Nodes with out-degree=0 (terminal outputs)."""
        G = self._build_nx()
        return sorted([n for n in G.nodes() if G.out_degree(n) == 0])

    def get_predecessors(self, node_id: str) -> list[str]:
        """Direct upstream nodes."""
        rows = self._conn.execute(
            "SELECT DISTINCT source FROM edges WHERE target = ?", (node_id,)
        ).fetchall()
        return sorted([r["source"] for r in rows])

    def get_successors(self, node_id: str) -> list[str]:
        """Direct downstream nodes."""
        rows = self._conn.execute(
            "SELECT DISTINCT target FROM edges WHERE source = ?", (node_id,)
        ).fetchall()
        return sorted([r["target"] for r in rows])

    def upstream_trace(self, node_id: str) -> list[str]:
        """BFS upstream: all ancestors of a node."""
        G = self._build_nx()
        if node_id not in G:
            return []
        visited = set()
        queue = deque([node_id])
        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            for pred in G.predecessors(current):
                if pred not in visited:
                    queue.append(pred)
        visited.discard(node_id)
        return sorted(list(visited))

    # ── SQL queries ─────────────────────────────────────────────────

    def query_nodes(self, sql_where: str = "1=1", params: tuple = ()) -> list[dict]:
        """Run a custom SQL query on the nodes table.

        Example: kg.query_nodes("node_type = ?", ("module",))
        """
        rows = self._conn.execute(
            f"SELECT id, data FROM nodes WHERE {sql_where}", params
        ).fetchall()
        results = []
        for row in rows:
            d = json.loads(row["data"])
            d["id"] = row["id"]
            results.append(d)
        return results

    def query_edges(self, sql_where: str = "1=1", params: tuple = ()) -> list[dict]:
        """Run a custom SQL query on the edges table."""
        rows = self._conn.execute(
            f"SELECT source, target, data FROM edges WHERE {sql_where}", params
        ).fetchall()
        results = []
        for row in rows:
            d = json.loads(row["data"])
            d["source"] = row["source"]
            d["target"] = row["target"]
            results.append(d)
        return results

    # ── Serialization ───────────────────────────────────────────────

    def to_json(self) -> dict:
        """Export the graph to a JSON-serializable dict."""
        G = self._build_nx()
        graph_data = nx.node_link_data(G)
        metadata = GraphMetadata(
            repo_path=self.repo_path,
            analysis_timestamp=self._created_at,
            node_count=self.node_count(),
            edge_count=self.edge_count(),
            graph_type="knowledge_graph",
            git_commit=self.get_meta("git_commit") or "",
        )
        return {
            "metadata": metadata.model_dump(),
            "graph": _make_serializable(graph_data),
        }

    def save(self, path: str | Path, graph_type: str = "knowledge_graph") -> None:
        """Save graph as JSON (for interop). The SQLite DB is the primary store."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = self.to_json()
        data["metadata"]["graph_type"] = graph_type
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    @classmethod
    def load(cls, path: str | Path) -> "KnowledgeGraph":
        """Load a graph from a JSON file into a new in-memory KG."""
        with open(path) as f:
            data = json.load(f)
        kg = cls(repo_path=data.get("metadata", {}).get("repo_path", ""))
        git_commit = data.get("metadata", {}).get("git_commit", "")
        if git_commit:
            kg.set_meta("git_commit", git_commit)
            
        graph_data = data.get("graph", {})
        G = nx.node_link_graph(graph_data)
        # Populate SQLite from the loaded graph
        for node_id, node_data in G.nodes(data=True):
            kg._conn.execute(
                "INSERT OR REPLACE INTO nodes (id, node_type, data) VALUES (?, ?, ?)",
                (node_id, node_data.get("node_type", "unknown"),
                 json.dumps(node_data, default=str)),
            )
        for u, v, edge_data in G.edges(data=True):
            kg._conn.execute(
                "INSERT INTO edges (source, target, edge_type, weight, data) VALUES (?, ?, ?, ?, ?)",
                (u, v, edge_data.get("edge_type", "UNKNOWN"),
                 edge_data.get("weight", 1.0),
                 json.dumps(edge_data, default=str)),
            )
        kg._conn.commit()
        return kg

    @property
    def db_path(self) -> str:
        """Path to the SQLite database file."""
        return self._db_path

    # ── Stats ───────────────────────────────────────────────────────

    def summary(self) -> dict:
        """Return a summary of the graph."""
        type_counts = {}
        for row in self._conn.execute(
            "SELECT node_type, COUNT(*) as cnt FROM nodes GROUP BY node_type"
        ).fetchall():
            type_counts[row["node_type"]] = row["cnt"]

        return {
            "node_count": self.node_count(),
            "edge_count": self.edge_count(),
            "sources": len(self.find_sources()),
            "sinks": len(self.find_sinks()),
            "circular_deps": len(self.strongly_connected_components()),
            "node_types": type_counts,
            "db_path": self._db_path,
        }

    # ── Internal helpers ────────────────────────────────────────────

    def set_meta(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
            (key, value),
        )
        self._conn.commit()

    def get_meta(self, key: str) -> Optional[str]:
        row = self._conn.execute(
            "SELECT value FROM metadata WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None
        
    def _set_meta(self, key: str, value: str) -> None:
        self.set_meta(key, value)

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def __del__(self):
        try:
            self._conn.close()
        except Exception:
            pass

    # ── Compatibility shim ──────────────────────────────────────────
    # The Surveyor/Hydrologist reference `self.graph` (the nx.DiGraph).
    # This property lets that code keep working.

    @property
    def graph(self) -> nx.DiGraph:
        """Compatibility: return the NetworkX graph for direct access."""
        return self._build_nx()


def _make_serializable(obj: Any) -> Any:
    """Recursively convert non-serializable objects."""
    if isinstance(obj, dict):
        return {k: _make_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_make_serializable(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, set):
        return sorted(list(obj))
    return obj
