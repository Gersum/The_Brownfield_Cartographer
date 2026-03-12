"""Multi-language AST parsing using tree-sitter.

Provides the LanguageRouter and module analysis for building
the codebase structural skeleton.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

from src.models.nodes import FunctionNode, Language, ModuleNode

# Tree-sitter language setup
_LANGUAGE_MAP = {
    ".py": Language.PYTHON,
    ".sql": Language.SQL,
    ".yml": Language.YAML,
    ".yaml": Language.YAML,
    ".js": Language.JAVASCRIPT,
    ".ts": Language.TYPESCRIPT,
    ".ipynb": Language.PYTHON,  # Analyzed as Python content
}

# Directories and files to skip
_SKIP_DIRS = {
    ".git", "__pycache__", "node_modules", ".venv", "venv",
    ".tox", ".mypy_cache", ".pytest_cache", "dist", "build",
    ".cartography", ".egg-info", ".eggs",
}

_SKIP_EXTENSIONS = {
    ".pyc", ".pyo", ".so", ".dll", ".dylib",
    ".png", ".jpg", ".jpeg", ".gif", ".ico", ".svg",
    ".woff", ".woff2", ".ttf", ".eot",
    ".zip", ".tar", ".gz", ".bz2",
    ".lock", ".log",
}


class LanguageRouter:
    """Selects the correct tree-sitter grammar based on file extension."""

    def __init__(self):
        self._parsers = {}
        self._initialized = False

    def _init_parsers(self):
        """Lazy-initialize tree-sitter parsers."""
        if self._initialized:
            return
        try:
            import tree_sitter_python as tspython
            import tree_sitter_sql as tssql
            import tree_sitter_yaml as tsyaml
            from tree_sitter import Language as TSLanguage, Parser

            self._parsers[Language.PYTHON] = Parser(TSLanguage(tspython.language()))
            self._parsers[Language.SQL] = Parser(TSLanguage(tssql.language()))
            self._parsers[Language.YAML] = Parser(TSLanguage(tsyaml.language()))
            self._initialized = True
        except Exception as e:
            print(f"[WARN] tree-sitter init failed: {e}")
            self._initialized = True  # Don't retry

    def get_parser(self, language: Language):
        """Get the tree-sitter parser for a given language."""
        self._init_parsers()
        return self._parsers.get(language)

    def detect_language(self, file_path: str | Path) -> Language:
        """Detect language from file extension."""
        ext = Path(file_path).suffix.lower()
        return _LANGUAGE_MAP.get(ext, Language.UNKNOWN)


class TreeSitterAnalyzer:
    """Analyzes source files using tree-sitter AST parsing."""

    def __init__(self):
        self.router = LanguageRouter()

    def analyze_module(self, file_path: str | Path, repo_root: str | Path) -> Optional[ModuleNode]:
        """Analyze a single source file and return a ModuleNode.
        
        Extracts imports, public functions, classes, and complexity signals.
        """
        file_path = Path(file_path)
        repo_root = Path(repo_root)

        if not file_path.exists():
            return None

        try:
            rel_path = str(file_path.relative_to(repo_root))
        except ValueError:
            rel_path = str(file_path)

        language = self.router.detect_language(file_path)

        try:
            if file_path.suffix.lower() == ".ipynb":
                from src.analyzers.notebook_analyzer import NotebookAnalyzer
                content = NotebookAnalyzer().analyze(file_path) or ""
            else:
                content = file_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return None

        lines = content.splitlines()
        loc = len(lines)
        comment_lines = sum(1 for line in lines if line.strip().startswith("#") or line.strip().startswith("--"))
        comment_ratio = comment_lines / max(loc, 1)

        if language == Language.PYTHON:
            return self._analyze_python(file_path, rel_path, content, loc, comment_ratio)
        elif language == Language.SQL:
            return self._analyze_sql(file_path, rel_path, content, loc, comment_ratio)
        elif language == Language.YAML:
            return self._analyze_yaml(file_path, rel_path, content, loc, comment_ratio)
        else:
            # Basic analysis for unsupported languages
            return ModuleNode(
                id=rel_path,
                path=rel_path,
                language=language,
                lines_of_code=loc,
                comment_ratio=comment_ratio,
            )

    def _analyze_python(self, file_path: Path, rel_path: str,
                        content: str, loc: int, comment_ratio: float) -> ModuleNode:
        """Analyze a Python file using tree-sitter AST."""
        imports = []
        public_functions = []
        public_classes = []
        complexity = 0.0

        parser = self.router.get_parser(Language.PYTHON)

        if parser:
            try:
                tree = parser.parse(content.encode("utf-8"))
                root = tree.root_node

                # Extract imports
                imports = self._extract_python_imports(root, content)
                # Extract public functions 
                public_functions = self._extract_python_functions(root, content)
                # Extract public classes
                public_classes = self._extract_python_classes(root, content)
                # Estimate complexity
                complexity = self._estimate_complexity(root, content)
            except Exception as e:
                print(f"[WARN] tree-sitter parse failed for {rel_path}: {e}")
                # Fall back to regex
                imports = self._regex_python_imports(content)
                public_functions = self._regex_python_functions(content)
                public_classes = self._regex_python_classes(content)
        else:
            # Fallback to regex-based extraction
            imports = self._regex_python_imports(content)
            public_functions = self._regex_python_functions(content)
            public_classes = self._regex_python_classes(content)

        return ModuleNode(
            id=rel_path,
            path=rel_path,
            language=Language.PYTHON,
            lines_of_code=loc,
            comment_ratio=comment_ratio,
            complexity_score=complexity,
            imports=imports,
            public_functions=public_functions,
            public_classes=public_classes,
        )

    def _extract_python_imports(self, root_node, content: str) -> list[str]:
        """Extract import statements from Python AST."""
        imports = []
        for child in self._walk(root_node):
            if child.type == "import_statement":
                # import foo, bar
                for name_node in self._walk(child):
                    if name_node.type == "dotted_name":
                        imports.append(content[name_node.start_byte:name_node.end_byte])
            elif child.type == "import_from_statement":
                # from foo import bar
                module_node = None
                for sub in child.children:
                    if sub.type == "dotted_name":
                        module_node = content[sub.start_byte:sub.end_byte]
                        break
                    elif sub.type == "relative_import":
                        module_node = content[sub.start_byte:sub.end_byte]
                        break
                if module_node:
                    imports.append(module_node)
        return imports

    def _extract_python_functions(self, root_node, content: str) -> list[str]:
        """Extract public function definitions from Python AST."""
        functions = []
        for child in root_node.children:
            if child.type == "function_definition":
                name_node = child.child_by_field_name("name")
                if name_node:
                    name = content[name_node.start_byte:name_node.end_byte]
                    if not name.startswith("_"):
                        functions.append(name)
            elif child.type == "decorated_definition":
                for sub in child.children:
                    if sub.type == "function_definition":
                        name_node = sub.child_by_field_name("name")
                        if name_node:
                            name = content[name_node.start_byte:name_node.end_byte]
                            if not name.startswith("_"):
                                functions.append(name)
        return functions

    def _extract_python_classes(self, root_node, content: str) -> list[str]:
        """Extract public class definitions from Python AST."""
        classes = []
        for child in root_node.children:
            if child.type == "class_definition":
                name_node = child.child_by_field_name("name")
                if name_node:
                    name = content[name_node.start_byte:name_node.end_byte]
                    if not name.startswith("_"):
                        classes.append(name)
            elif child.type == "decorated_definition":
                for sub in child.children:
                    if sub.type == "class_definition":
                        name_node = sub.child_by_field_name("name")
                        if name_node:
                            name = content[name_node.start_byte:name_node.end_byte]
                            if not name.startswith("_"):
                                classes.append(name)
        return classes

    def _estimate_complexity(self, root_node, content: str) -> float:
        """Estimate cyclomatic complexity from AST branching constructs."""
        complexity = 1  # Base complexity
        branch_types = {
            "if_statement", "elif_clause", "for_statement",
            "while_statement", "try_statement", "except_clause",
            "with_statement", "assert_statement",
            "conditional_expression",  # ternary
        }
        for node in self._walk(root_node):
            if node.type in branch_types:
                complexity += 1
        return float(complexity)

    def _walk(self, node):
        """Walk all nodes in the AST."""
        yield node
        for child in node.children:
            yield from self._walk(child)

    # ── Regex fallbacks ─────────────────────────────────────────────

    def _regex_python_imports(self, content: str) -> list[str]:
        """Regex-based import extraction as fallback."""
        imports = []
        for match in re.finditer(r'^import\s+(\S+)', content, re.MULTILINE):
            imports.append(match.group(1).split(',')[0].strip())
        for match in re.finditer(r'^from\s+(\S+)\s+import', content, re.MULTILINE):
            imports.append(match.group(1))
        return imports

    def _regex_python_functions(self, content: str) -> list[str]:
        """Regex-based function extraction as fallback."""
        functions = []
        for match in re.finditer(r'^def\s+([a-zA-Z][a-zA-Z0-9_]*)\s*\(', content, re.MULTILINE):
            name = match.group(1)
            if not name.startswith("_"):
                functions.append(name)
        return functions

    def _regex_python_classes(self, content: str) -> list[str]:
        """Regex-based class extraction as fallback."""
        classes = []
        for match in re.finditer(r'^class\s+([a-zA-Z][a-zA-Z0-9_]*)', content, re.MULTILINE):
            name = match.group(1)
            if not name.startswith("_"):
                classes.append(name)
        return classes

    def _analyze_sql(self, file_path: Path, rel_path: str,
                     content: str, loc: int, comment_ratio: float) -> ModuleNode:
        """Basic analysis for SQL files."""
        complexity = 0.0
        parser = self.router.get_parser(Language.SQL)
        if parser:
            try:
                tree = parser.parse(content.encode("utf-8"))
                root = tree.root_node
                # Count statement-like nodes as a coarse complexity proxy
                complexity = float(
                    sum(1 for node in self._walk(root) if node.type.endswith("_statement"))
                )
            except Exception as e:
                print(f"[WARN] tree-sitter SQL parse failed for {rel_path}: {e}")
        return ModuleNode(
            id=rel_path,
            path=rel_path,
            language=Language.SQL,
            lines_of_code=loc,
            comment_ratio=comment_ratio,
            complexity_score=complexity,
        )

    def _analyze_yaml(self, file_path: Path, rel_path: str,
                      content: str, loc: int, comment_ratio: float) -> ModuleNode:
        """Basic analysis for YAML files."""
        return ModuleNode(
            id=rel_path,
            path=rel_path,
            language=Language.YAML,
            lines_of_code=loc,
            comment_ratio=comment_ratio,
        )


def discover_files(repo_root: str | Path) -> list[Path]:
    """Discover all analyzable source files in a repository."""
    repo_root = Path(repo_root)
    files = []

    for root, dirs, filenames in os.walk(repo_root):
        # Filter out skip directories in-place
        dirs[:] = [d for d in dirs if d not in _SKIP_DIRS and not d.startswith(".")]

        for fname in filenames:
            fpath = Path(root) / fname
            ext = fpath.suffix.lower()
            if ext in _SKIP_EXTENSIONS:
                continue
            if ext in _LANGUAGE_MAP:
                files.append(fpath)

    return sorted(files)


def resolve_python_import(import_name: str, current_file: str, repo_root: str | Path) -> Optional[str]:
    """Resolve a Python import name to a file path relative to repo root.
    
    Handles:
    - Relative imports (., ..)
    - Absolute imports (package.module)
    """
    repo_root = Path(repo_root)

    # Handle relative imports
    if import_name.startswith("."):
        current_dir = Path(current_file).parent
        # Count leading dots
        dots = len(import_name) - len(import_name.lstrip("."))
        module_part = import_name.lstrip(".")
        
        # Navigate up directories
        base_dir = current_dir
        for _ in range(dots - 1):
            if base_dir == base_dir.parent: # Reached root
                break
            base_dir = base_dir.parent

        if module_part:
            parts = module_part.split(".")
            candidate_rel = base_dir.joinpath(*parts)
        else:
            candidate_rel = base_dir

        # Check if it's a module (.py) or package (__init__.py)
        # 1. module.py
        py_file = repo_root / candidate_rel.with_suffix(".py")
        if py_file.exists() and py_file.is_file():
            return str(py_file.relative_to(repo_root))
            
        # 2. module/__init__.py
        init_file = repo_root / candidate_rel / "__init__.py"
        if init_file.exists() and init_file.is_file():
            return str(init_file.relative_to(repo_root))

        return None

    # Handle absolute imports
    parts = import_name.split(".")
    
    # Try as a direct module file or package at different depths
    for i in range(len(parts), 0, -1):
        candidate_rel = Path(*parts[:i])
        
        # Try .py
        py_path = repo_root / candidate_rel.with_suffix(".py")
        if py_path.exists() and py_path.is_file():
            return str(py_path.relative_to(repo_root))
        
        # Try /__init__.py
        init_path = repo_root / candidate_rel / "__init__.py"
        if init_path.exists() and init_path.is_file():
            return str(init_path.relative_to(repo_root))

    return None  # External package or unresolvable
