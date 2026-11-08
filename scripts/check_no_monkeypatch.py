"""Reject patch APIs and dependency/method replacement in tests without running them.

This is a focused review guard, not whole-program analysis: it resolves imports
and simple aliases, recognizes standard patch fixtures, and rejects writes to
imported objects or stream methods. Dynamic reflection and container-carried
aliases still require review. State changes on test-owned fakes remain legal.
"""

from __future__ import annotations

import argparse
import ast
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Violation:
    line: int
    reason: str


_STREAM_METHODS = frozenset({"send_all", "receive_some", "send_eof", "aclose"})


class PolicyVisitor(ast.NodeVisitor):
    def __init__(self, tree: ast.AST) -> None:
        self.bindings: dict[str, str] = {}
        self.modules: set[str] = set()
        self.callables = (
            self.declared_callables(tree.body)
            if isinstance(tree, ast.Module)
            else set()
        )
        self.violations: list[Violation] = []

    @staticmethod
    def declared_callables(body: list[ast.stmt]) -> set[str]:
        # Do not leak method or nested-function names into unrelated scopes.
        return {
            node.name
            for node in body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        }

    def path(self, node: ast.AST) -> str:
        match node:
            case ast.Name(id=name):
                return self.bindings.get(name, name)
            case ast.Attribute(value=value, attr=attr):
                return f"{self.path(value)}.{attr}"
            case ast.Call(func=func):
                return self.path(func)
            case ast.Subscript(value=value):
                return self.path(value)
            case _:
                return ""

    def dependency(self, node: ast.AST) -> bool:
        return self.path(node).split(".")[0] in self.modules

    def annotation_path(self, node: ast.AST) -> str:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            try:
                expression = ast.parse(node.value, mode="eval").body
            except SyntaxError:
                return ""
            return self.annotation_path(expression)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            for item in (node.left, node.right):
                origin = self.annotation_path(item)
                if origin.split(".")[0] in self.modules:
                    return origin
            return ""
        path = self.path(node)
        if path.split(".")[0] not in {"typing", "typing_extensions"}:
            return path
        # Only transparent wrappers retain the annotated value's own origin.
        # Container members, Callable arguments, and Annotated metadata do not.
        if isinstance(node, ast.Subscript):
            items = (
                node.slice.elts if isinstance(node.slice, ast.Tuple) else [node.slice]
            )
            wrapper = path.rsplit(".", 1)[-1]
            if wrapper in {"Optional", "Annotated"}:
                return self.annotation_path(items[0]) if items else ""
            if wrapper == "Union":
                for item in items:
                    origin = self.annotation_path(item)
                    if origin.split(".")[0] in self.modules:
                        return origin
        return ""

    def report(self, node: ast.stmt | ast.expr, reason: str) -> None:
        self.violations.append(Violation(node.lineno, reason))

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            name = alias.asname or alias.name.split(".")[0]
            self.bindings[name] = alias.name if alias.asname else name
            self.modules.add(alias.name.split(".")[0])

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        self.modules.add(module.split(".")[0])
        for alias in node.names:
            self.bindings[alias.asname or alias.name] = f"{module}.{alias.name}"

    def patch_api(self, path: str) -> bool:
        return path == "unittest.mock.patch" or path.startswith(
            (
                "unittest.mock.patch.",
                "pytest.MonkeyPatch",
                "_pytest.monkeypatch.MonkeyPatch",
                "monkeypatch.",
                "mocker.patch",
                "mocker.spy",
            )
        )

    def method_replacement(self, name: str, value: ast.AST | None) -> bool:
        return (
            name in _STREAM_METHODS
            or isinstance(value, ast.Lambda)
            or (isinstance(value, ast.Name) and value.id in self.callables)
        )

    def bind(self, target: ast.AST, value: ast.AST | None) -> None:
        if isinstance(target, ast.Name) and value is not None:
            if self.method_replacement("", value):
                self.callables.add(target.id)
            else:
                self.callables.discard(target.id)
            path = self.path(value)
            if path:
                self.bindings[target.id] = path
            else:
                self.bindings.pop(target.id, None)

    def write(self, target: ast.AST, value: ast.AST | None = None) -> None:
        if isinstance(target, (ast.Tuple, ast.List)):
            for item in target.elts:
                self.write(item, value)
        elif isinstance(target, (ast.Attribute, ast.Subscript)):
            method = isinstance(target, ast.Attribute) and self.method_replacement(
                target.attr, value
            )
            if self.dependency(target) or method:
                self.report(target, "dependency or method replacement is forbidden")

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            self.write(target, node.value)
        self.generic_visit(node)
        for target in node.targets:
            self.bind(target, node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self.write(node.target, node.value)
        self.generic_visit(node)
        self.bind(node.target, node.value)
        if isinstance(node.target, ast.Name):
            origin = self.annotation_path(node.annotation)
            if origin.split(".")[0] in self.modules:
                self.bindings[node.target.id] = origin

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        self.write(node.target, node.value)
        self.generic_visit(node)

    def visit_Delete(self, node: ast.Delete) -> None:
        for target in node.targets:
            self.write(target)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        path = self.path(node.func)
        if self.patch_api(path):
            self.report(node, "patch APIs are forbidden; use explicit injection")
        elif path in {"setattr", "delattr"} and len(node.args) >= 2:
            target, name = node.args[:2]
            if self.dependency(target) or (
                isinstance(name, ast.Constant)
                and isinstance(name.value, str)
                and self.method_replacement(
                    name.value, node.args[2] if len(node.args) >= 3 else None
                )
            ):
                self.report(node, "dependency or method replacement is forbidden")
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.callables.add(node.name)
        outer = self.bindings.copy()
        outer_callables = self.callables.copy()
        self.callables.update(self.declared_callables(node.body))
        args = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
        if node.args.vararg is not None:
            args.append(node.args.vararg)
        if node.args.kwarg is not None:
            args.append(node.args.kwarg)
        for arg in args:
            self.bindings.pop(arg.arg, None)
            self.callables.discard(arg.arg)
            if arg.annotation is not None:
                path = self.annotation_path(arg.annotation)
                if path:
                    self.bindings[arg.arg] = path
        self.generic_visit(node)
        self.bindings = outer
        self.callables = outer_callables

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.callables.add(node.name)
        outer = self.bindings.copy()
        outer_callables = self.callables.copy()
        self.callables.update(self.declared_callables(node.body))
        self.generic_visit(node)
        self.bindings = outer
        self.callables = outer_callables


def check_source(source: str) -> tuple[Violation, ...]:
    """Return violations with 1-based lines; syntax errors also fail the gate."""
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return (Violation(exc.lineno or 1, f"syntax error: {exc.msg}"),)
    visitor = PolicyVisitor(tree)
    visitor.visit(tree)
    return tuple(visitor.violations)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", type=Path, default=[Path("tests")])
    args = parser.parse_args(argv)
    files = sorted(
        {
            file
            for path in args.paths
            for file in (path.rglob("*.py") if path.is_dir() else [path])
        }
    )
    failed = False
    for file in files:
        for violation in check_source(file.read_text()):
            print(f"{file}:{violation.line}: {violation.reason}")
            failed = True
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
