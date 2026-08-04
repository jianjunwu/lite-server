"""Static model analysis: inspect model.py, config.yaml, requirements.txt.

Pure-AST analysis — user code is NEVER executed (no importlib exec_module).
"Model loading is code execution" (CVE-2026-40156 et al.): the default path
must be zero-execution. Detection limits (dynamic imports, metaclasses) are
surfaced as ``unresolved`` info findings instead of silent false negatives.

Output contract: analyze JSON schema v1 (see AnalysisReport.to_dict).
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from packaging.requirements import InvalidRequirement, Requirement

from lite_server import validate_model_config

LITAPI_ROOT = "lite_server.LitAPI"

#: severity ordering for exit-code thresholds
_SEVERITY_RANK = {"info": 0, "warning": 1, "error": 2}

# method groups reported in schema v1 (methods absent from the class body are
# "default" — the LitAPI base provides them; only predict is truly required)
_CORE_REQUIRED = ("setup", "predict")
_CODEC = ("decode_request", "encode_response")
_BATCHING = ("batch", "unbatch")
_STREAMING = ("stream_predict", "predict_decoupled")
_OPS_HOOKS = ("teardown", "on_file_changed")


@dataclass
class Finding:
    """One rule hit. rule_id is frozen once published (CI baseline may cite it)."""

    rule_id: str
    severity: str  # "error" | "warning" | "info"
    message: str
    hint: str | None = None
    file: str | None = None
    line: int | None = None

    def to_dict(self) -> dict:
        return {
            "rule_id": self.rule_id,
            "severity": self.severity,
            "location": {"file": self.file, "line": self.line},
            "message": self.message,
            "hint": self.hint,
        }


@dataclass
class AnalysisReport:
    """Authoritative analyze data model (schema v1 sections minus envelope)."""

    model_name: str
    requested_version: str | None = None
    resolved_version: str | None = None
    versions_found: list[str] = field(default_factory=list)
    implicit_latest: bool = False
    executed_user_code: bool = False
    api_class: dict | None = None
    methods: dict = field(default_factory=dict)
    files: dict = field(default_factory=dict)
    config: dict = field(default_factory=dict)
    dependencies: list[str] = field(default_factory=list)
    findings: list[Finding] = field(default_factory=list)
    checks_passed: list[str] = field(default_factory=list)

    def severity_counts(self) -> dict[str, int]:
        counts = {"error": 0, "warning": 0, "info": 0}
        for f in self.findings:
            counts[f.severity] += 1
        return counts

    def exit_code(self, fail_severity: str = "error") -> int:
        """0 = no finding at/above threshold, 1 = at least one. (2 is reserved
        for analysis failure and is raised as an exception before this point.)"""
        threshold = _SEVERITY_RANK[fail_severity]
        for f in self.findings:
            if _SEVERITY_RANK[f.severity] >= threshold:
                return 1
        return 0

    def to_dict(self, tool_version: str, command: str) -> dict:
        """JSON schema v1 — the single authoritative representation."""
        counts = self.severity_counts()
        return {
            "schema_version": 1,
            "tool_version": tool_version,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "command": command,
            "target": {
                "model_name": self.model_name,
                "requested_version": self.requested_version,
                "resolved_version": self.resolved_version,
                "executed_user_code": self.executed_user_code,
            },
            "summary": {
                "errors": counts["error"],
                "warnings": counts["warning"],
                "infos": counts["info"],
                "checks_passed": len(self.checks_passed),
            },
            "versions": {
                "found": self.versions_found,
                "resolved": self.resolved_version,
                "implicit_latest": self.implicit_latest,
            },
            "api_class": self.api_class,
            "methods": self.methods,
            "files": self.files,
            "config": self.config,
            "dependencies": self.dependencies,
            "findings": [f.to_dict() for f in self.findings],
            "checks_passed": self.checks_passed,
        }


class StaticAnalyzer:
    """Analyze a model repository without executing any model code."""

    def __init__(self, repo_path: Path | str):
        self.repo_path = Path(repo_path)
        if not self.repo_path.exists():
            raise ValueError(f"Repository path does not exist: {self.repo_path}")
        self._repo_root = self.repo_path.resolve()

    def analyze_model(self, model_name: str, version: str | None = None) -> AnalysisReport:
        """Return the schema v1 analysis report for a model.

        Raises (→ CLI exit code 2, "analysis failed"):
            ValueError: invalid model name / path escapes the repository root.
            FileNotFoundError: model dir, requested version, or any version
                directory not found.
        """
        if (
            not model_name
            or "/" in model_name
            or "\\" in model_name
            or model_name in (".", "..")
        ):
            raise ValueError(f"Invalid model name: {model_name!r}")
        model_dir = self.repo_path / model_name
        # Whitelist: resolve() collapses ".." and symlinks; anything landing
        # outside the repo root is rejected before any file is read.
        resolved_model_dir = model_dir.resolve()
        if not resolved_model_dir.is_relative_to(self._repo_root):
            raise ValueError(
                f"Model path escapes repository root: {model_name!r}"
            )
        if not resolved_model_dir.is_dir():
            raise FileNotFoundError(f"Model not found: {model_name!r}")

        report = AnalysisReport(
            model_name=model_name,
            requested_version=version,
        )

        # --- version discovery -------------------------------------------------
        version_dirs = [d for d in resolved_model_dir.iterdir() if d.is_dir()]
        report.versions_found = sorted(
            (d.name for d in version_dirs), key=_version_sort_key
        )
        if not version_dirs:
            raise FileNotFoundError(
                f"No version directories found under model {model_name!r} "
                f"(expected e.g. '1')"
            )
        if version is not None:
            if version not in report.versions_found:
                raise FileNotFoundError(
                    f"version {version!r} not found for model {model_name!r} "
                    f"(available: {', '.join(report.versions_found)})"
                )
            resolved = version
            report.checks_passed.append("version-explicit")
        else:
            resolved = _latest_version(report.versions_found)
            report.implicit_latest = True
            report.findings.append(Finding(
                "LS111", "warning",
                f"未指定 --version，已按 latest(1) 解析到版本 {resolved}",
                hint="生产环境建议显式 --version 锁定，避免新增版本导致静默漂移",
            ))
        report.resolved_version = resolved
        version_dir = resolved_model_dir / resolved

        # --- config.yaml ---------------------------------------------------------
        config_path = version_dir / "config.yaml"
        report.files["has_config"] = config_path.exists()
        config: dict[str, Any] = {}
        if config_path.exists():
            config = self._check_config(config_path, report)
        report.config = config

        # --- model.py (pure AST) -------------------------------------------------
        model_py = version_dir / "model.py"
        report.files["has_model_py"] = model_py.exists()
        py_files = sorted(version_dir.glob("*.py")) + sorted(resolved_model_dir.glob("*.py"))
        if model_py.exists():
            self._analyze_sources(py_files, config, report)

        # --- requirements.txt ----------------------------------------------------
        req_file = resolved_model_dir / "requirements.txt"
        report.files["has_requirements"] = req_file.exists()
        if req_file.exists():
            self._check_requirements(req_file, report)

        return report

    # ------------------------------------------------------------------ config

    def _check_config(self, config_path: Path, report: AnalysisReport) -> dict:
        """Parse config.yaml for display + conditional rules; delegate type
        validation to the Rust serde path (single source of truth)."""
        try:
            parsed = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        except yaml.YAMLError as e:
            report.findings.append(Finding(
                "LS004", "error", f"config.yaml 解析失败: {e}",
                file=config_path.name,
            ))
            return {}
        if not isinstance(parsed, dict):
            report.findings.append(Finding(
                "LS004", "error",
                f"config.yaml 顶层必须是 mapping，实际为 {type(parsed).__name__}",
                file=config_path.name,
            ))
            return {}
        if validate_model_config is None:
            # Rust extension unavailable — type validation skipped, but the
            # file parsed fine; do not fail the report for the environment.
            report.checks_passed.append("config-yaml-parseable")
        else:
            try:
                validate_model_config(str(config_path))
            except Exception as e:
                report.findings.append(Finding(
                    "LS004", "error", f"config.yaml 校验失败: {e}",
                    hint="修正后可用 lite-server config-check 复核",
                    file=config_path.name,
                ))
                return parsed
            report.checks_passed.append("config-yaml-valid")
        return parsed

    # ------------------------------------------------------------- requirements

    def _check_requirements(self, req_file: Path, report: AnalysisReport) -> None:
        bad = 0
        for lineno, line in enumerate(
            req_file.read_text(encoding="utf-8").splitlines(), start=1
        ):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            try:
                Requirement(stripped)
            except InvalidRequirement:
                bad += 1
                report.findings.append(Finding(
                    "LS104", "warning",
                    f"requirements.txt 第 {lineno} 行无法解析: {stripped!r}",
                    file=req_file.name, line=lineno,
                ))
            else:
                report.dependencies.append(stripped)
        if bad == 0:
            report.checks_passed.append("requirements-parseable")

    # -------------------------------------------------------------------- AST

    def _analyze_sources(
        self, py_files: list[Path], config: dict, report: AnalysisReport
    ) -> None:
        """Two-phase AST analysis: (1) build per-file import alias tables and
        the repo class table; (2) resolve LitAPI subclasses via transitive
        closure over the class table. Never executes user code."""
        classes: dict[str, dict] = {}
        syntax_failed = False
        for path in py_files:
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except (SyntaxError, UnicodeDecodeError) as e:
                syntax_failed = True
                line = getattr(e, "lineno", None)
                report.findings.append(Finding(
                    "LS005", "error", f"{path.name} 语法错误: {e}",
                    file=path.name, line=line,
                ))
                continue
            aliases = _build_alias_table(tree)
            for node in tree.body:
                if isinstance(node, ast.ClassDef):
                    classes[node.name] = {
                        "node": node,
                        "file": path.name,
                        "aliases": aliases,
                    }
        if not syntax_failed:
            report.checks_passed.append("model-py-syntax-ok")

        repo_modules = {p.stem for p in py_files}

        # phase 2: classify every class
        hits: list[tuple[str, str]] = []  # (class name, confidence)
        unresolved_candidates: list[str] = []
        for name in classes:
            confidence = self._confidence(name, classes, repo_modules, seen=frozenset())
            if confidence in ("exact", "transitive"):
                hits.append((name, confidence))
            elif confidence == "unresolved-candidate":
                unresolved_candidates.append(name)

        # Leaf filter: a repo-internal base class is itself a LitAPI subclass
        # (that's what makes it a base), so naive counting always reports >1
        # for layered designs. Count only classes not used as a base by
        # another repo class — the most-derived class is the served model.
        used_as_base = {
            ref
            for info in classes.values()
            for base in info["node"].bases
            for kind, ref in [
                _resolve_class_ref(base, info["aliases"], classes, repo_modules)
            ]
            if kind == "class"
        }
        leaf_hits = [h for h in hits if h[0] not in used_as_base]
        if leaf_hits:
            hits = leaf_hits

        if len(hits) != 1:
            if hits:
                names = ", ".join(n for n, _ in hits)
                report.findings.append(Finding(
                    "LS002", "error",
                    f"命中 {len(hits)} 个 LitAPI 子类（应恰好 1 个）: {names}",
                    hint="每个模型版本目录只保留一个 LitAPI 子类",
                ))
            else:
                report.findings.append(Finding(
                    "LS002", "error",
                    "未找到 LitAPI 子类",
                    hint="模型应定义一个继承 lite_server.LitAPI 的类",
                ))
                for candidate in unresolved_candidates:
                    report.findings.append(Finding(
                        "LS202", "info",
                        f"possible LitAPI subclass (unresolved base): {candidate}",
                        file=classes[candidate]["file"],
                        line=classes[candidate]["node"].lineno,
                    ))
            report.api_class = None
            report.methods = _empty_method_groups()
            return

        report.checks_passed.append("exactly-one-litapi-subclass")
        name, confidence = hits[0]
        info = classes[name]
        report.api_class = {
            "name": name,
            "bases": [_base_source(b) for b in info["node"].bases],
            "confidence": confidence,
            "location": {"file": info["file"], "line": info["node"].lineno},
        }

        methods = self._collect_methods(name, classes, repo_modules, seen=frozenset())
        self._check_methods(methods, config, report)

    def _confidence(
        self,
        name: str,
        classes: dict[str, dict],
        repo_modules: set[str],
        seen: frozenset,
    ) -> str | None:
        """exact = direct base resolves to lite_server.LitAPI; transitive = via
        a repo-internal intermediate class; unresolved-candidate = has a base
        we cannot statically resolve; None = not a LitAPI subclass."""
        if name in seen or name not in classes:
            return None
        info = classes[name]
        bases = info["node"].bases

        for base in bases:
            kind, ref = _resolve_class_ref(base, info["aliases"], classes, repo_modules)
            if kind == "litapi":
                return "exact"
        has_unresolved = False
        for base in bases:
            kind, ref = _resolve_class_ref(base, info["aliases"], classes, repo_modules)
            if kind == "class":
                c = self._confidence(ref, classes, repo_modules, seen | {name})
                if c in ("exact", "transitive"):
                    return "transitive"
            elif kind == "unresolved":
                has_unresolved = True
        return "unresolved-candidate" if has_unresolved else None

    def _collect_methods(
        self,
        name: str,
        classes: dict[str, dict],
        repo_modules: set[str],
        seen: frozenset,
    ) -> dict[str, ast.FunctionDef]:
        """Method table of the hit class, merging repo-internal ancestors
        (subclass definitions win)."""
        info = classes[name]
        methods: dict[str, ast.FunctionDef] = {}
        for node in info["node"].body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                methods.setdefault(node.name, node)
        for base in info["node"].bases:
            kind, ref = _resolve_class_ref(base, info["aliases"], classes, repo_modules)
            if kind == "class" and ref not in seen:
                for m, fn in self._collect_methods(
                    ref, classes, repo_modules, seen | {name}
                ).items():
                    methods.setdefault(m, fn)
        return methods

    def _check_methods(
        self, methods: dict[str, ast.FunctionDef], config: dict, report: AnalysisReport
    ) -> None:
        impl = set(methods)

        def status(name: str) -> str:
            if name in impl:
                return "implemented"
            return "missing" if name == "predict" else "default"

        # --- LS001 / LS102: core contract (predict is the only truly abstract)
        if "predict" in impl:
            report.checks_passed.append("predict-implemented")
        else:
            report.findings.append(Finding(
                "LS001", "error",
                "predict 未实现（LitAPI 基类抛 NotImplementedError）",
                hint="实现 predict(self, x) 作为推理入口",
            ))
        if "setup" in impl:
            report.checks_passed.append("setup-implemented")
        else:
            report.findings.append(Finding(
                "LS102", "warning",
                "setup 未覆写（基类默认 pass，模型初始化逻辑不会执行）",
                hint="如需加载权重/初始化资源，实现 setup(self, device)",
            ))

        # --- conditional: batching (driven by config.yaml, the authoritative
        # source of constructor parameters in lite-server)
        max_batch_size = config.get("max_batch_size", 1)
        batching_required = (
            isinstance(max_batch_size, int) and max_batch_size > 1
        )
        if batching_required and "batch" not in impl and "unbatch" not in impl:
            report.findings.append(Finding(
                "LS101", "warning",
                f"max_batch_size={max_batch_size} 但 batch/unbatch 均未覆写，"
                f"默认启发式可能不适用",
                hint="覆写 batch(inputs) 与 unbatch(outputs)，或将 max_batch_size 降为 1",
            ))

        # --- conditional: streaming
        stream_required = config.get("stream") is True
        stream_fn = methods.get("stream_predict")
        if stream_required and (stream_fn is None or not _is_generator(stream_fn)):
            report.findings.append(Finding(
                "LS103", "warning",
                "stream=true 但 stream_predict 未覆写或非 generator",
                hint="实现 def stream_predict(self, request): yield ...",
            ))

        # --- ops hooks (info only)
        if not {"teardown", "on_file_changed"} & impl:
            report.findings.append(Finding(
                "LS201", "info",
                "teardown/on_file_changed 生命周期钩子未覆写，均使用基类默认实现",
                hint="需要释放资源或热更新响应时可覆写",
            ))

        report.methods = {
            "core_required": {m: status(m) for m in _CORE_REQUIRED},
            "codec": {m: status(m) for m in _CODEC},
            "batching": {
                **{m: status(m) for m in _BATCHING},
                "required_by": (
                    f"max_batch_size={max_batch_size}" if batching_required else None
                ),
            },
            "streaming": {
                **{m: status(m) for m in _STREAMING},
                "required_by": "stream=true" if stream_required else None,
            },
            "ops_hooks": {m: status(m) for m in _OPS_HOOKS},
        }

    # ------------------------------------------------------------------ misc

    def list_models(self) -> list[str]:
        """Return list of model names in the repository."""
        return sorted([
            d.name for d in self.repo_path.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ])


# ---------------------------------------------------------------------------
# module helpers
# ---------------------------------------------------------------------------


def _empty_method_groups() -> dict:
    return {
        "core_required": {m: "missing" for m in _CORE_REQUIRED},
        "codec": {m: "default" for m in _CODEC},
        "batching": {**{m: "default" for m in _BATCHING}, "required_by": None},
        "streaming": {**{m: "default" for m in _STREAMING}, "required_by": None},
        "ops_hooks": {m: "default" for m in _OPS_HOOKS},
    }


def _version_sort_key(name: str) -> tuple[int, str]:
    """Numeric-aware ordering: 1 < 2 < 10; 'v1' by its numeric tail;
    non-numeric names sort lexicographically below all numeric ones."""
    if name.isdigit():
        return (int(name), "")
    tail = name[1:] if name[:1].lower() == "v" else ""
    if tail.isdigit():
        return (int(tail), "")
    return (-1, name)


def _latest_version(names: list[str]) -> str:
    """Triton latest(1): numerically greatest (non-numeric names accepted by
    the server scanner rank below numeric ones)."""
    return max(names, key=_version_sort_key)


def _build_alias_table(tree: ast.Module) -> dict[str, str]:
    """local name -> dotted path for module-level imports."""
    aliases: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.Import):
            for a in node.names:
                aliases[a.asname or a.name.split(".")[0]] = a.name
        elif isinstance(node, ast.ImportFrom) and node.module:
            for a in node.names:
                aliases[a.asname or a.name] = f"{node.module}.{a.name}"
    return aliases


def _resolve_class_ref(
    expr: ast.expr,
    aliases: dict[str, str],
    classes: dict[str, dict],
    repo_modules: set[str],
) -> tuple[str, str | None]:
    """Classify a base-class expression:
    ("litapi", None)      — resolves to lite_server.LitAPI
    ("class", name)       — repo-internal class (recurse for transitive)
    ("external", dotted)  — resolvable but not LitAPI
    ("unresolved", None)  — cannot be statically resolved
    """
    if isinstance(expr, ast.Name):
        if expr.id in classes:
            return ("class", expr.id)
        dotted = aliases.get(expr.id)
        if dotted == LITAPI_ROOT:
            return ("litapi", None)
        if dotted is not None:
            mod, _, attr = dotted.rpartition(".")
            if mod in repo_modules and attr in classes:
                return ("class", attr)
            return ("external", dotted)
        return ("external", expr.id)
    if isinstance(expr, ast.Attribute):
        parts: list[str] = []
        e = expr
        while isinstance(e, ast.Attribute):
            parts.append(e.attr)
            e = e.value
        if isinstance(e, ast.Name):
            root = aliases.get(e.id, e.id)
            chain = list(reversed(parts))
            dotted = ".".join([root] + chain)
            if dotted == LITAPI_ROOT:
                return ("litapi", None)
            if root in repo_modules and len(chain) == 1 and chain[0] in classes:
                return ("class", chain[0])
            return ("external", dotted)
    return ("unresolved", None)


def _base_source(expr: ast.expr) -> str:
    """Human-readable source of a base-class expression."""
    try:
        return ast.unparse(expr)
    except Exception:
        return "?"


def _is_generator(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """True if the function body yields (nested scopes excluded)."""
    stack = list(fn.body)
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.Yield, ast.YieldFrom)):
            return True
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            continue
        stack.extend(ast.iter_child_nodes(node))
    return False
