"""E9-A: declarative ensemble DAG authoring (declaration only).

A `dag.py` next to a model's `config.yaml` can declare the DAG as Python
objects and serialize it to the equivalent handwritten config the Rust
orchestrator executes. Nothing here RUNS a DAG — execution stays in the Rust
core (`src/ensemble.rs`), and DAG-level validation (graph refs, streaming
rules, R1-R19) is the server's job at load time. This module checks
structural/type-level well-formedness only (per-field rules mirrored from
the Rust schema where statically decidable) — cross-field and graph rules
stay with the server, and `lite-server analyze` surfaces load problems
statically.

`lite-server analyze` cross-checks a `dag.py` declaration against the model's
`config.yaml` via pure AST evaluation (never executing the file) and reports
drift as a warning finding (see `lite_server/analyzer/static.py`).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

import yaml

#: InputDecl / StepOutputDecl declared types (R1/R6: mandatory field).
_INPUT_TYPES = ("json", "binary")
#: E6 fault-tolerance modes (Rust OnErrorKind).
_ON_ERROR_KINDS = (None, "fail", "skip")

#: Step schema keys in their canonical (defaults-filled) form — used by
#: `canonical_ensemble_block` so a minimal YAML and a minimal declaration
#: compare equal in the analyzer drift check.
_STEP_DEFAULT_KEYS = (
    ("version", None),
    ("stream", False),
    ("params", {}),
    ("timeout_secs", None),
    ("on_error", None),
    ("retries", None),
    ("outputs", None),
    ("when", None),
)
_BLOCK_DEFAULT_KEYS = (
    ("output", None),
    ("inputs", None),
    ("outputs", None),
    ("dags", None),
)


@dataclass(frozen=True)
class InputDecl:
    """MIMO (D8/D31): a named root input's declaration — the static type of
    `$inputs.NAME`. `type` is mandatory; the shape/datatype fields are
    hint-only (carried onto the Binary value, never enforced by the server)."""

    type: str
    required: bool = True
    # json only (R2); a default makes the input never-absent.
    default: Any = None
    # binary only (R2): expected MIME — documentation/hint, not enforced.
    content_type: str | None = None
    shape: list[int] | None = None
    datatype: str | None = None

    def __post_init__(self) -> None:
        if self.type not in _INPUT_TYPES:
            raise ValueError(
                f"InputDecl type must be 'json' or 'binary', got {self.type!r}"
            )
        if self.type == "binary" and self.default is not None:
            raise ValueError(
                "default is not allowed on type='binary' inputs (json only, R2)"
            )
        if self.required and self.default is not None:
            raise ValueError(
                "required: true conflicts with a default (R2) — set "
                "required=False when a default is provided"
            )
        if self.type == "json" and any(
            v is not None for v in (self.content_type, self.shape, self.datatype)
        ):
            raise ValueError(
                "content_type/shape/datatype are not allowed on type='json' "
                "inputs (binary only, R2)"
            )
        if self.shape is not None and (
            not isinstance(self.shape, list)
            or any(
                isinstance(v, bool) or not isinstance(v, int) for v in self.shape
            )
        ):
            raise ValueError(f"shape must be a list of integers, got {self.shape!r}")

    def _config(self) -> dict[str, Any]:
        cfg: dict[str, Any] = {"type": self.type}
        if not self.required:
            cfg["required"] = False
        if self.default is not None:
            cfg["default"] = self.default
        if self.content_type is not None:
            cfg["content_type"] = self.content_type
        if self.shape is not None:
            cfg["shape"] = list(self.shape)
        if self.datatype is not None:
            cfg["datatype"] = self.datatype
        return cfg


@dataclass(frozen=True)
class StepOutput:
    """MIMO (D10): a step's named output projection against its single worker
    response. `type: binary` + no `path` = the whole response (non-JSON
    media_type); `type: binary` + `path` = a `$binary_b64` marker object at
    that JSON path; `type: json` = a `$.a.b`-style projection."""

    type: str
    path: str | None = None

    def __post_init__(self) -> None:
        if self.type not in _INPUT_TYPES:
            raise ValueError(
                f"StepOutput type must be 'json' or 'binary', got {self.type!r}"
            )
        if self.path is not None and not isinstance(self.path, str):
            raise ValueError(f"path must be a string, got {self.path!r}")

    def _config(self) -> dict[str, Any]:
        cfg: dict[str, Any] = {"type": self.type}
        if self.path is not None:
            cfg["path"] = self.path
        return cfg


@dataclass(frozen=True)
class Step:
    """One DAG step — the Python mirror of the Rust `EnsembleStepRaw`
    schema (batches 0-5 released field surface)."""

    name: str
    model: str
    inputs: dict[str, str]
    # E4: omitted or "latest" resolves at execution time (registry active).
    version: str | None = None
    # §4.1: tail streaming.
    stream: bool = False
    # E3: constant params merged into the assembled JSON payload.
    params: dict[str, Any] = field(default_factory=dict)
    # E5: per-step wall-clock cap (seconds); None = parent deadline only.
    timeout_secs: float | None = None
    # E6: fault tolerance — "fail" (default) or "skip".
    on_error: str | None = None
    # E6: worker-inference retries (5xx/timeouts only). Default 0 = single attempt.
    retries: int | None = None
    # MIMO (D10): step-level named outputs. None = the historical single output.
    outputs: dict[str, StepOutput] | None = None
    # E8-2: when condition — false skips the step at runtime.
    when: str | None = None

    def __post_init__(self) -> None:
        for attr in ("name", "model"):
            value = getattr(self, attr)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{attr} must be a non-empty string, got {value!r}")
        if not isinstance(self.inputs, dict) or any(
            not isinstance(k, str) or not isinstance(v, str)
            for k, v in self.inputs.items()
        ):
            raise ValueError("inputs must be a dict of str -> str references")
        if self.version is not None and not isinstance(self.version, str):
            raise ValueError(f"version must be a string, got {self.version!r}")
        if not isinstance(self.stream, bool):
            raise ValueError(f"stream must be a bool, got {self.stream!r}")
        if self.on_error not in _ON_ERROR_KINDS:
            raise ValueError(
                f"on_error must be 'fail' or 'skip', got {self.on_error!r}"
            )
        if self.retries is not None and (
            isinstance(self.retries, bool)
            or not isinstance(self.retries, int)
            or self.retries < 0
        ):
            raise ValueError(f"retries must be a non-negative int, got {self.retries!r}")
        if self.timeout_secs is not None and (
            isinstance(self.timeout_secs, bool)
            or not isinstance(self.timeout_secs, (int, float))
            or not math.isfinite(self.timeout_secs)
            or self.timeout_secs <= 0
        ):
            raise ValueError(
                f"timeout_secs must be a positive finite number, got {self.timeout_secs!r}"
            )
        if self.outputs is not None and (
            not isinstance(self.outputs, dict)
            or any(not isinstance(v, StepOutput) for v in self.outputs.values())
        ):
            raise ValueError("outputs must be a dict of str -> StepOutput")
        if self.when is not None and not isinstance(self.when, str):
            raise ValueError(f"when must be a string, got {self.when!r}")

    def _config(self) -> dict[str, Any]:
        cfg: dict[str, Any] = {
            "name": self.name,
            "model": self.model,
            "inputs": dict(self.inputs),
        }
        if self.version is not None:
            cfg["version"] = self.version
        if self.stream:
            cfg["stream"] = True
        if self.params:
            cfg["params"] = dict(self.params)
        if self.timeout_secs is not None:
            cfg["timeout_secs"] = self.timeout_secs
        if self.on_error is not None:
            cfg["on_error"] = self.on_error
        if self.retries is not None:
            cfg["retries"] = self.retries
        if self.outputs is not None:
            cfg["outputs"] = {k: v._config() for k, v in self.outputs.items()}
        if self.when is not None:
            cfg["when"] = self.when
        return cfg


@dataclass(frozen=True)
class DagSet:
    """E8-1: a named DAG set — the same field surface as the single-set
    form, validated independently by the server (R15)."""

    steps: list[Step]
    output: str | None = None
    outputs: dict[str, str] | None = None
    inputs: dict[str, InputDecl] | None = None

    def __post_init__(self) -> None:
        if not self.steps or any(not isinstance(s, Step) for s in self.steps):
            raise ValueError("steps must be a non-empty list of Step")
        if self.outputs is not None and any(
            not isinstance(v, str) for v in self.outputs.values()
        ):
            raise ValueError("outputs must be a dict of str -> str references")
        if self.inputs is not None and any(
            not isinstance(v, InputDecl) for v in self.inputs.values()
        ):
            raise ValueError("inputs must be a dict of str -> InputDecl")

    def _config(self) -> dict[str, Any]:
        cfg: dict[str, Any] = {"steps": [s._config() for s in self.steps]}
        if self.output is not None:
            cfg["output"] = self.output
        if self.outputs is not None:
            cfg["outputs"] = dict(self.outputs)
        if self.inputs is not None:
            cfg["inputs"] = {k: v._config() for k, v in self.inputs.items()}
        return cfg


@dataclass(frozen=True)
class EnsembleDAG:
    """Declarative ensemble DAG — serializes to the `ensemble:` block of a
    model's config.yaml. Single-set form (`steps`) or named-sets form
    (`dags`, E8-1) — mutually exclusive, matching the Rust schema."""

    steps: list[Step] = field(default_factory=list)
    # E2: explicit DAG output (`$stepN` / `$stepN.field`). Omitted = steps.last().
    output: str | None = None
    # MIMO (D8/D9): request-level named inputs; None = the historical single
    # anonymous input (`$request`, byte-identical legacy behavior).
    inputs: dict[str, InputDecl] | None = None
    # E7: multi-sink outputs {alias: $ref} — mutually exclusive with `output`.
    outputs: dict[str, str] | None = None
    # E8-1: named DAG sets selected via `x-lite-dag`.
    dags: dict[str, DagSet] | None = None

    def __post_init__(self) -> None:
        if any(not isinstance(s, Step) for s in self.steps):
            raise ValueError("steps must be a list of Step")
        if self.dags is not None:
            if not self.dags:
                raise ValueError("dags must declare at least one set (E8-1)")
            if self.steps:
                raise ValueError(
                    "steps must be empty in the dags form — everything lives "
                    "inside the sets (E8-1)"
                )
            if any(not isinstance(s, DagSet) for s in self.dags.values()):
                raise ValueError("dags must be a dict of str -> DagSet")
        elif not self.steps:
            raise ValueError("steps required in the single-set form (or use dags)")
        if self.output is not None and self.outputs is not None:
            raise ValueError("output and outputs are mutually exclusive (E2/E7)")
        if self.outputs is not None and any(
            not isinstance(v, str) for v in self.outputs.values()
        ):
            raise ValueError("outputs must be a dict of str -> str references")
        if self.inputs is not None and any(
            not isinstance(v, InputDecl) for v in self.inputs.values()
        ):
            raise ValueError("inputs must be a dict of str -> InputDecl")

    def _block_config(self) -> dict[str, Any]:
        if self.dags is not None:
            return {"dags": {k: v._config() for k, v in self.dags.items()}}
        cfg: dict[str, Any] = {"steps": [s._config() for s in self.steps]}
        if self.output is not None:
            cfg["output"] = self.output
        if self.inputs is not None:
            cfg["inputs"] = {k: v._config() for k, v in self.inputs.items()}
        if self.outputs is not None:
            cfg["outputs"] = dict(self.outputs)
        return cfg

    def to_config(self, full: bool = False) -> dict[str, Any]:
        """Serialize to the top-level config dict (`{"ensemble": {...}}`).
        `full=True` fills every schema default so the shape is identical to
        `canonical_ensemble_block` output (the analyzer drift-check form)."""
        cfg = {"ensemble": self._block_config()}
        if full:
            cfg["ensemble"] = canonical_ensemble_block(cfg["ensemble"])
        return cfg

    def to_yaml(self, full: bool = False) -> str:
        """Serialize to config.yaml text (minimal form = what a hand-written
        config would look like; `full=True` = defaults filled)."""
        return yaml.safe_dump(
            self.to_config(full=full), sort_keys=False, allow_unicode=True
        )


def canonical_ensemble_block(block: dict[str, Any]) -> dict[str, Any]:
    """Fill schema defaults on a parsed `ensemble:` block (from YAML or a
    declaration) so minimal and defaults-filled forms compare equal.

    Pure normalization — no validation (a malformed YAML simply compares
    unequal and surfaces as drift in the analyzer).
    """
    result: dict[str, Any] = {}
    if "dags" in block and block["dags"] is not None:
        result["dags"] = {
            name: _canonical_dag_set(set_cfg)
            for name, set_cfg in block["dags"].items()
        }
    else:
        result["steps"] = [_canonical_step(s) for s in block.get("steps", [])]
    for key, default in _BLOCK_DEFAULT_KEYS:
        if key not in result:
            result[key] = (
                _canonical_inputs(block.get(key)) if key == "inputs" else block.get(key)
            )
    return result


def _canonical_dag_set(set_cfg: dict[str, Any]) -> dict[str, Any]:
    result = {"steps": [_canonical_step(s) for s in set_cfg.get("steps", [])]}
    for key, default in _BLOCK_DEFAULT_KEYS:
        if key not in result:
            result[key] = (
                _canonical_inputs(set_cfg.get(key)) if key == "inputs" else set_cfg.get(key)
            )
    return result


def _canonical_step(step: dict[str, Any]) -> dict[str, Any]:
    result = dict(step)
    for key, default in _STEP_DEFAULT_KEYS:
        if key not in result:
            result[key] = default
    if result["outputs"] is not None:
        result["outputs"] = {
            alias: {"type": decl.get("type"), "path": decl.get("path")}
            for alias, decl in result["outputs"].items()
        }
    return result


def _canonical_inputs(inputs: dict[str, Any] | None) -> dict[str, Any] | None:
    if inputs is None:
        return None
    return {
        name: {
            "type": decl.get("type"),
            "required": decl.get("required", True),
            "default": decl.get("default"),
            "content_type": decl.get("content_type"),
            "shape": decl.get("shape"),
            "datatype": decl.get("datatype"),
        }
        for name, decl in inputs.items()
    }
