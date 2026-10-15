"""/audit (2026-10-15, feat 2aa66fd): D42/D43 Python declaration mirror.

The D42/D43 step keys are mirrored in `lite_server.ensemble` so that
`lite-server analyze` can cross-check a `dag.py` declaration against
`config.yaml` (LS112 drift). Three gaps found in that mirror and fixed here:

1. `forward_headers` canonicalized to `None` while an empty allowlist is
   omitted from the serialized form — so `[]` and "key absent" compared
   unequal even though the Rust loader (`Vec<String>` + `#[serde(default)]`)
   treats them as the same plan.
2. The R23 validation normalized names to lowercase but the emitter kept the
   raw spelling, so a case-only difference (behaviourally identical
   server-side) reported drift.
3. The regression test added for 1./2. compared two config WRAPPERS, both of
   which lost their step list, so it could never fail.

These tests are the regression pins: they fail on the pre-fix mirror and pass
on the fixed one.
"""

from __future__ import annotations

import yaml

from lite_server.ensemble import EnsembleDAG, Step, canonical_ensemble_block


def _canonical_config(text: str) -> dict:
    """The analyzer's own comparison form (`analyzer/static.py`)."""
    return canonical_ensemble_block(yaml.safe_load(text)["ensemble"])


def _canonical_declaration(dag: EnsembleDAG) -> dict:
    """The analyzer's own declaration form (`analyzer/static.py`)."""
    return canonical_ensemble_block(dag.to_config(full=True)["ensemble"])


# ===== 1. empty allowlist vs omitted key =====


def test_audit_empty_forward_headers_must_not_report_drift():
    """`forward_headers: []` and an omitted key are the same allowlist — the
    Rust plan stores both as an empty `Vec` and the executor treats them
    identically — so the drift check must not distinguish them."""
    declaration = EnsembleDAG(
        steps=[Step(name="a", model="m", inputs={"q": "$request"}, forward_headers=[])]
    )
    on_disk = """
ensemble:
  steps:
    - name: a
      model: m
      inputs: {q: "$request"}
      forward_headers: []
"""
    assert _canonical_declaration(declaration) == _canonical_config(on_disk), (
        "an empty allowlist must canonicalize like an omitted one"
    )


# ===== 2. case normalization =====


def test_audit_forward_headers_must_be_emitted_normalized():
    """R23 lowercases the allowlist at load on both sides, so a declaration
    that spells a name in mixed case describes the same config as the
    lowercase spelling — the serialized form must not report drift."""
    declaration = EnsembleDAG(
        steps=[
            Step(
                name="a",
                model="m",
                inputs={"q": "$request"},
                forward_headers=["X-Tenant-Id"],
            )
        ]
    )
    on_disk = """
ensemble:
  steps:
    - name: a
      model: m
      inputs: {q: "$request"}
      forward_headers: [x-tenant-id]
"""
    assert _canonical_declaration(declaration) == _canonical_config(on_disk), (
        "names are validated case-insensitively but emitted case-preserving"
    )


# ===== 3. the wrapper-vs-block trap behind the vacuous regression test =====


def test_audit_canonical_block_requires_the_ensemble_block():
    """`canonical_ensemble_block` reads `steps` at the TOP level
    (`ensemble.py`), so passing the `{"ensemble": {...}}` wrapper silently
    yields an empty step list. Comparing two wrappers therefore holds no
    matter what the step defaults are — the trap that made
    `test_new_step_keys_do_not_create_false_drift` vacuous. Pin the analyzer's
    own form (unwrap `["ensemble"]`) as the one that carries steps.
    """
    declaration = EnsembleDAG(steps=[Step(name="a", model="m", inputs={"q": "$request"})])

    wrapper = canonical_ensemble_block(declaration.to_config())
    assert wrapper["steps"] == [], (
        "the config wrapper has no top-level steps — comparing wrappers is vacuous"
    )

    analyzer_form = canonical_ensemble_block(declaration.to_config(full=True)["ensemble"])
    assert len(analyzer_form["steps"]) == 1, (
        "the drift check must compare blocks that actually carry the steps"
    )
