"""Tests for the E9-A analyzer consistency rule (dag.py ↔ config.yaml).

A model directory may carry a `dag.py` that declares the ensemble DAG via
`lite_server.ensemble.EnsembleDAG`. The analyzer evaluates the declaration
via PURE AST (never executing the file) and reports drift against the
model's config.yaml as a warning (LS112) — the server executes config.yaml,
so a drifted declaration is a stale-authoring-surface signal, not a
runtime hazard.
"""

from __future__ import annotations

from pathlib import Path

from lite_server.analyzer.static import AnalysisReport, StaticAnalyzer

ENSEMBLE_CONFIG = """\
ensemble:
  steps:
    - name: a
      model: step_a
      inputs:
        q: "$request"
"""

DAG_PY_MATCHING = """\
from lite_server import EnsembleDAG, Step

dag = EnsembleDAG(
    steps=[
        Step(name="a", model="step_a", inputs={"q": "$request"}),
    ],
)
"""


def _make_model(
    repo: Path,
    name: str = "ens",
    version: str = "1",
    config: str | None = ENSEMBLE_CONFIG,
    dag_py: str | None = None,
) -> Path:
    vdir = repo / name / version
    vdir.mkdir(parents=True, exist_ok=True)
    if config is not None:
        (vdir / "config.yaml").write_text(config)
    if dag_py is not None:
        (vdir / "dag.py").write_text(dag_py)
    return vdir


def _rule_ids(report: AnalysisReport) -> list[str]:
    return [f.rule_id for f in report.findings]


def _ls112(report: AnalysisReport) -> list:
    return [f for f in report.findings if f.rule_id == "LS112"]


# ===== matching declaration =====


def test_matching_declaration_reports_no_drift(tmp_path):
    repo = tmp_path / "repo"
    _make_model(repo, dag_py=DAG_PY_MATCHING)
    report = StaticAnalyzer(repo).analyze_model("ens")
    assert _ls112(report) == []
    assert report.files["has_dag_py"] is True


def test_matching_declaration_tolerates_explicit_defaults_in_yaml(tmp_path):
    # The handwritten config spells out defaults the declaration omits —
    # canonicalization must treat the two as equal.
    config = (
        "ensemble:\n"
        "  steps:\n"
        "    - name: a\n"
        "      model: step_a\n"
        "      version: null\n"
        "      stream: false\n"
        "      params: {}\n"
        "      inputs:\n"
        "        q: \"$request\"\n"
    )
    repo = tmp_path / "repo"
    _make_model(repo, config=config, dag_py=DAG_PY_MATCHING)
    report = StaticAnalyzer(repo).analyze_model("ens")
    assert _ls112(report) == []


def test_no_dag_py_reports_nothing(tmp_path):
    repo = tmp_path / "repo"
    _make_model(repo)
    report = StaticAnalyzer(repo).analyze_model("ens")
    assert _ls112(report) == []
    assert report.files["has_dag_py"] is False


# ===== drift =====


def test_drift_in_steps_reports_warning(tmp_path):
    dag_py = DAG_PY_MATCHING.replace('model="step_a"', 'model="step_b"')
    repo = tmp_path / "repo"
    _make_model(repo, dag_py=dag_py)
    report = StaticAnalyzer(repo).analyze_model("ens")
    findings = _ls112(report)
    assert len(findings) == 1
    assert findings[0].severity == "warning"
    assert findings[0].file == "dag.py"
    assert "step_b" in findings[0].message


def test_drift_in_mimo_inputs_reports_warning(tmp_path):
    config = (
        "ensemble:\n"
        "  inputs:\n"
        "    text:\n"
        "      type: json\n"
        "  steps:\n"
        "    - name: a\n"
        "      model: step_a\n"
        "      inputs:\n"
        "        q: \"$inputs.text\"\n"
    )
    dag_py = (
        "from lite_server import EnsembleDAG, InputDecl, Step\n\n"
        "dag = EnsembleDAG(\n"
        "    inputs={\"text\": InputDecl(type=\"binary\")},\n"
        "    steps=[Step(name=\"a\", model=\"step_a\", inputs={\"q\": \"$inputs.text\"})],\n"
        ")\n"
    )
    repo = tmp_path / "repo"
    _make_model(repo, config=config, dag_py=dag_py)
    report = StaticAnalyzer(repo).analyze_model("ens")
    findings = _ls112(report)
    assert len(findings) == 1
    assert findings[0].severity == "warning"


def test_drift_in_dags_sets_reports_warning(tmp_path):
    config = (
        "ensemble:\n"
        "  dags:\n"
        "    default:\n"
        "      steps:\n"
        "        - name: main\n"
        "          model: pre\n"
        "          inputs:\n"
        "            text: \"$request.text\"\n"
    )
    dag_py = (
        "from lite_server import DagSet, EnsembleDAG, Step\n\n"
        "dag = EnsembleDAG(dags={\n"
        "    \"default\": DagSet(steps=[Step(name=\"main\", model=\"echo\", inputs={\"text\": \"$request.text\"})]),\n"
        "})\n"
    )
    repo = tmp_path / "repo"
    _make_model(repo, config=config, dag_py=dag_py)
    report = StaticAnalyzer(repo).analyze_model("ens")
    assert len(_ls112(report)) == 1


# ===== unevaluable / unsourced declarations =====


def test_dag_py_without_declaration_reports_warning(tmp_path):
    repo = tmp_path / "repo"
    _make_model(repo, dag_py="# just a comment\nx = 1\n")
    report = StaticAnalyzer(repo).analyze_model("ens")
    findings = _ls112(report)
    assert len(findings) == 1
    assert "EnsembleDAG" in findings[0].message


def test_non_literal_declaration_reports_unevaluable(tmp_path):
    dag_py = (
        "from lite_server import EnsembleDAG, Step\n\n"
        "name = \"a\"\n"
        "dag = EnsembleDAG(steps=[Step(name=name, model=\"step_a\", inputs={})])\n"
    )
    repo = tmp_path / "repo"
    _make_model(repo, dag_py=dag_py)
    report = StaticAnalyzer(repo).analyze_model("ens")
    findings = _ls112(report)
    assert len(findings) == 1
    assert findings[0].severity == "warning"


def test_unsourced_import_reports_unevaluable(tmp_path):
    dag_py = (
        "from elsewhere import EnsembleDAG\n\n"
        "dag = EnsembleDAG(steps=[])\n"
    )
    repo = tmp_path / "repo"
    _make_model(repo, dag_py=dag_py)
    report = StaticAnalyzer(repo).analyze_model("ens")
    findings = _ls112(report)
    assert len(findings) == 1


def test_dag_py_without_ensemble_config_reports_warning(tmp_path):
    repo = tmp_path / "repo"
    _make_model(repo, config="max_batch_size: 4\n", dag_py=DAG_PY_MATCHING)
    report = StaticAnalyzer(repo).analyze_model("ens")
    findings = _ls112(report)
    assert len(findings) == 1


# ===== safety: never execute dag.py =====


def test_dag_py_side_effects_are_never_executed(tmp_path):
    # A module-level side effect would fire on import/exec — the analyzer
    # must evaluate pure AST only, so the marker file must NOT appear.
    marker = tmp_path / "marker.txt"
    dag_py = (
        f"open({str(marker)!r}, 'w').write('executed')\n"
        "from lite_server import EnsembleDAG, Step\n\n"
        "dag = EnsembleDAG(steps=[Step(name=\"a\", model=\"step_a\", inputs={\"q\": \"$request\"})])\n"
    )
    repo = tmp_path / "repo"
    _make_model(repo, dag_py=dag_py)
    report = StaticAnalyzer(repo).analyze_model("ens")
    assert not marker.exists()
    # The declaration itself still cross-checks cleanly.
    assert _ls112(report) == []
