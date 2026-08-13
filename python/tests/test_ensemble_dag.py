"""E9-A: declarative ensemble DAG authoring surface (python/lite_server/ensemble.py).

The Python declaration must serialize to a config.yaml that is byte-for-byte
equivalent (semantically) to the handwritten form the Rust orchestrator
executes — full field surface incl. MIMO (inputs / step.outputs).
"""

from __future__ import annotations

import pytest
import yaml

from lite_server.ensemble import (
    DagSet,
    EnsembleDAG,
    InputDecl,
    Step,
    StepOutput,
    canonical_ensemble_block,
)


# ===== minimal form (examples/05_ensemble pipeline shape) =====


def test_minimal_dag_serializes_like_handwritten():
    dag = EnsembleDAG(
        steps=[
            Step(name="preprocess", model="step_a", version="1",
                 inputs={"input": "$request.input"}),
            Step(name="postprocess", model="step_b", version="1",
                 inputs={"input": "$preprocess.output"}),
        ]
    )
    assert dag.to_config() == {
        "ensemble": {
            "steps": [
                {
                    "name": "preprocess",
                    "model": "step_a",
                    "version": "1",
                    "inputs": {"input": "$request.input"},
                },
                {
                    "name": "postprocess",
                    "model": "step_b",
                    "version": "1",
                    "inputs": {"input": "$preprocess.output"},
                },
            ]
        }
    }


def test_omitted_defaults_are_not_emitted():
    dag = EnsembleDAG(
        steps=[Step(name="a", model="m", inputs={"q": "$request"})]
    )
    step_cfg = dag.to_config()["ensemble"]["steps"][0]
    assert set(step_cfg) == {"name", "model", "inputs"}


# ===== full field surface (batches 0-5 released feature set) =====


def test_full_step_field_surface():
    dag = EnsembleDAG(
        steps=[
            Step(
                name="tok",
                model="pre",
                version="latest",
                inputs={"text": "$inputs.text"},
                stream=True,
                params={"temperature": 0.7},
                timeout_secs=10,
                on_error="skip",
                retries=2,
                when="$request.dag == 'default'",
                outputs={"pre": StepOutput(type="json", path="$.pre")},
            )
        ]
    )
    assert dag.to_config() == {
        "ensemble": {
            "steps": [
                {
                    "name": "tok",
                    "model": "pre",
                    "version": "latest",
                    "inputs": {"text": "$inputs.text"},
                    "stream": True,
                    "params": {"temperature": 0.7},
                    "timeout_secs": 10,
                    "on_error": "skip",
                    "retries": 2,
                    "when": "$request.dag == 'default'",
                    "outputs": {"pre": {"type": "json", "path": "$.pre"}},
                }
            ]
        }
    }


def test_block_level_output_field():
    dag = EnsembleDAG(
        steps=[Step(name="a", model="m", inputs={"q": "$request"})],
        output="$a.score",
    )
    assert dag.to_config()["ensemble"]["output"] == "$a.score"


def test_block_level_outputs_and_inputs():
    dag = EnsembleDAG(
        steps=[Step(name="a", model="m", inputs={"q": "$request"})],
        outputs={"answer": "$a"},
        inputs={"text": InputDecl(type="json")},
    )
    cfg = dag.to_config()["ensemble"]
    assert cfg["outputs"] == {"answer": "$a"}
    assert cfg["inputs"] == {"text": {"type": "json"}}


def test_output_and_outputs_are_mutually_exclusive():
    with pytest.raises(ValueError, match="mutually exclusive"):
        EnsembleDAG(
            steps=[Step(name="a", model="m", inputs={})],
            output="$a",
            outputs={"answer": "$a"},
        )


# ===== MIMO declarations (D31 InputDecl / D10 StepOutputDecl) =====


def test_input_decl_json_optional_default():
    cfg = EnsembleDAG(
        steps=[Step(name="a", model="m", inputs={"sys": "$inputs.system_prompt"})],
        inputs={
            "system_prompt": InputDecl(
                type="json", required=False, default="You are helpful."
            )
        },
    ).to_config()
    assert cfg["ensemble"]["inputs"]["system_prompt"] == {
        "type": "json",
        "required": False,
        "default": "You are helpful.",
    }


def test_input_decl_binary_full_surface():
    cfg = EnsembleDAG(
        steps=[Step(name="a", model="m", inputs={"img": "$inputs.image"})],
        inputs={
            "image": InputDecl(
                type="binary",
                content_type="image/png",
                shape=[1, 3, 224, 224],
                datatype="FP32",
            )
        },
    ).to_config()
    assert cfg["ensemble"]["inputs"]["image"] == {
        "type": "binary",
        "content_type": "image/png",
        "shape": [1, 3, 224, 224],
        "datatype": "FP32",
    }


def test_step_output_binary_marker_path():
    cfg = EnsembleDAG(
        steps=[
            Step(
                name="enc",
                model="vis_enc",
                version="1",
                inputs={"img": "$inputs.image"},
                outputs={"thumb": StepOutput(type="binary", path="$.thumb")},
            )
        ],
        inputs={"image": InputDecl(type="binary")},
    ).to_config()
    assert cfg["ensemble"]["steps"][0]["outputs"] == {
        "thumb": {"type": "binary", "path": "$.thumb"}
    }


def test_mimo_binary_chain_matches_handwritten_yaml():
    # ENS_MIMO_BIN_YAML from tests/audit_ensemble_stream.rs, declared in Python.
    dag = EnsembleDAG(
        inputs={"image": InputDecl(type="binary", content_type="image/png")},
        steps=[
            Step(name="enc", model="vis_enc", version="1",
                 inputs={"img": "$inputs.image"},
                 outputs={"thumb": StepOutput(type="binary", path="$.thumb")}),
            Step(name="crop", model="cropper", version="1",
                 inputs={"img": "$enc.thumb"},
                 outputs={"crop": StepOutput(type="binary", path="$.crop")}),
            Step(name="cls", model="classifier", version="1",
                 inputs={"img": "$crop.crop"}),
        ],
    )
    handwritten = {
        "ensemble": {
            "inputs": {"image": {"type": "binary", "content_type": "image/png"}},
            "steps": [
                {
                    "name": "enc",
                    "model": "vis_enc",
                    "version": "1",
                    "outputs": {"thumb": {"type": "binary", "path": "$.thumb"}},
                    "inputs": {"img": "$inputs.image"},
                },
                {
                    "name": "crop",
                    "model": "cropper",
                    "version": "1",
                    "outputs": {"crop": {"type": "binary", "path": "$.crop"}},
                    "inputs": {"img": "$enc.thumb"},
                },
                {
                    "name": "cls",
                    "model": "classifier",
                    "version": "1",
                    "inputs": {"img": "$crop.crop"},
                },
            ],
        }
    }
    assert dag.to_config() == handwritten


# ===== E8-1 dags form =====


def test_dags_form_with_per_set_inputs():
    dag = EnsembleDAG(
        dags={
            "default": DagSet(
                steps=[Step(name="main", model="pre", version="1",
                            inputs={"text": "$request.text"})],
            ),
            "fast": DagSet(
                steps=[Step(name="main", model="echo", version="1",
                            inputs={"data": "$request.text"})],
                inputs={"mode": InputDecl(type="json")},
            ),
        }
    )
    assert dag.to_config() == {
        "ensemble": {
            "dags": {
                "default": {
                    "steps": [
                        {
                            "name": "main",
                            "model": "pre",
                            "version": "1",
                            "inputs": {"text": "$request.text"},
                        }
                    ]
                },
                "fast": {
                    "steps": [
                        {
                            "name": "main",
                            "model": "echo",
                            "version": "1",
                            "inputs": {"data": "$request.text"},
                        }
                    ],
                    "inputs": {"mode": {"type": "json"}},
                },
            }
        }
    }


# ===== YAML serialization =====


def test_to_yaml_round_trips_through_safe_load():
    dag = EnsembleDAG(
        steps=[Step(name="a", model="m", inputs={"q": "$request.input"},
                    params={"temperature": 0.7})],
        inputs={"text": InputDecl(type="json", required=False, default="x")},
    )
    parsed = yaml.safe_load(dag.to_yaml())
    assert parsed == dag.to_config()


# ===== light validation (declaration only — Rust owns DAG validation) =====


def test_input_decl_rejects_unknown_type():
    with pytest.raises(ValueError, match="type"):
        InputDecl(type="float")


def test_input_decl_rejects_default_on_binary():
    with pytest.raises(ValueError, match="binary"):
        InputDecl(type="binary", default=b"x")


def test_input_decl_rejects_binary_hints_on_json():
    with pytest.raises(ValueError, match="json"):
        InputDecl(type="json", content_type="image/png")


def test_input_decl_rejects_non_int_shape():
    with pytest.raises(ValueError, match="shape"):
        InputDecl(type="binary", shape=[1.5])


def test_step_rejects_unknown_on_error():
    with pytest.raises(ValueError, match="on_error"):
        Step(name="a", model="m", inputs={}, on_error="ignore")


def test_step_rejects_negative_retries():
    with pytest.raises(ValueError, match="retries"):
        Step(name="a", model="m", inputs={}, retries=-1)


def test_step_output_rejects_unknown_type():
    with pytest.raises(ValueError, match="type"):
        StepOutput(type="tensor")


def test_step_rejects_non_positive_timeout_like_rust_schema():
    # The Rust schema rejects timeout_secs <= 0 at load time (E5: "must be a
    # positive, finite duration"). The module docstring promises a serialized
    # config is "always loadable" — a declaration accepting 0 (or NaN/inf)
    # would serialize a config the server refuses.
    with pytest.raises(ValueError, match="timeout_secs"):
        Step(name="a", model="m", inputs={}, timeout_secs=0)


def test_step_rejects_nan_timeout_like_rust_schema():
    with pytest.raises(ValueError, match="timeout_secs"):
        Step(name="a", model="m", inputs={}, timeout_secs=float("nan"))


def test_step_rejects_inf_timeout_like_rust_schema():
    with pytest.raises(ValueError, match="timeout_secs"):
        Step(name="a", model="m", inputs={}, timeout_secs=float("inf"))


def test_ensemble_dag_rejects_empty_dags():
    # Rust rejects "ensemble.dags declares no sets (E8-1)" at load time.
    with pytest.raises(ValueError, match="dags"):
        EnsembleDAG(dags={})


def test_input_decl_rejects_required_with_default_like_rust_schema():
    # Rust R2: "required: true conflicts with a default" is a load-time
    # Config error. The declaration must not serialize configs the server
    # cannot load.
    with pytest.raises(ValueError, match="required"):
        InputDecl(type="json", required=True, default="You are helpful.")


# ===== canonical form (analyzer drift check foundation) =====


def test_to_config_full_fills_step_defaults():
    dag = EnsembleDAG(
        steps=[Step(name="a", model="m", inputs={"q": "$request"})]
    )
    step_cfg = dag.to_config(full=True)["ensemble"]["steps"][0]
    assert step_cfg == {
        "name": "a",
        "model": "m",
        "version": None,
        "inputs": {"q": "$request"},
        "stream": False,
        "params": {},
        "timeout_secs": None,
        "on_error": None,
        "retries": None,
        "outputs": None,
        "when": None,
    }


def test_to_config_full_fills_block_defaults():
    dag = EnsembleDAG(steps=[Step(name="a", model="m", inputs={})])
    cfg = dag.to_config(full=True)["ensemble"]
    assert cfg["output"] is None
    assert cfg["inputs"] is None
    assert cfg["outputs"] is None
    assert cfg["dags"] is None


def test_canonical_ensemble_block_normalizes_minimal_yaml():
    minimal = {"steps": [{"name": "a", "model": "m", "inputs": {}}]}
    dag = EnsembleDAG(steps=[Step(name="a", model="m", inputs={})])
    assert canonical_ensemble_block(minimal) == dag.to_config(full=True)[
        "ensemble"
    ]


def test_canonical_ensemble_block_roundtrip_is_idempotent():
    full = EnsembleDAG(
        steps=[Step(name="a", model="m", inputs={"q": "$request"})],
        inputs={"t": InputDecl(type="json")},
    ).to_config(full=True)["ensemble"]
    assert canonical_ensemble_block(full) == full
