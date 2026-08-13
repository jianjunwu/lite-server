"""E9-A: the pipeline DAG declared in Python (declaration only).

This declaration serializes to the equivalent config.yaml next to this
file; the server executes config.yaml — this file is an authoring surface
and is never run. `lite-server analyze --model pipeline` cross-checks the
declaration against the config via pure AST evaluation (drift → LS112).
"""

from lite_server import EnsembleDAG, Step

dag = EnsembleDAG(
    steps=[
        Step(
            name="preprocess",
            model="step_a",
            version="1",
            inputs={"input": "$request.input"},
        ),
        Step(
            name="postprocess",
            model="step_b",
            version="1",
            inputs={"input": "$preprocess.output"},
        ),
    ],
)
