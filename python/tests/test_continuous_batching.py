"""pytest unit tests for ContinuousBatchingLoop."""

import pytest

from lite_server.worker.inference import ContinuousBatchingLoop


class MockStepAPI:
    """Mock API with predict_step for multi-step generation."""

    def __init__(self, steps_to_complete=3):
        self.steps_to_complete = steps_to_complete
        self.call_count = 0

    def decode_request(self, request):
        return request.get("value", 0)

    def predict_step(self, inputs, states):
        """Increment state each step; done when state reaches steps_to_complete."""
        self.call_count += 1
        outputs = []
        new_states = []
        dones = []
        for inp in inputs:
            state = states[len(outputs)] if states and states[len(outputs)] is not None else 0
            state += 1
            outputs.append({"result": inp + state})
            new_states.append(state)
            dones.append(state >= self.steps_to_complete)
        return outputs, new_states, dones

    def encode_response(self, output):
        return output


class MockPredictOnlyAPI:
    """Mock API with only predict (no predict_step)."""

    def decode_request(self, request):
        return request.get("value", 0)

    def predict(self, x):
        return {"result": x * 2}

    def encode_response(self, output):
        return output


class TestContinuousBatchingLoop:
    def test_add_request_decodes_input(self):
        api = MockStepAPI()
        cbl = ContinuousBatchingLoop(api, "test", 0)

        cbl.add_request({"uid": "r1", "payload": {"data": {"value": 10}}})
        assert "r1" in cbl.active
        assert cbl.active["r1"].input == 10

    def test_step_multi_step_until_done(self):
        api = MockStepAPI(steps_to_complete=3)
        cbl = ContinuousBatchingLoop(api, "test", 0)

        cbl.add_request({"uid": "r1", "payload": {"data": {"value": 10}}})

        # Step 1: not done
        completed = cbl.step()
        assert len(completed) == 0
        assert len(cbl.active) == 1
        assert api.call_count == 1

        # Step 2: not done
        completed = cbl.step()
        assert len(completed) == 0

        # Step 3: done
        completed = cbl.step()
        assert len(completed) == 1
        assert completed[0]["uid"] == "r1"
        assert completed[0]["status"]["code"] == "Ok"
        assert completed[0]["data"]["result"] == 13  # 10 + 3
        assert len(cbl.active) == 0

    def test_step_multiple_requests_interleaved(self):
        api = MockStepAPI(steps_to_complete=2)
        cbl = ContinuousBatchingLoop(api, "test", 0)

        cbl.add_request({"uid": "r1", "payload": {"data": {"value": 1}}})
        cbl.add_request({"uid": "r2", "payload": {"data": {"value": 10}}})

        # Both should be batched together in one step call
        completed = cbl.step()
        assert len(completed) == 0
        assert api.call_count == 1
        assert len(cbl.active) == 2

        # Second step completes both
        completed = cbl.step()
        assert len(completed) == 2
        uids = {c["uid"] for c in completed}
        assert uids == {"r1", "r2"}
        assert len(cbl.active) == 0

    def test_step_with_streaming_emits_chunks(self):
        api = MockStepAPI(steps_to_complete=2)
        cbl = ContinuousBatchingLoop(api, "test", 0)

        cbl.add_request({"uid": "r1", "payload": {"data": {"value": 5}, "stream": True}})

        # Step 1: not done, streaming → emit chunk
        completed = cbl.step()
        assert len(completed) == 1
        assert completed[0]["status"]["code"] == "Streaming"
        assert "r1" in cbl.active

        # Step 2: done → emit final
        completed = cbl.step()
        assert len(completed) == 1
        assert completed[0]["status"]["code"] == "Ok"
        assert "r1" not in cbl.active

    def test_fallback_to_predict_when_no_predict_step(self):
        api = MockPredictOnlyAPI()
        cbl = ContinuousBatchingLoop(api, "test", 0)

        cbl.add_request({"uid": "r1", "payload": {"data": {"value": 5}}})

        # Should complete in one step since predict always finishes
        completed = cbl.step()
        assert len(completed) == 1
        assert completed[0]["uid"] == "r1"
        assert completed[0]["data"]["result"] == 10  # 5 * 2
        assert len(cbl.active) == 0

    def test_step_empty_active_returns_empty(self):
        api = MockStepAPI()
        cbl = ContinuousBatchingLoop(api, "test", 0)
        assert cbl.step() == []

    def test_request_removed_after_completion(self):
        api = MockStepAPI(steps_to_complete=1)
        cbl = ContinuousBatchingLoop(api, "test", 0)

        cbl.add_request({"uid": "r1", "payload": {"data": {"value": 1}}})
        cbl.step()
        assert "r1" not in cbl.active

    def test_encode_response_applied_to_output(self):
        class EncodeAPI:
            def predict_step(self, inputs, states):
                return [{"raw": 42}], [None], [True]

            def encode_response(self, output):
                return {"encoded": output["raw"]}

        cbl = ContinuousBatchingLoop(EncodeAPI(), "test", 0)
        cbl.add_request({"uid": "r1", "payload": {"data": {}}})
        completed = cbl.step()
        assert completed[0]["data"] == {"encoded": 42}
