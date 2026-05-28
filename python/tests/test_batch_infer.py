"""pytest tests for BATCH_INFER handling in lite_server.worker.inference."""

import pytest

from lite_server.worker.inference import handle_batch_request


class TestHandleBatchRequest:
    def test_batch_with_batch_methods(self):
        """Model with batch/unbatch/predict processes items as a batch."""

        class BatchModel:
            def decode_request(self, request):
                return request.get("input", 0)

            def batch(self, inputs):
                return {"batched": inputs}

            def predict(self, x):
                return {"outputs": [v * 2 for v in x["batched"]]}

            def unbatch(self, output):
                return output["outputs"]

            def encode_response(self, response):
                return {"output": response}

        req = {
            "items": [
                {"uid": "b1", "data": {"input": 3}},
                {"uid": "b2", "data": {"input": 5}},
            ]
        }
        resp = handle_batch_request(BatchModel(), req)
        assert resp["type"] == "BATCH_RESPONSE"
        items = {item["uid"]: item for item in resp["items"]}
        assert items["b1"]["status"]["code"] == "Ok"
        assert items["b1"]["data"]["output"] == 6
        assert items["b2"]["status"]["code"] == "Ok"
        assert items["b2"]["data"]["output"] == 10

    def test_batch_without_batch_methods(self):
        """Model without batch/unbatch processes items individually."""

        class SimpleModel:
            def decode_request(self, request):
                return request.get("input", 0)

            def predict(self, x):
                return x * 3

            def encode_response(self, response):
                return {"output": response}

        req = {
            "items": [
                {"uid": "b3", "data": {"input": 2}},
                {"uid": "b4", "data": {"input": 4}},
            ]
        }
        resp = handle_batch_request(SimpleModel(), req)
        assert resp["type"] == "BATCH_RESPONSE"
        items = {item["uid"]: item for item in resp["items"]}
        assert items["b3"]["data"]["output"] == 6
        assert items["b4"]["data"]["output"] == 12

    def test_batch_single_item(self):
        """Batch with a single item behaves like a single INFER."""

        class SimpleModel:
            def predict(self, x):
                return x * 2

        req = {
            "items": [
                {"uid": "b5", "data": 7},
            ]
        }
        resp = handle_batch_request(SimpleModel(), req)
        items = {item["uid"]: item for item in resp["items"]}
        assert items["b5"]["data"]["output"] == 14

    def test_batch_missing_predict(self):
        """Missing predict returns error for all items."""

        class NoPredict:
            pass

        req = {
            "items": [
                {"uid": "b6", "data": {}},
                {"uid": "b7", "data": {}},
            ]
        }
        resp = handle_batch_request(NoPredict(), req)
        items = {item["uid"]: item for item in resp["items"]}
        assert items["b6"]["status"]["code"] == "Error"
        assert "predict method not found" in items["b6"]["status"]["message"]
        assert items["b7"]["status"]["code"] == "Error"

    def test_batch_predict_exception(self):
        """Predict exception is caught and returned as error per item."""

        class BrokenModel:
            def predict(self, x):
                raise ValueError("boom")

        req = {
            "items": [
                {"uid": "b8", "data": {}},
            ]
        }
        resp = handle_batch_request(BrokenModel(), req)
        items = {item["uid"]: item for item in resp["items"]}
        assert items["b8"]["status"]["code"] == "Error"
        assert "boom" in items["b8"]["status"]["message"]

    def test_batch_without_decode_encode(self):
        """Model without decode_request/encode_response passes data through."""

        class PlainModel:
            def predict(self, x):
                return {"result": x}

        req = {
            "items": [
                {"uid": "b9", "data": {"input": 1}},
                {"uid": "b10", "data": {"input": 2}},
            ]
        }
        resp = handle_batch_request(PlainModel(), req)
        items = {item["uid"]: item for item in resp["items"]}
        assert items["b9"]["data"] == {"result": {"input": 1}}
        assert items["b10"]["data"] == {"result": {"input": 2}}

    def test_batch_item_count_mismatch(self):
        """If predict returns a non-list without batch, wrap as single-item list."""

        class ScalarModel:
            def batch(self, inputs):
                return sum(inputs)

            def predict(self, x):
                return x * 2

            def unbatch(self, output):
                # Returns scalar but expects list
                return [output]

        req = {
            "items": [
                {"uid": "b11", "data": 5},
            ]
        }
        resp = handle_batch_request(ScalarModel(), req)
        items = {item["uid"]: item for item in resp["items"]}
        assert items["b11"]["data"]["output"] == 10
