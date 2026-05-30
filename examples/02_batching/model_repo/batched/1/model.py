"""Batched model: groups requests into batches for higher throughput.

Batching is configured in config.yaml (max_batch_size, batch_timeout).
The worker process reads these values and passes them to LitAPI.__init__.
"""

from lite_server import LitAPI


class BatchedAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return request.get("input", "")

    def predict(self, inputs):
        # When batching is enabled, `inputs` is a list.
        if isinstance(inputs, list):
            return [{"output": x, "batch_size": len(inputs)} for x in inputs]
        return {"output": inputs, "batch_size": 1}

    def encode_response(self, output):
        return output
