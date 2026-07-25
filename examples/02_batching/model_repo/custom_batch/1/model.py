"""Custom batch/unbatch demo.

Shows how to override batch() and unbatch() to reshape data between
individual requests and the batched predict() call.

Pipeline: decode_request -> batch -> predict -> unbatch -> encode_response
"""

from lite_server import LitAPI, RequestContext


class CustomBatchAPI(LitAPI):
    def setup(self, device):
        self.device = device

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return {"value": request["input"], "weight": request.get("weight", 1.0)}

    def batch(self, inputs):
        """Merge decoded requests into a single batch dict.

        Instead of passing a plain list to predict(), we pack values
        into arrays — mimicking how real models stack tensors.
        """
        return {
            "values": [x["value"] for x in inputs],
            "weights": [x["weight"] for x in inputs],
            "batch_size": len(inputs),
        }

    async def predict(self, batch):
        """Run inference on the packed batch.

        When multiple requests are queued, predict() receives the dict
        returned by batch().  For a single request the framework skips
        batch() and passes the decoded request directly, so we handle
        both cases here.
        """
        if isinstance(batch, dict) and "values" in batch:
            # Came through batch()
            results = [v * w for v, w in zip(batch["values"], batch["weights"])]
            return {"results": results, "batch_size": batch["batch_size"]}
        # Single request — batch() and unbatch() are both skipped,
        # so return the final per-request format directly.
        return {"output": batch["value"] * batch["weight"], "batch_size": 1}

    def unbatch(self, output):
        """Split batch output back into per-request responses.

        The default unbatch() just returns the list unchanged.
        Here we unpack the dict and produce one response dict per request.
        """
        batch_size = output["batch_size"]
        return [
            {"output": r, "batch_size": batch_size}
            for r in output["results"]
        ]

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
