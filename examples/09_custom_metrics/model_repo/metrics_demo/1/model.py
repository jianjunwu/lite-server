"""Demo model with custom Prometheus metrics (gauge, counter, histogram)."""

import time

from lite_server import LitAPI, RequestContext


class MetricsDemoAPI(LitAPI):
    def setup(self, device):
        # Pre-register custom metrics — one-time cost
        self.g_batch_size = self.register_metric("demo_batch_size", "gauge")
        self.c_predictions = self.register_metric("demo_predictions_total", "counter")
        self.h_latency = self.register_metric("demo_inference_ms", "histogram")

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return request.get("input", 0)

    async def predict(self, x):
        # max_batch_size > 1 in config.yaml → the server batches concurrent
        # requests and x is a list here. predict has no per-request ctx on the
        # batch path; thread per-request data through decode_request instead.
        start = time.time()

        if isinstance(x, list):
            result = [{"output": item * 2} for item in x]
            batch_size = len(x)
        else:
            result = {"output": x * 2}
            batch_size = 1

        elapsed_ms = (time.time() - start) * 1000

        # Report metrics — hot path, ~50ns each
        self.report_metric(self.g_batch_size, batch_size)
        self.report_metric(self.c_predictions, 1.0)
        self.report_metric(self.h_latency, elapsed_ms)

        return result

    async def encode_response(self, output, ctx: RequestContext | None = None):
        return output
