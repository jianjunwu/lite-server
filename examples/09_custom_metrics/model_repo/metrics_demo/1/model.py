"""Demo model with custom Prometheus metrics (gauge, counter, histogram)."""

import time

from lite_server import LitAPI


class MetricsDemoAPI(LitAPI):
    def setup(self, device):
        # Pre-register custom metrics — one-time cost
        self.g_batch_size = self.register_metric("demo_batch_size", "gauge")
        self.c_predictions = self.register_metric("demo_predictions_total", "counter")
        self.h_latency = self.register_metric("demo_inference_ms", "histogram")

    def decode_request(self, request):
        return request.get("input", 0)

    def predict(self, x):
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

    def encode_response(self, output):
        return output
