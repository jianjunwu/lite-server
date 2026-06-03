"""Middleware demo: protected endpoint with auth, rate limiting, CORS, and logging.

The model provides standard inference. Custom endpoints (endpoints/status.py)
demonstrate middleware chains.
"""

from lite_server import LitAPI


class ProtectedAPI(LitAPI):
    def setup(self, device):
        self.device = device

    def decode_request(self, request):
        return request.get("input", "")

    def predict(self, x):
        if isinstance(x, list):
            return [{"output": f"protected: {item}"} for item in x]
        return {"output": f"protected: {x}"}

    def encode_response(self, output):
        return output
