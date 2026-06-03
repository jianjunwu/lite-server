"""Bidirectional streaming demo: simulates real-time ASR (speech recognition).

Implements bidi_stream() returning a BidiStreamHandler with on_open(),
on_chunk(), and on_close().
"""

from lite_server import LitAPI, BidiStreamHandler


class ASRHandler(BidiStreamHandler):
    def __init__(self, model):
        self.model = model
        self.buffer = []

    def on_open(self, initial_data):
        """Called when the stream opens. Returns initial response."""
        return {"status": "ready", "sample_rate": 16000}

    def on_chunk(self, chunk):
        """Process each incoming audio chunk. Returns partial result if available."""
        text = chunk.get("text", "")
        if text:
            self.buffer.append(text)
            partial = " ".join(self.buffer)
            return {"partial": partial, "is_final": False}
        return None

    def on_close(self):
        """Called when stream closes. Returns final result."""
        result = " ".join(self.buffer)
        return {"final": result, "is_final": True, "buffer": self.buffer}


class ASRAPI(LitAPI):
    def setup(self, device):
        self.model_loaded = True

    def decode_request(self, request):
        return request

    def predict(self, x):
        return {"output": "sync fallback"}

    def encode_response(self, output):
        return output

    def bidi_stream(self):
        return ASRHandler(self)
