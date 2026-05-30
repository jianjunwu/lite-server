# Model Authoring Guide

This guide covers how to write model code for lite-server. Models are Python classes that implement the `LitAPI` interface.

## Quick Start

```python
from lite_server import LitAPI

class MyModel(LitAPI):
    def setup(self, device):
        """Load model weights and initialize resources."""
        self.model = load_my_model()

    def decode_request(self, request):
        """Parse the raw HTTP request body."""
        return request.get("input", "")

    def predict(self, x):
        """Run inference. Receives decoded input, returns output."""
        return self.model(x)

    def encode_response(self, output):
        """Format the prediction into an HTTP response body."""
        return {"result": output}
```

Save as `model_repo/{model_name}/{version}/model.py`.

## Directory Structure

```
model_repo/
  {model_name}/
    {version}/
      model.py          # Required: LitAPI subclass
      config.yaml        # Optional: model configuration
  orchestration.yaml     # Optional: model loading strategy
```

- `model_name`: alphanumeric, underscores, hyphens (e.g., `my_model`, `resnet-v2`)
- `version`: numeric or string (e.g., `1`, `v2`, `latest`)

## LitAPI Interface

### Required Methods

#### `setup(self, device)`

Called once when the worker starts. Load your model and any resources here.

```python
def setup(self, device):
    self.device = device
    self.model = torch.load("weights.pt", map_device=device)
    self.model.eval()
```

- `device` is a string like `"cpu"` or `"cuda:0"`
- Resources stored on `self` persist for the worker's lifetime

#### `decode_request(self, request)`

Parse the raw HTTP request body (dict from JSON) into the format your model expects.

```python
def decode_request(self, request):
    return {
        "text": request["text"],
        "max_length": request.get("max_length", 128),
    }
```

#### `predict(self, x)`

Run inference. Receives the output of `decode_request()`.

```python
def predict(self, x):
    tokens = self.tokenizer(x["text"], max_length=x["max_length"])
    return self.model(**tokens)
```

When batching is enabled (`max_batch_size > 1`), `x` is a **list** of decoded inputs:

```python
def predict(self, x):
    # x is a list when batching is active
    if isinstance(x, list):
        return [self._infer(item) for item in x]
    return self._infer(x)
```

#### `encode_response(self, output)`

Format the prediction output into an HTTP response body (must be JSON-serializable).

```python
def encode_response(self, output):
    return {"prediction": output.tolist(), "confidence": float(output.max())}
```

### Optional Methods

#### `stream_predict(self, request)`

Generator for streaming output. Each yielded value is sent as a chunk via SSE/WebSocket/gRPC.

```python
def stream_predict(self, request):
    prompt = request.get("prompt", "")
    for token in self.model.generate(prompt):
        yield {"token": token}
        time.sleep(0.02)  # simulate generation latency
```

Enable streaming in `config.yaml`:

```yaml
stream: true
```

If `stream_predict()` is not implemented, the server falls back to `predict()` and sends the result as a single chunk.

#### `on_request(self, request, meta)`

Called after `decode_request()`, before `predict()`. Use for auth, logging, or request modification.

```python
def on_request(self, request, meta):
    self.logger.info(f"Request from {meta.client_ip}: {meta.request_id}")
    if not self._check_auth(meta.headers):
        raise PermissionError("Unauthorized")
    return request
```

`meta` is a `RequestMeta` object with: `route`, `headers`, `client_ip`, `request_id`, `timestamp_ns`, `payload`.

#### `on_response(self, response, meta)`

Called after `encode_response()`, before sending to client. Use for response modification or logging. Also called in the streaming path (after each chunk is encoded).

```python
def on_response(self, response, meta):
    response["latency_ms"] = (time.time_ns() - meta.timestamp_ns) / 1_000_000
    return response
```

#### `on_file_changed(self, changed_files)`

Called when files in the model directory change (hot reload). Override to implement custom reload logic.

```python
def on_file_changed(self, changed_files):
    if any(f.endswith(".pt") for f in changed_files):
        self.logger.info("Reloading model weights...")
        self.model = torch.load("weights.pt")
```

If not overridden, the default behavior restarts the worker (re-runs `setup()`).

#### `teardown(self)`

Called when the model is unloaded. Release resources here.

```python
def teardown(self):
    del self.model
    torch.cuda.empty_cache()
```

## Continuous Batching (LLM)

For LLM workloads, enable continuous batching to process multiple sequences simultaneously with iterative generation.

```yaml
# config.yaml
continuous_batching: true
max_sequence_length: 4096
```

Implement three hooks:

```python
class LLMModel(LitAPI):
    def prefill(self, uid, decoded_input):
        """Initialize a new sequence in the KV cache."""
        tokens = self.tokenizer.encode(decoded_input["prompt"])
        self.kv_cache.add(uid, tokens)

    def step(self, active_sequences):
        """Run one generation step for all active sequences."""
        new_tokens = []
        for seq in active_sequences:
            token = self.model.generate_step(seq["uid"])
            new_tokens.append(token)
        return new_tokens

    def has_finished(self, uid, token, generated_sequence):
        """Check if a sequence is done generating."""
        return token == self.eos_token or len(generated_sequence) >= self.max_length
```

Each element in `active_sequences` has keys: `uid`, `input`, `output` (list of tokens so far).

## Batching

Enable batching to process multiple requests in a single `predict()` call:

```yaml
# config.yaml
max_batch_size: 8
batch_timeout: 0.01
adaptive_batching: true
```

When batching is active, `predict()` receives a **list** of decoded inputs:

```python
def predict(self, x):
    # x is a list of decoded inputs
    batch_input = [item["text"] for item in x]
    results = self.model(batch(batch_input))
    return [{"output": r} for r in results]  # must return list, one per input
```

Key rules:
- Return a **list** with one result per input
- The order must match the input order
- `batch_timeout` controls how long to wait for more requests (adaptive batching adjusts this automatically)

#### Custom `batch()` / `unbatch()`

Override `batch()` to reshape decoded inputs before prediction, and `unbatch()` to split batch output back into per-request responses. The full pipeline becomes:

```
decode_request → batch → predict → unbatch → encode_response
```

When only one request is queued, `batch()` and `unbatch()` are both skipped — `predict()` receives the decoded request directly.

```python
class CustomBatchModel(LitAPI):
    def decode_request(self, request):
        return {"value": request["input"], "weight": request.get("weight", 1.0)}

    def batch(self, inputs):
        """Merge decoded requests into a single batch dict."""
        return {
            "values": [x["value"] for x in inputs],
            "weights": [x["weight"] for x in inputs],
            "batch_size": len(inputs),
        }

    def predict(self, batch):
        if isinstance(batch, dict) and "values" in batch:
            # Multiple requests — came through batch()
            results = [v * w for v, w in zip(batch["values"], batch["weights"])]
            return {"results": results, "batch_size": batch["batch_size"]}
        # Single request — batch() skipped
        return {"output": batch["value"] * batch["weight"], "batch_size": 1}

    def unbatch(self, output):
        """Split batch output back into per-request responses."""
        return [
            {"output": r, "batch_size": output["batch_size"]}
            for r in output["results"]
        ]

    def encode_response(self, output):
        return output
```

See [examples/02_batching](../examples/02_batching/) for a runnable demo.

## Bidirectional Streaming

For real-time bidirectional communication (e.g., ASR):

```python
class ASRModel(LitAPI):
    def bidi_stream(self):
        class Handler:
            def on_chunk(self, chunk):
                # Process incoming audio chunk, return partial result
                return self.model.process_audio(chunk)

            def on_close(self):
                # Finalize and return final result
                return self.model.finalize()
        return Handler()
```

Enable in config:

```yaml
bidirectional: true
```

## Best Practices

### Resource Management

- Load heavy resources (model weights, tokenizers) in `setup()`, not in `predict()`
- Use `teardown()` to release GPU memory and file handles
- Store all state on `self` — workers are long-lived processes

### Error Handling

- Raise exceptions in `predict()` to signal errors — the server retries on a different worker
- Use `on_request()` for input validation — raise to reject early
- Avoid bare `except:` — let unexpected errors propagate for debugging

### Performance

- Keep `decode_request()` and `encode_response()` lightweight — they run on every request
- For batch inference, ensure `predict()` returns results in the same order as inputs
- Use `adaptive_batching: true` for variable-load workloads

### Testing

Models can be tested independently without starting the server:

```python
api = MyModel(max_batch_size=1)
api.setup("cpu")
result = api.encode_response(api.predict(api.decode_request({"input": 42})))
assert result == {"result": 84}
```

## Example: Complete Model

```python
"""Image classification model with preprocessing and batch support."""

import numpy as np
from lite_server import LitAPI

class ImageClassifier(LitAPI):
    def setup(self, device):
        self.device = device
        self.model = load_model("resnet50.pt", device=device)
        self.labels = load_labels("imagenet_labels.txt")

    def decode_request(self, request):
        # request: {"image": base64_encoded_string}
        import base64
        img_bytes = base64.b64decode(request["image"])
        return preprocess_image(img_bytes)

    def predict(self, x):
        if isinstance(x, list):
            # Batching: x is a list of preprocessed images
            batch = np.stack(x)
            outputs = self.model(batch)
            return [self._decode_output(o) for o in outputs]
        return self._decode_output(self.model(x))

    def encode_response(self, output):
        return output  # already a dict with label + confidence

    def _decode_output(self, logits):
        idx = int(np.argmax(logits))
        return {"label": self.labels[idx], "confidence": float(logits[idx])}

    def teardown(self):
        del self.model
```

## Config Reference

See [configuration.md](configuration.md) for the full model config field reference.
