#!/usr/bin/env python3
"""KServe V2 dataplane end-to-end checks for example 25.

Two paths are exercised:
  1. Raw tensor bytes (application/octet-stream + x-tensor-* headers)
  2. Triton Binary Tensor Data Extension via the official tritonclient
     (JSON head + binary tail; binary response negotiated with
     `binary_data_output`)

Usage: start the example server, then:
    python test_kserve.py
"""

from __future__ import annotations

import json
import sys
import urllib.request

import numpy as np
import tritonclient.http as httpclient

BASE = "http://localhost:8000"


def raw_tensor() -> dict:
    """octet-stream body + x-tensor-dtype/x-tensor-shape headers → JSON summary."""
    body = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32).tobytes()
    req = urllib.request.Request(
        BASE + "/v2/models/raw_tensor/infer",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/octet-stream",
            "x-tensor-dtype": np.dtype("<f4").str,  # "<f4" = little-endian float32
            "x-tensor-shape": "4",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode())


def triton_binary() -> np.ndarray:
    """Two FP32 inputs over the Triton Binary channel; binary output negotiated."""
    client = httpclient.InferenceServerClient(url="localhost:8000")
    a = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
    b = np.array([[10.0, 20.0], [30.0, 40.0]], dtype=np.float32)
    inp_a = httpclient.InferInput("a", [2, 2], "FP32")
    inp_a.set_data_from_numpy(a)
    inp_b = httpclient.InferInput("b", [2, 2], "FP32")
    inp_b.set_data_from_numpy(b)
    out = httpclient.InferRequestedOutput("output0", binary_data=True)
    resp = client.infer("binary_sum", [inp_a, inp_b], outputs=[out])
    return resp.as_numpy("output0")


def main() -> int:
    r = raw_tensor()
    ok_raw = r.get("sum") == 10.0 and r.get("shape") == [4]
    print(f"raw tensor : sum={r.get('sum')} shape={r.get('shape')} dtype={r.get('dtype')} "
          f"{'PASS' if ok_raw else 'FAIL'}")
    if not ok_raw:
        return 1

    got = triton_binary()
    expected = np.array([[11.0, 22.0], [33.0, 44.0]], dtype=np.float32)
    ok_bin = np.array_equal(got, expected)
    print(f"triton bin : output0={got.tolist()} {'PASS' if ok_bin else 'FAIL'}")
    return 0 if ok_bin else 1


if __name__ == "__main__":
    sys.exit(main())
