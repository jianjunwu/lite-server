# CPU testbed for this example — runs the full stack without a GPU.
#
# Uses the official prebuilt vLLM CPU wheel (no source build).
#
# Build (from this directory):
#   docker build -t lite-vllm-example .
# Run (the mount reuses the host's HF cache — download the model first with
# `hf download yifan02/qwen3-0.6b-tool-distill` to avoid a cold pull):
#   docker run --rm -p 8000:8000 \
#     -v ~/.cache/huggingface:/root/.cache/huggingface \
#     lite-vllm-example
FROM python:3.12-slim

# apt via a CN mirror (build-time network is proxied but slow for deb.debian.org).
# g++ is required by torch Inductor (vLLM CPU compiles kernels at runtime).
RUN sed -i 's|deb.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list.d/debian.sources \
    && apt-get update && apt-get install -y --no-install-recommends libnuma1 libgl1 g++ \
    && rm -rf /var/lib/apt/lists/*

# vLLM 0.26.0+cpu's _C_AVX2.abi3.so is linked with an executable-stack flag
# that hardened glibc refuses to load; this tunable allows it.
ENV GLIBC_TUNABLES=glibc.rtld.execstack=2

# vLLM CPU wheel from the official index + lite-server (published as miraserver).
RUN pip install --no-cache-dir uv \
    && uv pip install --system --no-cache "vllm==0.26.0+cpu" \
       --extra-index-url https://wheels.vllm.ai/0.26.0/cpu \
       --index-strategy first-index \
       --torch-backend cpu \
    && uv pip install --system --no-cache miraserver==0.8.11

WORKDIR /app
COPY . /app/

ENTRYPOINT ["python", "-m", "lite_server", "serve", "--config", "server.yaml"]
