"""Client-side exact token counting for streaming benchmarks (批次 4, plan §8.2).

``TokenizerCounter`` wraps a ``tokenizers.Tokenizer`` and counts tokens per
chunk from a configured text field.  Server-reported ``token_count`` chunk
meta always wins (§8.2 B4) — the tokenizer only fills in when the model
cannot be modified to report counts.

``tokenizers`` lives in the ``benchmark`` optional extra (not dev, not
runtime); both imports happen lazily inside ``load_tokenizer``.
"""

from __future__ import annotations

from pathlib import Path

from lite_server.benchmark.benchmark import StreamChunk


class TokenizerLoadError(Exception):
    """--tokenizer spec could not be loaded (missing package / bad spec)."""


def load_tokenizer(spec: str):
    """Load a ``tokenizers.Tokenizer`` from a local path or a hub id.

    Local paths (existing files) go through ``Tokenizer.from_file``;
    anything else is treated as a HuggingFace hub id (requires network).
    Raises ``TokenizerLoadError`` on failure.
    """
    try:
        from tokenizers import Tokenizer
    except ImportError as e:
        raise TokenizerLoadError(
            "the 'tokenizers' package is required for --tokenizer: "
            "pip install lite-server[benchmark]"
        ) from e
    try:
        if Path(spec).exists():
            return Tokenizer.from_file(spec)
        return Tokenizer.from_pretrained(spec)
    except Exception as e:
        raise TokenizerLoadError(f"failed to load tokenizer {spec!r}: {e}") from e


class TokenizerCounter:
    """Per-chunk token counter injected into ``run_stream(token_counter=...)``.

    Returns ``None`` for chunks whose meta already carries ``token_count``
    (server value wins) or that have no meta at all; returns 0 for chunks
    whose text field is missing/empty (tracked in ``missed_chunks``).
    """

    def __init__(self, tokenizer, text_field: str | None = None):
        self._tok = tokenizer
        self._field = text_field
        self.missed_chunks = 0

    def __call__(self, chunk: StreamChunk) -> int | None:
        meta = chunk.meta
        if not isinstance(meta, dict):
            return None
        if meta.get("token_count") is not None:
            return None  # server-reported count wins (B4)
        if self._field is not None:
            text = meta.get(self._field)
        else:
            text = meta.get("text")
            if not isinstance(text, str):
                text = meta.get("token")
        if not isinstance(text, str) or not text:
            self.missed_chunks += 1
            return 0
        return len(self._tok.encode(text).ids)
