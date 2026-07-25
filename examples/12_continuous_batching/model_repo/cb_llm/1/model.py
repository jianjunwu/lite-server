"""Continuous batching demo: simulates LLM token generation.

Implements the three CB hooks: prefill(), step(), has_finished().
Each request generates a sequence of tokens, one per step.
"""

from lite_server import LitAPI, RequestContext


class CBLlmAPI(LitAPI):
    EOS_TOKEN = "<eos>"

    def setup(self, device):
        self.max_tokens = self.config.get("max_tokens_per_seq", 5)

    async def decode_request(self, request, ctx: RequestContext | None = None):
        return request.get("prompt", "")

    def prefill(self, uid, decoded_input):
        """Initialize a new sequence in the batch."""
        self.logger.info("prefill uid=%s prompt=%s", uid, decoded_input)

    def step(self, active_sequences):
        """Generate one token per active sequence.

        active_sequences elements are :class:`~lite_server.CBSequence` objects with:
          - .uid: request id
          - .input: decoded_input (the prompt string)
          - .output: list of tokens generated so far
          - .state / .meta / .ctx: per-sequence context (0.7.0)

        step() operates across sequences, so it has no single per-request ctx.
        """
        new_tokens = []
        for seq in active_sequences:
            words = seq.input.split()
            step_idx = len(seq.output)
            if step_idx < len(words):
                token = words[step_idx]
            else:
                token = f"gen_{step_idx}"
            new_tokens.append(token)
        return new_tokens

    def has_finished(self, uid, token, generated_sequence):
        """Stop when max tokens reached or EOS generated."""
        return len(generated_sequence) >= self.max_tokens or token == self.EOS_TOKEN

    async def encode_response(self, output, ctx: RequestContext | None = None):
        # output is the accumulated list of tokens
        return {"tokens": output, "text": " ".join(output)}

    def teardown(self):
        self.logger.info("cb_llm model unloaded")
