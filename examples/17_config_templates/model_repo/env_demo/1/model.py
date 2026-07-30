"""Environment-aware model: reads configuration from os.environ and self.config.

Demonstrates three patterns for externalising configuration from code:

1. **os.environ** — read in ``setup()`` for one-time init (model backend, log
   verbosity, feature flags). Override at process startup.  Suitable for
   secrets and per-deployment values that should never live in YAML.

2. **self.config** — custom fields in config.yaml.  Tunable per model without
   touching Python.  Change + reload is faster than re-deploying.

3. **${VAR} in policies.auth.keys** — framework-native env-var expansion for
   auth keys.  Fail-closed: an unset variable prevents the model from loading.
"""

import os

from lite_server import LitAPI, RequestContext


class EnvDemoAPI(LitAPI):

    def setup(self, device):
        self.device = device

        # ── os.environ: one-time init, read once at worker start ──
        self.backend = os.environ.get("DEMO_BACKEND", "cpu")
        self.log_verbose = os.environ.get("DEMO_LOG_VERBOSE", "0") == "1"

        # ── self.config: custom YAML fields, read on every worker start ──
        self.greeting = self.config.get("greeting", "hello")
        self.version_label = self.config.get("version_label", "dev")

    def decode_request(self, request, ctx: RequestContext | None = None):
        return request.get("input", "")

    def predict(self, x, ctx: RequestContext | None = None):
        if self.log_verbose:
            import logging
            logging.getLogger("env_demo").info(
                "predict backend=%s greeting=%s version=%s input=%s",
                self.backend, self.greeting, self.version_label, x,
            )
        return {
            "output": f"{self.greeting}, {x}",
            "backend": self.backend,
            "version": self.version_label,
        }

    def encode_response(self, output, ctx: RequestContext | None = None):
        return output
