"""DEPRECATED since 0.7.0 — import lite_server.middleware is no longer supported.

Route middleware became the unified Callback system in 0.7.0; the built-in
policy callbacks (require_api_key / rate_limit / log_requests / cors) were
then retired in 0.7.6 in favor of declarative per-model policies in
config.yaml — enforced by the Rust server:

    policies:
      auth: { header: "X-API-Key", keys: ["${API_KEYS}"] }
      rate_limit: { requests_per_minute: 60, key: ip, burst: 100 }
      cors: { allow_origins: ["*"], allow_methods: ["POST"], allow_headers: ["content-type"] }
      request_log: {}

Custom route logic: use lite_server.Callback subclasses instead of middleware.

Migration guide: https://lite-server.dev/migration-0.7
"""

raise ImportError(__doc__)
