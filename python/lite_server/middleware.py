"""DEPRECATED since 0.7.0 — import lite_server.middleware is no longer supported.

The middleware API has been unified with the Callback system.  Use the
built-in Callback classes instead:

    Old                                    New
    ─────────────────────────────────────────────────────────────
    from lite_server.middleware import     from lite_server import
      require_api_key                        RequireApiKey
      rate_limit                             RateLimit
      log_requests                           LogRequests
      cors                                   Cors

    @route.get("/p",                     @route.get("/p",
      middleware=[...])                       callbacks=[...])

    async def handler(request, server):     def handler(ctx):
        ...                                    ...

Migration guide: https://lite-server.dev/migration-0.7
Full discussion: .claude/design-callback-middleware-unification.md

TokenBucket is now an internal implementation detail (lite_server.callbacks.rate_limit._TokenBucket).
"""

raise ImportError(__doc__)
