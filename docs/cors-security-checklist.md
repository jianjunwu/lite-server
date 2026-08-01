# CORS Security Checklist (P-CORS)

The eight security properties enforced by the self-written `cors_middleware`
(`src/http/cors.rs`, 蓝图 §4.3 P-CORS, 评审 2.2). Each maps to a concrete rule in
the implementation and a test that pins it.

CORS is **not** `tower-http::cors`: per-model policy override requires resolving
the model from the path at request time, which a statically-mounted `CorsLayer`
cannot do. The middleware resolves the effective policy (per-model → global) and
applies the rules below.

## 1. Exact Origin match

`Origin` is matched exactly against the configured `allow_origins` after
normalization (lowercase scheme/host, default port stripped). No fuzzy matching.
→ `resolve_acao` / `normalize_origin`.

## 2. No reflection

`Access-Control-Allow-Origin` is never set to the request's raw `Origin` as an
echo. It is set only to (a) a configured origin that the request matched, or
(b) the literal `*`. An unconfigured origin gets **no** ACAO. → `apply_acao`.

## 3. Reject `null`

An `Origin: null` header (sandboxed iframes, `file://`, data URIs) is treated as
no origin — no CORS headers are attached. → `normalize_origin` returns `None`.

## 4. No suffix confusion

`https://evil-example.com` does not match `https://example.com`, and
`https://a.notexample.com` does not match `https://*.example.com`. Subdomain
wildcards (`*.example.com`) require a leading label (`a.example.com`) and never
match the apex (`example.com`). → `WildcardOrigin::matches`.

## 5. Credentials + `*` rejected

When `allow_credentials: true`, a wildcard `*` origin is **not** reflected — no
ACAO is emitted (browsers forbid `Access-Control-Allow-Origin: *` together with
`Access-Control-Allow-Credentials: true`). Configure explicit origins. →
`apply_acao`.

## 6. `Vary: Origin` always

Every CORS-relevant response carries `Vary: Origin` (preflight additionally
carries `Vary: Access-Control-Request-Method` / `-Headers`) so a shared cache
does not serve a response obtained for one Origin to a different Origin. →
`cors_middleware` / `preflight_response`.

## 7. Preflight validates method + headers

A preflight (`OPTIONS` + `Access-Control-Request-Method`) attaches CORS headers
**only** when the Origin is allowed; the allowed methods/headers are advertised
from the policy (the browser enforces the request method/headers against them).
A non-qualifying preflight returns 204 with no CORS headers. →
`preflight_response`.

## 8. `max_age` ≤ 7200

`max_age_secs` defaults to 7200 — Chrome's cap on the preflight cache. Values
above it are clamped by the browser anyway; configure ≤ 7200. → `CorsPolicy`
default.

## Layering

`cors_middleware` is mounted **outside** `access_control` (D21): a preflight
`OPTIONS` short-circuits with 204 before authentication runs (preflight carries
no credentials). It is inside `observability` so the 204 carries `x-request-id`.

## WebSocket

Browsers send no preflight and do not enforce ACAO on a WebSocket handshake, so
the CORS middleware cannot stop cross-site WebSocket hijacking (CSWSH). The WS
upgrade handler independently checks `Origin` against the same engine
(`ws_origin_allowed`). When no CORS policy is configured, WS security relies
entirely on `access_control` (P7-1) key authentication.

## Admin endpoints

Admin-class endpoints are not browser-facing; `cors_middleware` skips them (no
ACAO attached). Configure a global `server.cors` policy only if you need
cross-origin admin access.
