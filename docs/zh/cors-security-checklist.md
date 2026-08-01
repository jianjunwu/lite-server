# CORS 安全检查清单（P-CORS）

自研 `cors_middleware`（`src/http/cors.rs`，蓝图 §4.3 P-CORS，评审 2.2）强制执行的八条安全属性。每条都对应实现中的一条具体规则和一条钉住它的测试。

CORS **不是** `tower-http::cors`：按模型覆盖策略需要在请求时从路径解析出模型，静态挂载的 `CorsLayer` 做不到。中间件解析生效策略（per-model → 全局）并应用以下规则。

## 1. 精确 Origin 匹配

`Origin` 与配置的 `allow_origins` 在归一化后（scheme/host 转小写、去掉默认端口）**精确匹配**，无模糊匹配。→ `resolve_acao` / `normalize_origin`。

## 2. 不回显

`Access-Control-Allow-Origin` 永远不会被设置为请求原始 `Origin` 的回显。只允许设为 (a) 匹配到的已配置 origin，或 (b) 字面 `*`。未配置的 origin **不会**获得 ACAO。→ `apply_acao`。

## 3. 拒绝 `null`

`Origin: null` 头（沙箱 iframe、`file://`、data URI）视同无 origin——不附加任何 CORS 头。→ `normalize_origin` 返回 `None`。

## 4. 无后缀混淆

`https://evil-example.com` 不匹配 `https://example.com`，`https://a.notexample.com` 不匹配 `https://*.example.com`。子域通配符（`*.example.com`）要求有前置标签（`a.example.com`），且绝不匹配主域（`example.com`）。→ `WildcardOrigin::matches`。

## 5. Credentials 与 `*` 互斥

`allow_credentials: true` 时，通配符 `*` origin **不会**被反射——不发出 ACAO（浏览器禁止 `Access-Control-Allow-Origin: *` 与 `Access-Control-Allow-Credentials: true` 同时出现）。请配置显式 origins。→ `apply_acao`。

## 6. 恒带 `Vary: Origin`

每条 CORS 相关响应都携带 `Vary: Origin`（预检额外携带 `Vary: Access-Control-Request-Method` / `-Headers`），保证共享缓存不会把一个 Origin 的响应服务于另一个 Origin。→ `cors_middleware` / `preflight_response`。

## 7. 预检验证 method + headers

预检（`OPTIONS` + `Access-Control-Request-Method`）**仅当** Origin 被允许时才附加 CORS 头；允许的 methods/headers 从策略中公布（浏览器据此强制请求的 method/headers）。不合格的预检返回 204 且不带 CORS 头。→ `preflight_response`。

## 8. `max_age` ≤ 7200

`max_age_secs` 默认为 7200——Chrome 对预检缓存的上限。超过该值的配置浏览器也会截断；请配置 ≤ 7200。→ `CorsPolicy` 默认值。

## 分层

`cors_middleware` 挂在 `access_control` **之外**（D21）：预检 `OPTIONS` 在认证之前就以 204 短路（预检不带凭据）。它在 `observability` 之内，因此 204 携带 `x-request-id`。

## WebSocket

浏览器对 WebSocket 握手不发预检、也不执行 ACAO，所以 CORS 中间件无法阻止跨站 WebSocket 劫持（CSWSH）。WS 升级处理器用同一套引擎独立检查 `Origin`（`ws_origin_allowed`）。未配置 CORS 策略时，WS 安全完全依赖 `access_control`（P7-1）的 key 认证。

## Admin 端点

Admin 类端点不面向浏览器；`cors_middleware` 跳过它们（不附加 ACAO）。仅在确实需要跨源 admin 访问时才配置全局 `server.cors` 策略。
