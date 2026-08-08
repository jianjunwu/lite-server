//! Legacy/OpenAI 风格错误体 renderer:现状 wire 形状整体迁移,byte-identical
//! (P2.0 字节快照门禁)。openai-compact(阶段 6)复用本 renderer。

use super::CanonicalError;
use axum::Json;
use serde_json::{json, Map, Value};

/// 现状 wire 形状 `{"error": {type, message, code, param[, extra…]}}`,
/// 逐字节不变(键排序由 serde_json 保证,与迁移前一致)。extra 仅
/// PayloadTooLarge 携带(max_size/actual_size),合入 error 对象。
pub(crate) fn render_body(err: &CanonicalError) -> Json<Value> {
    let mut error_obj = Map::new();
    error_obj.insert("type".into(), Value::String(err.error_type.clone()));
    error_obj.insert("message".into(), Value::String(err.message.clone()));
    error_obj.insert(
        "code".into(),
        err.code.clone().map(Value::String).unwrap_or(Value::Null),
    );
    error_obj.insert(
        "param".into(),
        err.param.clone().map(Value::String).unwrap_or(Value::Null),
    );
    if let Some(Value::Object(extra)) = &err.extra {
        for (k, v) in extra {
            error_obj.insert(k.clone(), v.clone());
        }
    }
    Json(json!({ "error": error_obj }))
}
