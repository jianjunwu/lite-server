//! KServe V2 dataplane 错误体 renderer:扁平 `{"error": "<message>"}`
//! (本地 kserve-master errors.py 实证)。P2.0 纯迁移仅 render 双 arm 之一,
//! 尚无检测产出(detect 在 P2.1);is_envelope 信封判定随阶段 2 codec 落地。

use super::CanonicalError;
use axum::Json;
use serde_json::{json, Value};

/// KServe 扁平错误体;extra(如 max_size/actual_size)按规范丢弃——KServe
/// 错误体只有 error 字符串。
pub(crate) fn render_body(err: &CanonicalError) -> Json<Value> {
    Json(json!({ "error": err.message }))
}
