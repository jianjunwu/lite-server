use crate::error::AppError;
use crate::http::state::AppState;
use bytes::Bytes;
use indexmap::IndexMap;
use serde_json::Value;
use std::sync::Arc;
use tracing::debug;

use super::*;

/// MIMO (D8/D9): the request root after [`parse_root_inputs`].
/// `Single` is the legacy path (byte-identical); `Named` is the declared
/// multi-input path — `absent` lists optional inputs the envelope did not
/// carry (R4 conditional steps skip on them).
#[derive(Debug)]
pub enum RootInputs {
    Single(EnsembleValue),
    Named {
        values: IndexMap<String, EnsembleValue>,
        absent: Vec<String>,
    },
}

/// MIMO (D31/D32, R18/R19): the DAG entry's root parsing — the SINGLE R18
/// validation point (D39: endpoints de-frame transport only, never validate).
///  - decl = None (legacy): payload passes through untouched, except the
///    reserved `$inputs` namespace (400, D14) and the envelope container
///    form (400 — TritonBinary/LSBE-1 have no legacy semantics);
///  - decl = Some: the payload must be a KServe envelope — Json head with
///    `inputs[]` (plus an optional binary tail from [`EnsembleValue::Envelope`]);
///    elements match by name in header order, binary elements slice the tail
///    cumulatively (`parameters.binary_data_size`), `$binary_b64` marker data
///    decodes in-place (secondary path), defaults fill, absent optionals
///    list.
pub fn parse_root_inputs(
    payload: EnsembleValue,
    decl: Option<&IndexMap<String, InputDecl>>,
    // §5.5.8 (R15): true only for the dags form — a shared client may send
    // the SUPERSET of every set's inputs, so names outside the SELECTED set
    // are ignored with a debug log (never 400). Single-set forms keep the
    // strict R18 unknown-name rejection.
    tolerate_unknown: bool,
) -> Result<RootInputs, AppError> {
    let Some(decl) = decl else {
        match &payload {
            // R19/D14: `$inputs` is a reserved namespace on legacy payloads.
            EnsembleValue::Json(v) => {
                if v.as_object().map(|o| o.contains_key("$inputs")).unwrap_or(false) {
                    return Err(AppError::InvalidRequestBody(
                        "'$inputs' is a reserved namespace (D14) — this ensemble has no \
                         inputs declaration; send a plain JSON body"
                            .to_string(),
                    ));
                }
            }
            // The envelope container (TritonBinary / LSBE-1) has no legacy
            // semantics — undeclared ensembles reject it (historical
            // TritonBinary 400 kept byte-identical).
            EnsembleValue::Envelope { .. } => {
                return Err(AppError::InvalidRequestBody(
                    "Triton Binary Tensor Data Extension requests (JSON head + binary \
                     tail container) are only supported by ensembles with an inputs \
                     declaration"
                        .to_string(),
                ));
            }
            _ => {}
        }
        return Ok(RootInputs::Single(payload));
    };

    let (head, tail) = match payload {
        EnsembleValue::Json(v) => (v, Bytes::new()),
        EnsembleValue::Envelope { head, tail } => (head, tail),
        EnsembleValue::Binary(..) | EnsembleValue::RawJson(..) => {
            return Err(AppError::InvalidRequestBody(
                "this ensemble declares named inputs — requests must be a KServe \
                 envelope (JSON with inputs[], or JSON head + binary tail); raw \
                 binary bodies have no envelope semantics (R18)"
                    .to_string(),
            ));
        }
    };
    let inputs = head.get("inputs").and_then(|i| i.as_array()).ok_or_else(|| {
        AppError::InvalidRequestBody(
            "declared-inputs ensemble requires a KServe envelope with an inputs[] \
             array (R18/D31)"
                .to_string(),
        )
    })?;

    let mut values: IndexMap<String, EnsembleValue> = IndexMap::new();
    let mut offset = 0usize;
    for el in inputs {
        let name = el.get("name").and_then(|n| n.as_str()).ok_or_else(|| {
            AppError::InvalidRequestBody("envelope input element is missing 'name'".to_string())
        })?;
        let d = match decl.get(name) {
            Some(d) => d,
            None => {
                if tolerate_unknown {
                    // A tolerated binary element's declared tail slice must
                    // still be consumed — header-order slicing means a skip
                    // that leaves bytes unaccounted misaligns every later
                    // binary element.
                    if let Some(size) = el
                        .get("parameters")
                        .and_then(|p| p.get("binary_data_size"))
                        .and_then(|s| s.as_u64())
                    {
                        let end = offset.checked_add(size as usize).ok_or_else(|| {
                            AppError::InvalidRequestBody("binary tail size overflow".to_string())
                        })?;
                        let _ = tail.get(offset..end).ok_or_else(|| {
                            AppError::InvalidRequestBody(format!(
                                "envelope input '{name}': binary_data_size {size} overruns \
                                 the binary tail (R18)"
                            ))
                        })?;
                        offset = end;
                    }
                    debug!(
                        input = %name,
                        "envelope input ignored — not declared by the selected dag set (§5.5.8)"
                    );
                    continue;
                }
                return Err(AppError::InvalidRequestBody(format!(
                    "envelope declares unknown input '{name}' (not in ensemble.inputs, R18)"
                )));
            }
        };
        let value = match d.ty {
            InputType::Json => {
                let data = el.get("data").ok_or_else(|| {
                    AppError::InvalidRequestBody(format!(
                        "envelope input '{name}' (type json) is missing 'data'"
                    ))
                })?;
                EnsembleValue::Json(data.clone())
            }
            InputType::Binary => {
                // Primary path: header element + tail slice (header order).
                if let Some(marker) = el.get("data") {
                    // Secondary path: `$binary_b64` marker object in-JSON.
                    let (bytes, ct) = decode_binary_marker(marker)?;
                    EnsembleValue::Binary(bytes, ct, None, None)
                } else {
                    let size = el
                        .get("parameters")
                        .and_then(|p| p.get("binary_data_size"))
                        .and_then(|s| s.as_u64())
                        .ok_or_else(|| {
                            AppError::InvalidRequestBody(format!(
                                "envelope input '{name}' (type binary) needs \
                                 parameters.binary_data_size or $binary_b64 data (R18)"
                            ))
                        })? as usize;
                    let end = offset.checked_add(size).ok_or_else(|| {
                        AppError::InvalidRequestBody("binary tail size overflow".to_string())
                    })?;
                    let slice = tail.get(offset..end).ok_or_else(|| {
                        AppError::InvalidRequestBody(format!(
                            "envelope input '{name}': binary_data_size {size} overruns \
                             the binary tail (R18)"
                        ))
                    })?;
                    offset = end;
                    EnsembleValue::Binary(
                        Bytes::copy_from_slice(slice),
                        d.content_type.clone().unwrap_or_else(|| "application/octet-stream".to_string()),
                        d.shape.clone(),
                        d.datatype.clone(),
                    )
                }
            }
        };
        values.insert(name.to_string(), value);
    }
    if offset != tail.len() {
        return Err(AppError::InvalidRequestBody(format!(
            "binary tail has {} byte(s) beyond the declared sizes (R18)",
            tail.len() - offset
        )));
    }

    let mut absent = Vec::new();
    for (name, d) in decl {
        if values.contains_key(name) {
            continue;
        }
        match (d.required, &d.default) {
            (true, _) => {
                return Err(AppError::InvalidRequestBody(format!(
                    "envelope is missing required input '{name}' (R18)"
                )));
            }
            (false, Some(def)) => {
                values.insert(name.clone(), EnsembleValue::Json(def.clone()));
            }
            (false, None) => absent.push(name.clone()),
        }
    }
    Ok(RootInputs::Named { values, absent })
}

/// D31 (secondary in-JSON path): decode a `{"$binary_b64": "...",
/// "content_type": "..."}` marker object into bytes (content_type optional,
/// defaults to application/octet-stream).
pub(crate) fn decode_binary_marker(v: &Value) -> Result<(Bytes, String), AppError> {
    let obj = v.as_object().ok_or_else(|| {
        AppError::InvalidRequestBody(
            "binary input data must be a {\"$binary_b64\": ...} marker object".to_string(),
        )
    })?;
    let b64 = obj.get("$binary_b64").and_then(|b| b.as_str()).ok_or_else(|| {
        AppError::InvalidRequestBody(
            "binary marker object is missing the \"$binary_b64\" field".to_string(),
        )
    })?;
    let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
        .map_err(|e| {
            AppError::InvalidRequestBody(format!("invalid base64 in $binary_b64 marker: {e}"))
        })?;
    let ct = obj
        .get("content_type")
        .and_then(|c| c.as_str())
        .unwrap_or("application/octet-stream")
        .to_string();
    Ok((Bytes::from(bytes), ct))
}

/// D32: LSBE-1 — the in-frame self-describing container for transports
/// without a metadata channel (gRPC data slot, WS/h2 frames):
/// `"LSB1"` (4B magic) ‖ u64 LE head length ‖ JSON head (UTF-8) ‖ binary
/// tail. Bare JSON never containerizes (its first byte `{` is naturally
/// distinguishable — no heuristics). Returns the parsed head and the tail;
/// any malformation is a 400 (R18 row).
pub fn split_envelope(blob: &[u8]) -> Result<(serde_json::Value, Option<Bytes>), AppError> {
    let malformed = || {
        AppError::InvalidRequestBody(
            "malformed LSBE-1 envelope container (expected 'LSB1' magic + u64 LE head \
             length + JSON head + binary tail, D32)"
                .to_string(),
        )
    };
    if !blob.starts_with(b"LSB1") {
        return Err(malformed());
    }
    if blob.len() < 12 {
        return Err(malformed());
    }
    let head_len = u64::from_le_bytes(blob[4..12].try_into().unwrap()) as usize;
    let head_end = 12usize.checked_add(head_len).ok_or_else(malformed)?;
    if head_end > blob.len() {
        return Err(AppError::InvalidRequestBody(
            "LSBE-1 envelope head length overruns the frame (D32)".to_string(),
        ));
    }
    let head: serde_json::Value = serde_json::from_slice(&blob[12..head_end]).map_err(|_| {
        AppError::InvalidRequestBody(
            "LSBE-1 envelope head is not valid JSON (D32)".to_string(),
        )
    })?;
    let tail = if head_end == blob.len() {
        None
    } else {
        Some(Bytes::copy_from_slice(&blob[head_end..]))
    };
    Ok((head, tail))
}

/// D32: de-frame an opaque byte payload (gRPC data slot / batch element)
/// into the uniform internal form — transport de-framing only, zero
/// validation (D39). Bare JSON stays Json (first byte `{`); the LSBE-1
/// container splits into Envelope; anything else is the legacy Binary
/// passthrough (parse_root_inputs rejects it with 400 when the ensemble
/// declares inputs, R18).
pub fn ensemble_payload_from_bytes(
    data: &Bytes,
    content_type: Option<String>,
) -> Result<EnsembleValue, AppError> {
    // D32: the LSBE-1 magic can never be valid JSON (JSON starts with `{`,
    // `[`, `"`, a digit, `-`, `t`, `f` or `n`) — check it BEFORE any JSON
    // parsing so a container is never misread as a payload.
    if data.starts_with(b"LSB1") {
        let (head, tail) = split_envelope(data)?;
        return Ok(EnsembleValue::Envelope { head, tail: tail.unwrap_or_default() });
    }
    // §5.5.7 legacy byte-compat: ANY valid JSON — objects, arrays, scalars,
    // whitespace-prefixed — parses as Json (the historical gRPC unary
    // behaviour); malformed falls back to Binary passthrough.
    if let Ok(v) = serde_json::from_slice::<Value>(data) {
        return Ok(EnsembleValue::Json(v));
    }
    Ok(EnsembleValue::Binary(
        data.clone(),
        content_type.unwrap_or_else(|| "application/octet-stream".to_string()),
        None,
        None,
    ))
}

/// D33 (bidi): whether this ensemble declares named inputs — a declared
/// ensemble's envelope is self-describing, so the bidi upstream triggers on
/// the FIRST frame (no end signal); undeclared ensembles keep the legacy
/// multi-frame aggregation (D17).
pub async fn ensemble_declares_inputs(
    state: &Arc<AppState>,
    model_name: &str,
    version: &str,
    // E8-1: the declaration follows the SELECTED set (per-set inputs are
    // independent, R15).
    dag_selector: Option<&str>,
) -> Result<bool, AppError> {
    let plan = get_ensemble_plan(state, model_name, version).await?;
    let plan = select_dag_set(&plan, dag_selector)?;
    Ok(plan.inputs_decl.is_some())
}

/// D33/D32: de-frame a bidi FIRST frame for a DECLARED ensemble — the JSON
/// form (WS text frame / json content-type) carries the bare JSON envelope;
/// the binary form carries the LSBE-1 container. Transport-agnostic: the
/// three bidi endpoints pass their frame kind as `is_json` + bytes.
pub fn bidi_envelope_frame(
    frame: &Bytes,
    is_json: bool,
    ct: Option<String>,
) -> Result<EnsembleValue, AppError> {
    if is_json {
        let v: serde_json::Value = serde_json::from_slice(frame).map_err(|e| {
            AppError::InvalidRequestBody(format!("envelope frame is not valid JSON: {e}"))
        })?;
        return Ok(EnsembleValue::Json(v));
    }
    ensemble_payload_from_bytes(frame, ct)
}

