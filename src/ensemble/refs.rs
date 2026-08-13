use crate::error::AppError;
use serde_json::Value;
use std::collections::HashMap;

use super::*;

/// The result of resolving a `$ref` against the runtime context.
#[derive(Debug)]
pub enum ResolvedRef {
    /// The referenced value (Json or Binary).
    Value(EnsembleValue),
    /// MIMO R4: the referenced named input was absent this request — only
    /// reachable for optional inputs (conditional steps are skipped before
    /// resolution; a step resolving one here is an internal inconsistency).
    Absent(String),
}

/// P1 (batch 6): the context key a `$ref` resolves against — mirrors
/// [`resolve_ref`]'s key derivation exactly (parse-time use; ref shapes are
/// already validated, so unreachable shapes still derive the same key the
/// runtime would compute before erroring).
pub(crate) fn context_key_for_ref(steps: &[EnsembleStep], ref_str: &str) -> Result<String, AppError> {
    let caps = REF_RE.captures(ref_str).ok_or_else(|| {
        AppError::Config(format!("invalid reference: {}", ref_str))
    })?;
    let source = caps.get(1).unwrap().as_str();
    let rest = caps.get(2).map(|m| m.as_str());
    Ok(match source {
        "inputs" => {
            let name = rest.and_then(|r| r.split('.').next()).unwrap_or("");
            format!("inputs.{name}")
        }
        step_name => {
            match steps
                .iter()
                .find(|s| s.name == step_name)
                .and_then(|s| s.outputs_decl.as_ref())
            {
                Some(_decl) => {
                    let alias = rest.and_then(|r| r.split('.').next()).unwrap_or("");
                    format!("{step_name}.{alias}")
                }
                None => step_name.to_string(),
            }
        }
    })
}

/// P1 (batch 6): clone only the context keys a step references — the
/// spawn-time whole-table clone (O(payload × steps) deep copies) is gone.
/// A referenced key always exists in a running step (absent optional inputs
/// skip the step before spawn), so missing keys are simply not selected.
pub(crate) fn select_ctx_keys(
    context: &HashMap<String, EnsembleValue>,
    keys: &[String],
) -> HashMap<String, EnsembleValue> {
    keys.iter()
        .filter_map(|k| context.get(k).map(|v| (k.clone(), v.clone())))
        .collect()
}

/// Resolve a `$ref` against the runtime context. The PLAN's static type
/// environment decides the semantics (parse rules R3/R5/R7/R8 already
/// rejected ill-typed refs — the runtime only projects/forwards):
/// - `$request[.field]` — legacy anonymous root (parse-rejected in
///   declared mode, R5);
/// - `$inputs.NAME[.a.b]` — named root input; absent optional → Absent;
/// - `$stepX[.field]` — undeclared step (static json, legacy single-field);
/// - `$stepX.ALIAS` — declared alias; a binary alias passes through whole
///   (D8), a json alias projects (MIMO②).
///
/// Runtime type mismatches (declared json but the worker produced binary)
/// are step errors (I3's runtime failure #1).
pub(crate) fn resolve_ref(
    plan: &EnsemblePlan,
    ref_str: &str,
    context: &HashMap<String, EnsembleValue>,
) -> Result<ResolvedRef, AppError> {
    let caps = REF_RE.captures(ref_str).ok_or_else(|| {
        AppError::Config(format!("invalid reference: {}", ref_str))
    })?;
    let source = caps.get(1).unwrap().as_str();
    let rest = caps.get(2).map(|m| m.as_str());

    let (key, json_path) = match source {
        "inputs" => {
            let name = rest.and_then(|r| r.split('.').next()).unwrap_or("");
            let key = format!("inputs.{name}");
            let json_path = rest.and_then(|r| r.strip_prefix(name))
                .and_then(|p| p.strip_prefix('.'));
            (key, json_path)
        }
        step_name => {
            let src = plan.steps.iter().find(|s| s.name == step_name);
            match src.and_then(|s| s.outputs_decl.as_ref()) {
                Some(_decl) => {
                    // Declared step: context key is `step.alias`.
                    let alias = rest.and_then(|r| r.split('.').next()).unwrap_or("");
                    let key = format!("{step_name}.{alias}");
                    let json_path = rest.and_then(|r| r.strip_prefix(alias))
                        .and_then(|p| p.strip_prefix('.'));
                    (key, json_path)
                }
                None => {
                    // Undeclared step / request: legacy semantics — the key
                    // is the source name and the field is a SINGLE path
                    // segment. Multi-segment paths stay
                    // MIMO-declaration-only (§5.5.3) — reject them instead
                    // of silently taking the first segment (wrong data).
                    let key = source.to_string();
                    let rest = rest.unwrap_or("");
                    if rest.contains('.') {
                        return Err(AppError::Config(format!(
                            "multi-segment path '{}' is not supported on legacy \
                             steps — declare step.outputs for path projections (R7)",
                            ref_str
                        )));
                    }
                    let json_path = (!rest.is_empty()).then_some(rest);
                    (key, json_path)
                }
            }
        }
    };

    let Some(source_data) = context.get(&key) else {
        if source == "inputs" {
            // MIMO R4: an absent optional named input — Absent (conditional
            // steps are pre-skipped; reaching here is an internal bug).
            return Ok(ResolvedRef::Absent(key));
        }
        return Err(AppError::Config(format!(
            "reference source not found: {}",
            ref_str
        )));
    };

    match source_data {
        EnsembleValue::Binary(..) => match json_path {
            None => {
                // D8: binary values flow along DECLARED binary edges — the
                // legacy root (`$request`), named binary inputs
                // (`$inputs.NAME`), and declared binary aliases
                // (`$stepX.ALIAS`). Anything else is the legacy Option-A
                // boundary (undeclared step outputs — their type is unknown
                // at parse) or a declared-json mismatch (I3 runtime #1).
                let declared_binary_edge = match source {
                    "request" | "inputs" => true,
                    step_name => plan
                        .steps
                        .iter()
                        .find(|s| s.name == step_name)
                        .and_then(|s| s.outputs_decl.as_ref())
                        .and_then(|d| {
                            rest.and_then(|r| r.split('.').next()).and_then(|a| d.get(a))
                        })
                        .map(|dd| dd.ty == InputType::Binary)
                        .unwrap_or(false),
                };
                if declared_binary_edge {
                    Ok(ResolvedRef::Value(source_data.clone()))
                } else {
                    Err(AppError::InvalidRequestBody(format!(
                        "cannot reference binary step output '{}' as a whole; \
                         binary values flow only along declared binary inputs/outputs (D8) \
                         — root input → first layer or final layer → client",
                        ref_str
                    )))
                }
            }
            Some(_) => Err(AppError::InvalidRequestBody(format!(
                "cannot extract field '{}' from binary data; \
                 binary values have no field-level semantics",
                ref_str
            ))),
        },
        EnsembleValue::Json(v) => match json_path {
            None => Ok(ResolvedRef::Value(EnsembleValue::Json(v.clone()))),
            Some(f) => {
                let field_val = project_json_path(v, f).map_err(|_| {
                    AppError::Config(format!("cannot resolve '{}' from {}", ref_str, v))
                })?;
                Ok(ResolvedRef::Value(EnsembleValue::Json(field_val)))
            }
        },
        // P2/P8 (batch 6): a raw-resident value — whole refs pass the bytes
        // through by refcount; a field access parses ONCE (cached) and
        // projects from the shared value.
        EnsembleValue::RawJson(raw) => match json_path {
            None => Ok(ResolvedRef::Value(source_data.clone())),
            Some(f) => {
                let v = raw.parse().map_err(|e| {
                    AppError::Internal(format!(
                        "step output for '{}' is not valid JSON: {}",
                        ref_str, e
                    ))
                })?;
                let field_val = project_json_path(v, f).map_err(|_| {
                    AppError::Config(format!("cannot resolve '{}' from {}", ref_str, v))
                })?;
                Ok(ResolvedRef::Value(EnsembleValue::Json(field_val)))
            }
        },
        EnsembleValue::Envelope { .. } => {
            Err(AppError::Internal("envelope reached a DAG edge".to_string()))
        }
    }
}

/// MIMO: dot-segment JSON projection (`a.b.c`, an optional leading `$.` is
/// stripped). No array subscripts or filters (D29: the `$.a.b` subset only).
pub(crate) fn project_json_path(v: &Value, path: &str) -> Result<Value, AppError> {
    let path = path.strip_prefix("$.").unwrap_or(path);
    let mut cur = v;
    for seg in path.split('.') {
        if seg.is_empty() {
            return Err(AppError::Config(format!(
                "empty segment in JSON path '{path}'"
            )));
        }
        cur = cur.get(seg).ok_or_else(|| {
            AppError::Config(format!("path segment '{seg}' not found in '{path}'"))
        })?;
    }
    Ok(cur.clone())
}

