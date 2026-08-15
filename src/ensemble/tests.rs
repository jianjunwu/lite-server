use crate::error::{AppError, ModelErrorData};
use crate::proto::liteserver as pb;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use indexmap::IndexMap;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::Duration;

use super::*;

#[test]
fn test_validate_dag_ok() {
    let steps = vec![
        EnsembleStep {
            name: "step1".to_string(),
            model: "m1".to_string(),
            version: Some("1".to_string()),

            params: HashMap::new(),

            timeout_secs: None,
            stream: false,
            on_error: OnErrorKind::Fail,
            retries: 0,
            outputs_decl: None,
            when: None,
            inputs: [("input".to_string(), "$request".to_string())].into(),
        },
        EnsembleStep {
            name: "step2".to_string(),
            model: "m2".to_string(),
            version: Some("1".to_string()),

            params: HashMap::new(),

            timeout_secs: None,
            stream: false,
            on_error: OnErrorKind::Fail,
            retries: 0,
            outputs_decl: None,
            when: None,
            inputs: [("data".to_string(), "$step1".to_string())].into(),
        },
    ];
    assert!(validate_dag(&steps).is_ok());
}

#[test]
fn test_validate_dag_cycle() {
    let steps = vec![
        EnsembleStep {
            name: "step1".to_string(),
            model: "m1".to_string(),
            version: Some("1".to_string()),

            params: HashMap::new(),

            timeout_secs: None,
            stream: false,
            on_error: OnErrorKind::Fail,
            retries: 0,
            outputs_decl: None,
            when: None,
            inputs: [("input".to_string(), "$step2".to_string())].into(),
        },
        EnsembleStep {
            name: "step2".to_string(),
            model: "m2".to_string(),
            version: Some("1".to_string()),

            params: HashMap::new(),

            timeout_secs: None,
            stream: false,
            on_error: OnErrorKind::Fail,
            retries: 0,
            outputs_decl: None,
            when: None,
            inputs: [("input".to_string(), "$step1".to_string())].into(),
        },
    ];
    assert!(validate_dag(&steps).is_err());
}

#[test]
fn test_validate_dag_unknown_ref() {
    let steps = vec![EnsembleStep {
        name: "step1".to_string(),
        model: "m1".to_string(),
        version: Some("1".to_string()),

        params: HashMap::new(),

        timeout_secs: None,
        stream: false,
        on_error: OnErrorKind::Fail,
        retries: 0,
        outputs_decl: None,
        when: None,
        inputs: [("input".to_string(), "$unknown".to_string())].into(),
    }];
    assert!(validate_dag(&steps).is_err());
}

#[test]
fn test_topological_layers() {
    let steps = vec![
        EnsembleStep {
            name: "a".to_string(),
            model: "m1".to_string(),
            version: Some("1".to_string()),

            params: HashMap::new(),

            timeout_secs: None,
            stream: false,
            on_error: OnErrorKind::Fail,
            retries: 0,
            outputs_decl: None,
            when: None,
            inputs: [("x".to_string(), "$request".to_string())].into(),
        },
        EnsembleStep {
            name: "b".to_string(),
            model: "m2".to_string(),
            version: Some("1".to_string()),

            params: HashMap::new(),

            timeout_secs: None,
            stream: false,
            on_error: OnErrorKind::Fail,
            retries: 0,
            outputs_decl: None,
            when: None,
            inputs: [("x".to_string(), "$request".to_string())].into(),
        },
        EnsembleStep {
            name: "c".to_string(),
            model: "m3".to_string(),
            version: Some("1".to_string()),

            params: HashMap::new(),

            timeout_secs: None,
            stream: false,
            on_error: OnErrorKind::Fail,
            retries: 0,
            outputs_decl: None,
            when: None,
            inputs: [("x".to_string(), "$a".to_string())].into(),
        },
    ];
    let layers = topological_layers(&steps);
    assert_eq!(layers.len(), 2);
    assert_eq!(layers[0].len(), 2); // a and b (no deps)
    assert_eq!(layers[1].len(), 1); // c (depends on a)
}

/// Legacy (undeclared) plan for resolve_ref tests.
fn legacy_plan() -> EnsemblePlan {
    parse_ensemble_plan(
            "ensemble:\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {x: \"$request\"}\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap()
}

#[test]
fn test_resolve_ref() {
    let plan = legacy_plan();
    let mut context = HashMap::new();
    context.insert(
        "request".to_string(),
        EnsembleValue::Json(json!({"image": "cat.jpg"})),
    );
    context.insert(
        "step1".to_string(),
        EnsembleValue::Json(json!({"output": 42})),
    );

    assert_eq!(
        match resolve_ref(&plan, "$request", &context).unwrap() {
            ResolvedRef::Value(EnsembleValue::Json(v)) => v,
            _ => panic!("expected Json"),
        },
        json!({"image": "cat.jpg"})
    );
    assert_eq!(
        match resolve_ref(&plan, "$request.image", &context).unwrap() {
            ResolvedRef::Value(EnsembleValue::Json(v)) => v,
            _ => panic!("expected Json"),
        },
        json!("cat.jpg")
    );
    assert_eq!(
        match resolve_ref(&plan, "$step1.output", &context).unwrap() {
            ResolvedRef::Value(EnsembleValue::Json(v)) => v,
            _ => panic!("expected Json"),
        },
        json!(42)
    );
}

// === B3: resolve_ref Binary rules (E7) ===

#[test]
fn b3_resolve_ref_request_whole_binary_passthrough() {
    let plan = legacy_plan();
    let mut context = HashMap::new();
    context.insert(
        "request".to_string(),
        EnsembleValue::Binary(
            Bytes::from_static(b"hello"),
            "text/plain".to_string(),
            None,
            None,
        ),
    );
    match resolve_ref(&plan, "$request", &context).unwrap() {
        ResolvedRef::Value(EnsembleValue::Binary(data, ct, ..)) => {
            assert_eq!(data.as_ref(), b"hello");
            assert_eq!(ct, "text/plain");
        }
        _ => panic!("expected Binary passthrough"),
    }
}

#[test]
fn b3_resolve_ref_request_field_on_binary_is_400() {
    let plan = legacy_plan();
    let mut context = HashMap::new();
    context.insert(
        "request".to_string(),
        EnsembleValue::Binary(
            Bytes::from_static(b"hello"),
            "text/plain".to_string(),
            None,
            None,
        ),
    );
    let err = resolve_ref(&plan, "$request.field", &context).unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "field access on binary must be 400, got {err:?}"
    );
    assert!(
        err.to_string().contains("field"),
        "error must mention field extraction, got: {err}"
    );
}

#[test]
fn b3_resolve_ref_step_binary_is_400() {
    let plan = legacy_plan();
    let mut context = HashMap::new();
    context.insert(
        "step1".to_string(),
        EnsembleValue::Binary(
            Bytes::from_static(b"hello"),
            "text/plain".to_string(),
            None,
            None,
        ),
    );
    // Whole step reference on binary → 400 (Option A boundary).
    let err = resolve_ref(&plan, "$step1", &context).unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "step binary reference must be 400, got {err:?}"
    );
    // Field access on step binary → same 400.
    let err = resolve_ref(&plan, "$step1.field", &context).unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "step binary field access must be 400, got {err:?}"
    );
}

// === P1 (batch 6): per-step dependency-key cloning ===

#[test]
fn p1_step_dep_keys_legacy_and_mimo() {
    let plan = parse_ensemble_plan(
            "ensemble:\n  inputs:\n    text:\n      type: json\n    image:\n      type: binary\n  steps:\n    - name: tok\n      model: pre\n      version: \"1\"\n      inputs:\n        text: \"$inputs.text\"\n    - name: enc\n      model: vis_enc\n      version: \"1\"\n      outputs:\n        thumb:\n          type: binary\n          path: \"$.thumb\"\n        emb:\n          type: json\n          path: \"$.emb\"\n      inputs:\n        img: \"$inputs.image\"\n    - name: out\n      model: echo\n      version: \"1\"\n      inputs:\n        data: \"$tok\"\n        emb: \"$enc.emb\"\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
    assert_eq!(plan.step_dep_keys[0], vec!["inputs.text".to_string()]);
    assert_eq!(plan.step_dep_keys[1], vec!["inputs.image".to_string()]);
    // Step inputs are a HashMap — ref order is arbitrary; the key SET
    // is the contract.
    let mut keys = plan.step_dep_keys[2].clone();
    keys.sort();
    assert_eq!(keys, vec!["enc.emb".to_string(), "tok".to_string()]);
}

#[test]
fn p1_step_dep_keys_legacy_root_dedups() {
    let plan = parse_ensemble_plan(
            "ensemble:\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs:\n        x: \"$request\"\n        y: \"$request.input\"\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
    // Both refs resolve against the same root key — one clone, not two.
    assert_eq!(plan.step_dep_keys[0], vec!["request".to_string()]);
}

#[test]
fn p1_step_dep_keys_dag_sets_computed_per_set() {
    let plan = parse_ensemble_plan(
            "ensemble:\n  dags:\n    default:\n      steps:\n        - name: main\n          model: pre\n          version: \"1\"\n          inputs:\n            text: \"$request.text\"\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
    // The outer container runs nothing — no dep keys of its own.
    assert!(plan.step_dep_keys.is_empty());
    let sets = plan.dag_sets.as_ref().unwrap();
    assert_eq!(
        sets["default"].step_dep_keys[0],
        vec!["request".to_string()]
    );
}

#[test]
fn p1_select_ctx_keys_clones_only_referenced_keys() {
    let mut context = HashMap::new();
    context.insert("a".to_string(), EnsembleValue::Json(json!(1)));
    context.insert("b".to_string(), EnsembleValue::Json(json!(2)));
    context.insert("c".to_string(), EnsembleValue::Json(json!(3)));
    let subset = select_ctx_keys(&context, &["a".to_string(), "missing".to_string()]);
    assert_eq!(subset.len(), 1, "only referenced keys are cloned");
    assert!(subset.contains_key("a"));
    assert!(!subset.contains_key("b"));
    assert!(!subset.contains_key("c"));
}

// === P2 + P8 (batch 6): raw-resident step outputs ===

#[test]
fn p2_raw_eligibility_whole_only_chain() {
    let plan = parse_ensemble_plan(
            "ensemble:\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      inputs:\n        x: \"$request\"\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs:\n        a: \"$a\"\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
    assert!(
        plan.step_raw_eligible[0],
        "a whole-referenced undeclared step stays raw-resident"
    );
    assert!(
        plan.step_raw_eligible[1],
        "the output step is whole-consumed"
    );
}

#[test]
fn p2_raw_eligibility_field_ref_forces_parse() {
    let plan = parse_ensemble_plan(
            "ensemble:\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      inputs:\n        x: \"$request\"\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs:\n        a: \"$a.output\"\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
    assert!(
        !plan.step_raw_eligible[0],
        "a field-referenced step must parse its output"
    );
}

#[test]
fn p2_raw_eligibility_declared_and_output_field_force_parse() {
    let declared = parse_ensemble_plan(
            "ensemble:\n  inputs:\n    text:\n      type: json\n  outputs:\n    out: \"$a.out\"\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      outputs:\n        out:\n          type: json\n      inputs:\n        x: \"$inputs.text\"\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
    assert!(
        !declared.step_raw_eligible[0],
        "declared step.outputs always parse"
    );
    let out_field = parse_ensemble_plan(
            "ensemble:\n  output: \"$a.output\"\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      inputs:\n        x: \"$request\"\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
    assert!(
        !out_field.step_raw_eligible[0],
        "an explicit output field forces the parse"
    );
}

#[test]
fn p2_assemble_splices_raw_identically_to_parsed() {
    // The raw fixture is the CANONICAL serialization of the parsed value
    // (a worker re-emitting received bytes keeps them verbatim).
    let raw_bytes: Bytes = Bytes::from_static(br#"{"output":42}"#);
    let mut resolved_raw = HashMap::new();
    resolved_raw.insert(
        "a".to_string(),
        EnsembleValue::RawJson(Arc::new(RawJsonValue::new(raw_bytes))),
    );
    let mut resolved_parsed = HashMap::new();
    resolved_parsed.insert("a".to_string(), EnsembleValue::Json(json!({"output": 42})));
    let (raw_out, _) = assemble_group_json("s", &resolved_raw, &HashMap::new()).unwrap();
    let (parsed_out, _) = assemble_group_json("s", &resolved_parsed, &HashMap::new()).unwrap();
    assert_eq!(
        raw_out, parsed_out,
        "raw splice must produce byte-identical assembly to the parsed path"
    );
}

#[test]
fn p2_assemble_sorted_keys_and_params_override() {
    let mut resolved = HashMap::new();
    resolved.insert("z".to_string(), EnsembleValue::Json(json!(1)));
    resolved.insert("a".to_string(), EnsembleValue::Json(json!(2)));
    let params: HashMap<String, Value> = [("z".to_string(), json!(3))].into_iter().collect();
    let (out, _) = assemble_group_json("s", &resolved, &params).unwrap();
    assert_eq!(
        out.as_ref(),
        br#"{"a":2,"z":3}"#,
        "keys emit sorted with params winning conflicts"
    );
}

#[test]
fn p2_resolve_ref_raw_field_parses_lazily() {
    let plan = legacy_plan();
    let mut context = HashMap::new();
    context.insert(
        "step1".to_string(),
        EnsembleValue::RawJson(Arc::new(RawJsonValue::new(Bytes::from_static(
            br#"{"output": 42}"#,
        )))),
    );
    let v = match resolve_ref(&plan, "$step1.output", &context).unwrap() {
        ResolvedRef::Value(EnsembleValue::Json(v)) => v,
        other => panic!("expected projected Json, got {other:?}"),
    };
    assert_eq!(v, json!(42));
    let whole = match resolve_ref(&plan, "$step1", &context).unwrap() {
        ResolvedRef::Value(EnsembleValue::RawJson(r)) => r,
        other => panic!("expected raw passthrough, got {other:?}"),
    };
    assert_eq!(whole.bytes.as_ref(), br#"{"output": 42}"#);
}

#[test]
fn p2_resolve_ref_raw_malformed_field_errors() {
    let plan = legacy_plan();
    let mut context = HashMap::new();
    context.insert(
        "step1".to_string(),
        EnsembleValue::RawJson(Arc::new(RawJsonValue::new(Bytes::from_static(b"{oops")))),
    );
    let err = resolve_ref(&plan, "$step1.output", &context).unwrap_err();
    assert!(
        err.to_string().contains("not valid JSON"),
        "a field access on malformed raw bytes must error, got: {err}"
    );
}

#[test]
fn p2_declared_step_materializes_nested_raw_outcome() {
    // A declared parent step over a nested child whose outcome is
    // raw-resident must parse before projecting (never panic).
    let step = EnsembleStep {
        name: "parent".to_string(),
        model: "child".to_string(),
        version: Some("1".to_string()),
        inputs: HashMap::new(),
        stream: false,
        params: HashMap::new(),
        timeout_secs: None,
        on_error: OnErrorKind::Fail,
        retries: 0,
        outputs_decl: Some(
            [(
                "out".to_string(),
                StepOutputDecl {
                    ty: InputType::Json,
                    path: None,
                },
            )]
            .into_iter()
            .collect(),
        ),
        when: None,
    };
    let raw = EnsembleValue::RawJson(Arc::new(RawJsonValue::new(Bytes::from_static(
        br#"{"out": 7}"#,
    ))));
    let out = materialize_step_outputs(&step, raw).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].0, "parent.out");
    assert_eq!(out[0].1, EnsembleValue::Json(json!(7)));
}

#[test]
fn p2_unary_response_raw_eligible_requires_valid_json() {
    // C5: raw residency validates (borrowed parse) without materializing —
    // whole refs SPLICE the bytes downstream, so invalid JSON must keep the
    // historical parse error even for pass-through schemas.
    let good = pb::SingleResponse {
        data: Bytes::from_static(br#"{"ok": 1}"#),
        media_type: "application/json".to_string(),
        ..Default::default()
    };
    let value = unary_response_to_value(
        "s",
        pb::Response {
            payload: Some(pb::response::Payload::Single(good)),
            ..Default::default()
        },
        true,
    )
    .unwrap();
    assert!(
        matches!(value, EnsembleValue::RawJson(r) if r.bytes.as_ref() == br#"{"ok": 1}"#),
        "valid JSON stays unparsed raw bytes (P2①)"
    );
    let bad = pb::SingleResponse {
        data: Bytes::from_static(b"{oops"),
        media_type: "application/json".to_string(),
        ..Default::default()
    };
    let err = unary_response_to_value(
        "s",
        pb::Response {
            payload: Some(pb::response::Payload::Single(bad)),
            ..Default::default()
        },
        true,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("invalid JSON"),
        "invalid JSON must error even when raw-eligible (splice safety, C5), got: {err}"
    );
}

#[test]
fn p2_raw_empty_body_keeps_historical_empty_object() {
    // Historical behavior: an Ok JSON response with EMPTY data normalized
    // to Json(json!({})) (parse_step_output's empty-body special case).
    // P2 raw residency must not change that — splicing empty bytes into a
    // downstream payload produces malformed JSON ({"a":}).
    let single = pb::SingleResponse {
        data: Bytes::new(),
        media_type: "application/json".to_string(),
        ..Default::default()
    };
    let value = unary_response_to_value(
        "s",
        pb::Response {
            payload: Some(pb::response::Payload::Single(single)),
            ..Default::default()
        },
        true,
    )
    .unwrap();
    let mut resolved = HashMap::new();
    resolved.insert("a".to_string(), value);
    let (out, _) = assemble_group_json("s2", &resolved, &HashMap::new()).unwrap();
    assert_eq!(
        out.as_ref(),
        br#"{"a":{}}"#,
        "an empty-body output must assemble like the historical parsed path"
    );
    serde_json::from_slice::<Value>(&out)
        .expect("raw splice of an empty body produced invalid JSON");
}

#[test]
fn p2_unary_response_raw_rejects_invalid_utf8() {
    // Historical behavior: a worker's non-UTF-8 bytes under an
    // application/json media_type errored at parse_step_output (the
    // step-error channel — skip/retries semantics intact). Raw
    // residency must not keep them: lossy decoding at the envelope
    // boundary would silently substitute U+FFFD for the original bytes.
    let single = pb::SingleResponse {
        data: Bytes::from(vec![b'{', b'"', b'k', b'"', b':', b'"', 0xFFu8, b'"', b'}']),
        media_type: "application/json".to_string(),
        ..Default::default()
    };
    let err = unary_response_to_value(
        "s",
        pb::Response {
            payload: Some(pb::response::Payload::Single(single)),
            ..Default::default()
        },
        true,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("invalid JSON"),
        "non-UTF-8 raw bytes must fall back to the historical parse error, got: {err}"
    );
}

// === P3 + P11 (batch 6): zero-spawn layer executor ===

struct DropFlag(Arc<std::sync::atomic::AtomicBool>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test]
async fn p11_first_error_drops_remaining_layer_futures() {
    // A failing step must propagate immediately and the layer's
    // remaining in-flight futures are dropped with it — the historical
    // JoinSet first-err + drop-abort semantics (a failed step's
    // siblings never keep burning worker capacity, P-FLOW §4.0.9).
    use futures::future::BoxFuture;
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let d = dropped.clone();
    let futs: FuturesUnordered<BoxFuture<'static, StepFutOutput>> = FuturesUnordered::new();
    futs.push(Box::pin(async move {
        // The slow sibling completes only if it is never dropped.
        let _guard = DropFlag(d);
        tokio::time::sleep(Duration::from_secs(3600)).await;
        ("slow".to_string(), Ok(Vec::new()))
    }));
    futs.push(Box::pin(async {
        (
            "fast".to_string(),
            Err(AppError::Internal("boom".to_string())),
        )
    }));
    let mut context = HashMap::new();
    let skip_set = HashSet::new();
    let err = drive_step_futs(futs, &mut context, &skip_set)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("boom"),
        "the first error must propagate, got {err:?}"
    );
    assert!(
        dropped.load(std::sync::atomic::Ordering::SeqCst),
        "remaining layer futures must be dropped on first error"
    );
    assert!(context.is_empty());
}

#[tokio::test]
async fn p11_layer_success_inserts_outputs_into_context() {
    use futures::future::BoxFuture;
    let futs: FuturesUnordered<BoxFuture<'static, StepFutOutput>> = FuturesUnordered::new();
    futs.push(Box::pin(async {
        (
            "a".to_string(),
            Ok(vec![("a".to_string(), EnsembleValue::Json(json!(1)))]),
        )
    }));
    let mut context = HashMap::new();
    let skip_set = HashSet::new();
    drive_step_futs(futs, &mut context, &skip_set)
        .await
        .unwrap();
    assert!(
        matches!(context.get("a"), Some(EnsembleValue::Json(v)) if *v == json!(1)),
        "completed step outputs land in the context"
    );
}

#[tokio::test]
async fn p11_skip_step_failure_continues_the_layer() {
    use futures::future::BoxFuture;
    let futs: FuturesUnordered<BoxFuture<'static, StepFutOutput>> = FuturesUnordered::new();
    futs.push(Box::pin(async {
        (
            "may".to_string(),
            Err(AppError::Internal("boom".to_string())),
        )
    }));
    let mut context = HashMap::new();
    let skip_set: HashSet<&str> = ["may"].into_iter().collect();
    drive_step_futs(futs, &mut context, &skip_set)
        .await
        .expect("a skip step's failure must not fail the layer");
    assert!(
        !context.contains_key("may"),
        "a skipped step stays absent from the context"
    );
}

// ===== P-FLOW (§4.0.9): ensemble shared cancel =====

#[tokio::test]
async fn p_flow_ensemble_joinset_aborts_inflight_on_drop() {
    // execute_ensemble uses a per-layer JoinSet: dropping it (parent
    // disconnect, total-budget timeout, or a sibling step error) must
    // ABORT in-flight sub-step tasks so workers are not left computing
    // for a cancelled ensemble. This guards the invariant the executor
    // relies on.
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    let ran = Arc::new(AtomicBool::new(false));
    let ran_clone = ran.clone();
    let mut set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    set.spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        ran_clone.store(true, Ordering::SeqCst);
    });
    // Simulate the ensemble future being dropped mid-layer.
    drop(set);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !ran.load(Ordering::SeqCst),
        "dropped JoinSet must abort its in-flight task (ensemble cancel)"
    );
}

// === B3: input assembly three branches (E7) ===

fn bin(data: &'static [u8], ct: &str) -> EnsembleValue {
    EnsembleValue::Binary(Bytes::from_static(data), ct.to_string(), None, None)
}

#[test]
fn b3_assemble_all_json_builds_object() {
    let mut resolved = HashMap::new();
    resolved.insert("a".to_string(), EnsembleValue::Json(json!(1)));
    resolved.insert("b".to_string(), EnsembleValue::Json(json!("x")));
    let (bytes, ct) = assemble_step_payload("s", &resolved, &HashMap::new(), None).unwrap();
    assert!(
        ct.is_none(),
        "all-Json assembly must not set a content-type"
    );
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"a": 1, "b": "x"}));
}

#[test]
fn b3_assemble_single_binary_passthrough_with_ct() {
    let mut resolved = HashMap::new();
    resolved.insert("img".to_string(), bin(b"\x00\x01\x02", "image/png"));
    let (bytes, ct) = assemble_step_payload("s", &resolved, &HashMap::new(), None).unwrap();
    assert_eq!(
        bytes.as_ref(),
        b"\x00\x01\x02",
        "binary payload must pass verbatim"
    );
    assert_eq!(ct.as_deref(), Some("image/png"), "CT must be forwarded");
}

#[test]
fn b3_assemble_mixed_binary_json_is_400() {
    let mut resolved = HashMap::new();
    resolved.insert("a".to_string(), bin(b"x", "application/octet-stream"));
    resolved.insert("b".to_string(), EnsembleValue::Json(json!(1)));
    let err = assemble_step_payload("s", &resolved, &HashMap::new(), None).unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "mixed JSON/Binary inputs must be 400, got {err:?}"
    );
}

#[test]
fn b3_assemble_two_binary_inputs_is_400() {
    // Even two whole-Binary inputs violate the "sole whole input" rule.
    let mut resolved = HashMap::new();
    resolved.insert("a".to_string(), bin(b"x", "application/octet-stream"));
    resolved.insert("b".to_string(), bin(b"y", "application/octet-stream"));
    let err = assemble_step_payload("s", &resolved, &HashMap::new(), None).unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "two binary inputs must be 400, got {err:?}"
    );
}

// === B3: step output typed parse (E8) ===

fn single(data: &'static [u8], media_type: &str) -> pb::SingleResponse {
    pb::SingleResponse {
        data: Bytes::from_static(data),
        media_type: media_type.to_string(),
        ..Default::default()
    }
}

#[test]
fn b3_output_parse_json_media_type() {
    let out = parse_step_output("s", single(br#"{"a":1}"#, "application/json")).unwrap();
    match out {
        EnsembleValue::Json(v) => assert_eq!(v, json!({"a": 1})),
        _ => panic!("expected Json"),
    }
}

#[test]
fn b3_output_parse_empty_media_type_defaults_json() {
    let out = parse_step_output("s", single(br#"{"a":1}"#, "")).unwrap();
    match out {
        EnsembleValue::Json(v) => assert_eq!(v, json!({"a": 1})),
        _ => panic!("expected Json"),
    }
}

#[test]
fn b3_output_parse_invalid_json_not_swallowed() {
    // Regression pin for the old `:483` behaviour: invalid JSON from a
    // worker must surface as an error naming the step, never collapse
    // into a silent `{}` that the DAG keeps running on.
    let err = parse_step_output("mystep", single(b"{oops", "")).unwrap_err();
    assert!(
        matches!(err, AppError::Internal(_)),
        "invalid JSON must be an Internal error, got {err:?}"
    );
    assert!(
        err.to_string().contains("mystep"),
        "error must name the failing step, got: {err}"
    );
}

#[test]
fn b3_output_parse_binary_media_type() {
    let out = parse_step_output("s", single(b"\x00\xff", "application/octet-stream")).unwrap();
    match out {
        EnsembleValue::Binary(d, ct, ..) => {
            assert_eq!(d.as_ref(), b"\x00\xff");
            assert_eq!(ct, "application/octet-stream");
        }
        _ => panic!("expected Binary"),
    }
}

#[test]
fn b3_output_parse_empty_data_is_empty_object() {
    let out = parse_step_output("s", single(b"", "")).unwrap();
    match out {
        EnsembleValue::Json(v) => assert_eq!(v, json!({})),
        _ => panic!("expected Json empty object"),
    }
}

// === §4.0/D16: pipeline-form validation (batch 2) ===

fn pstep(name: &str, inputs: &[(&str, &str)], stream: bool) -> EnsembleStep {
    sstep(name, inputs, stream)
}

#[test]
fn pipeline_chain_two_streaming_steps_valid() {
    // s0 (streaming) → s1 (streaming, consumes s0 whole) = a valid chain;
    // the chain tail is the config last step (output semantics, §4.1-4).
    let steps = vec![
        pstep("s0", &[("input", "$request")], true),
        pstep("s1", &[("data", "$s0")], true),
    ];
    let chains = build_chains(&steps, 1).expect("valid chain must build");
    assert_eq!(chains.len(), 1, "one chain expected");
    assert_eq!(chains[0].nodes, vec![0, 1], "chain order: s0 → s1");
}

#[test]
fn pipeline_r1_two_consumers_rejected() {
    // P-R1: a streaming step's output must have EXACTLY one consumer.
    let steps = vec![
        pstep("s0", &[("input", "$request")], true),
        pstep("s1", &[("data", "$s0")], true),
        pstep("s2", &[("data", "$s0")], true),
    ];
    let err = build_chains(&steps, 2).unwrap_err();
    assert!(
        err.to_string().contains("consumer"),
        "P-R1 must name the consumer rule, got: {err}"
    );
}

#[test]
fn pipeline_r2_whole_ref_only_rejected() {
    // P-R2: streaming outputs can only be referenced whole ($s0), never
    // field-projected ($s0.field).
    let steps = vec![
        pstep("s0", &[("input", "$request")], true),
        pstep("s1", &[("data", "$s0.token")], true),
    ];
    let err = build_chains(&steps, 1).unwrap_err();
    assert!(
        err.to_string().contains("whole"),
        "P-R2 must reject field references, got: {err}"
    );
}

#[test]
fn pipeline_d26_unary_consumer_rejected() {
    // D26: a chain's unary consumer has no clean chunk→unary→chunk
    // semantics — the form is rejected at parse time ("pipeline chain
    // tail must be a streaming step" covers the unary-tail shape).
    let steps = vec![
        pstep("s0", &[("input", "$request")], true),
        pstep("u1", &[("data", "$s0")], false),
    ];
    let err = build_chains(&steps, 1).unwrap_err();
    assert!(
        err.to_string().contains("streaming") || err.to_string().contains("tail"),
        "D26 must reject the unary-consumer chain form, got: {err}"
    );
}

#[test]
fn pipeline_r5_mixed_forms_rejected() {
    // P-R5: the DAG's streaming set is exactly ONE chain OR one tail
    // streaming step — a chain plus an orphan streaming step is an error.
    let steps = vec![
        pstep("s0", &[("input", "$request")], true),
        pstep("s1", &[("data", "$s0")], true),
        pstep("s2", &[("input", "$request")], true),
    ];
    // Chain s0→s1 (tail = s1 = config last); s2 is an orphan streaming
    // step that is NOT the output step.
    let err = build_chains(&steps, 1).unwrap_err();
    assert!(
        err.to_string().contains("streaming step"),
        "P-R5 must reject chain + orphan streaming step, got: {err}"
    );
}

#[test]
fn pipeline_chain_tail_must_be_output_step() {
    // P-R3: the chain tail must be the DAG output step. Chain s0→s1 but
    // the config last step is s2 (unary) → the chain tail is not the
    // output → rejected (§4.1-4 output semantics).
    let steps = vec![
        pstep("s0", &[("input", "$request")], true),
        pstep("s1", &[("data", "$s0")], true),
        pstep("s2", &[("data", "$request")], false),
    ];
    let err = build_chains(&steps, 2).unwrap_err();
    assert!(
        err.to_string().contains("output step"),
        "chain tail must be the output step, got: {err}"
    );
}

#[test]
fn pipeline_orphan_streaming_step_not_output_rejected() {
    // A zero-consumer streaming step that is NOT the output step is an
    // orphan (§4.1-4: the config last step must be the streaming step).
    let steps = vec![
        pstep("s0", &[("input", "$request")], true),
        pstep("u1", &[("data", "$request")], false),
    ];
    let err = build_chains(&steps, 1).unwrap_err();
    assert!(
        err.to_string().contains("streaming"),
        "orphan streaming step must be rejected, got: {err}"
    );
}

// === §4.0/D16: streaming validation (form dispatch, batch 0) ===

fn sstep(name: &str, inputs: &[(&str, &str)], stream: bool) -> EnsembleStep {
    EnsembleStep {
        name: name.to_string(),
        model: format!("m_{}", name),
        version: Some("1".to_string()),

        params: HashMap::new(),

        timeout_secs: None,
        inputs: inputs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        stream,
        on_error: OnErrorKind::Fail,
        retries: 0,
        outputs_decl: None,
        when: None,
    }
}

#[test]
fn stream_rules_tail_stream_dag_valid() {
    // s1 (unary) → s2 (streaming tail): the only open form in batch 0.
    let steps = vec![
        sstep("s1", &[("input", "$request")], false),
        sstep("s2", &[("data", "$s1")], true),
    ];
    assert!(
        validate_stream_rules(&steps, 1).is_ok(),
        "tail-streaming DAG must validate"
    );
}

#[test]
fn stream_rules_two_stream_steps_rejected() {
    // Rule 3: at most one streaming step per DAG.
    let steps = vec![
        sstep("s1", &[("input", "$request")], true),
        sstep("s2", &[("data", "$request")], true),
    ];
    let err = validate_stream_rules(&steps, 1).unwrap_err();
    assert!(
        err.to_string().contains("one streaming step"),
        "must reject two streaming steps, got: {err}"
    );
}

#[test]
fn stream_rules_tail_stream_not_config_last_rejected() {
    // Rule 4 (B-m4): with `output` omitted the DAG output is steps.last(),
    // which must be the streaming step — a streaming step that is not the
    // config last step would silently produce nothing streamable.
    let steps = vec![
        sstep("s1", &[("input", "$request")], true),
        sstep("s2", &[("data", "$request")], false),
    ];
    let err = validate_stream_rules(&steps, 1).unwrap_err();
    assert!(
        err.to_string().contains("output") && err.to_string().contains("s1"),
        "must reject streaming step not at config tail, got: {err}"
    );
}

#[test]
fn stream_rules_plain_dag_unchanged() {
    // No stream: false — behaviour parity with the historical validator.
    let steps = vec![
        sstep("s1", &[("input", "$request")], false),
        sstep("s2", &[("data", "$s1")], false),
    ];
    assert!(validate_stream_rules(&steps, 1).is_ok());
}

// === P10: streaming-DAG capacity (D40) ===

#[test]
fn p10_capacity_rejects_when_exhausted_and_releases_on_drop() {
    let cap = StreamingCapacityState::new(2);
    let p1 = cap.try_acquire().unwrap();
    let p2 = cap.try_acquire().unwrap();
    let err = cap.try_acquire().err().unwrap();
    assert!(
        matches!(err, AppError::StreamingCapacityExceeded(_)),
        "exhausted capacity must reject immediately (429), got {err:?}"
    );
    // Dropping a permit returns its slot — no queueing, no leak.
    drop(p1);
    let _p3 = cap
        .try_acquire()
        .expect("slot must be released on permit drop");
    // Clones share the same permit: slot stays held until ALL clones drop.
    let p2_clone = p2.clone();
    drop(p2);
    let err = cap.try_acquire().err().unwrap();
    assert!(
        matches!(err, AppError::StreamingCapacityExceeded(_)),
        "cloned permit must hold the slot until the last reference drops"
    );
    drop(p2_clone);
    let _p4 = cap
        .try_acquire()
        .expect("last clone drop must release the slot");
}

#[test]
fn p10_capacity_zero_permits_is_immediately_exhausted() {
    // A 0-limit installation rejects everything (misconfiguration guard);
    // production never installs one for 0 (server/mod.rs skips it).
    let cap = StreamingCapacityState::new(0);
    let err = cap.try_acquire().err().unwrap();
    assert!(matches!(err, AppError::StreamingCapacityExceeded(_)));
}

// === P0: EnsemblePlan cache (D6 + review ①-④) ===

fn test_plan(path: &str) -> Arc<EnsemblePlan> {
    Arc::new(EnsemblePlan {
        steps: Vec::new(),
        layers: Vec::new(),
        output_step: 0,
        output_field: None,
        chains: Vec::new(),
        inputs_decl: None,
        input_modes: Vec::new(),
        conditional_refs: Vec::new(),
        step_dep_keys: Vec::new(),
        step_raw_eligible: Vec::new(),
        outputs: None,
        dag_sets: None,
        config_path: PathBuf::from(path),
        source_mtime: None,
    })
}

#[tokio::test]
async fn p0_cache_hit_returns_same_arc() {
    let cache = EnsemblePlanCache::new();
    let key = PlanKey {
        model: "m".to_string(),
        version: "1".to_string(),
    };
    let plan = test_plan("/nonexistent");
    let first = cache
        .get_or_load(key.clone(), || {
            let plan = plan.clone();
            async move { Ok::<_, AppError>(plan) }
        })
        .await
        .unwrap();
    assert!(
        Arc::ptr_eq(&plan, &first),
        "first load returns the parsed plan"
    );
    let hit = cache
        .get_or_load(key, || async { panic!("cache hit must not parse again") })
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&plan, &hit), "hit must return the cached Arc");
}

#[tokio::test]
async fn p0_cache_single_flight_only_one_parse() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let cache = Arc::new(EnsemblePlanCache::new());
    let key = PlanKey {
        model: "m".to_string(),
        version: "1".to_string(),
    };
    let parse_count = Arc::new(AtomicUsize::new(0));
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..10 {
        let cache = cache.clone();
        let key = key.clone();
        let parse_count = parse_count.clone();
        set.spawn(async move {
            cache
                .get_or_load(key, || {
                    let parse_count = parse_count.clone();
                    async move {
                        parse_count.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok::<_, AppError>(test_plan("/nonexistent"))
                    }
                })
                .await
                .unwrap()
        });
    }
    let mut plans = Vec::new();
    while let Some(r) = set.join_next().await {
        plans.push(r.unwrap());
    }
    assert_eq!(
        parse_count.load(Ordering::SeqCst),
        1,
        "concurrent first requests must parse exactly once (single-flight, review ④)"
    );
    for p in &plans {
        assert!(
            Arc::ptr_eq(&plans[0], p),
            "all waiters must receive the holder's plan"
        );
    }
}

#[tokio::test]
async fn p0_cache_failed_load_not_cached() {
    let cache = EnsemblePlanCache::new();
    let key = PlanKey {
        model: "m".to_string(),
        version: "1".to_string(),
    };
    let err = cache
        .get_or_load(key.clone(), || async {
            Err::<Arc<EnsemblePlan>, _>(AppError::Config("boom".to_string()))
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("boom"));
    // A failed load must not be cached: the next call re-parses (a fixed
    // config heals without reload — behaviour parity with no cache).
    let ok = cache
        .get_or_load(key, || async {
            Ok::<_, AppError>(test_plan("/nonexistent"))
        })
        .await;
    assert!(
        ok.is_ok(),
        "failed load must not be cached; next call re-parses"
    );
}

#[tokio::test]
async fn c2_loader_panic_does_not_wedge_the_slot() {
    // C2 (resource-leak-plan): a panicking loader used to leave the Loading
    // placeholder in place — the key wedged and every later caller parked
    // forever. The panic must funnel into the Err path (evict + notify).
    let cache = EnsemblePlanCache::new();
    let key = PlanKey {
        model: "m".to_string(),
        version: "1".to_string(),
    };
    let err = cache
        .get_or_load(key.clone(), || async {
            panic!("loader exploded");
            #[allow(unreachable_code)]
            Ok::<_, AppError>(test_plan("/nonexistent"))
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("loader exploded"),
        "panic must surface as an Internal error, got: {err}"
    );
    // The slot is not wedged: the next call re-parses and succeeds.
    let ok = cache
        .get_or_load(key, || async { Ok::<_, AppError>(test_plan("/nonexistent")) })
        .await;
    assert!(ok.is_ok(), "a panicked load must not wedge the key");
}

#[tokio::test]
async fn p0_cache_invalidate_model_clears_all_versions() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let cache = EnsemblePlanCache::new();
    let count = Arc::new(AtomicUsize::new(0));
    let k_latest = PlanKey {
        model: "m".to_string(),
        version: "latest".to_string(),
    };
    let k_pinned = PlanKey {
        model: "m".to_string(),
        version: "1".to_string(),
    };
    let k_other = PlanKey {
        model: "other".to_string(),
        version: "1".to_string(),
    };
    for key in [&k_latest, &k_pinned, &k_other] {
        cache
            .get_or_load(key.clone(), || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, AppError>(test_plan("/nonexistent"))
                }
            })
            .await
            .unwrap();
    }
    cache.invalidate_model("m");
    // Review ③: both "latest" and the pinned version are separate keys
    // and must ALL clear on a model-prefix invalidation.
    for key in [&k_latest, &k_pinned] {
        cache
            .get_or_load(key.clone(), || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, AppError>(test_plan("/nonexistent"))
                }
            })
            .await
            .unwrap();
    }
    assert_eq!(
        count.load(Ordering::SeqCst),
        5,
        "invalidate_model must clear every version of the model"
    );
}

#[tokio::test]
async fn p0_cache_invalidate_version_clears_one() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let cache = EnsemblePlanCache::new();
    let count = Arc::new(AtomicUsize::new(0));
    let k1 = PlanKey {
        model: "m".to_string(),
        version: "1".to_string(),
    };
    let k2 = PlanKey {
        model: "m".to_string(),
        version: "2".to_string(),
    };
    for key in [&k1, &k2] {
        cache
            .get_or_load(key.clone(), || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, AppError>(test_plan("/nonexistent"))
                }
            })
            .await
            .unwrap();
    }
    cache.invalidate_version("m", "1");
    cache
        .get_or_load(k1, || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, AppError>(test_plan("/nonexistent"))
            }
        })
        .await
        .unwrap();
    // v2 untouched → no new parse.
    cache
        .get_or_load(k2, || async { panic!("v2 must stay cached") })
        .await
        .unwrap();
    assert_eq!(
        count.load(Ordering::SeqCst),
        3,
        "only version 1 must re-parse"
    );
}

#[tokio::test(start_paused = true)]
async fn p0_cache_mtime_recheck_after_interval() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = std::env::temp_dir().join(format!("liteserver-ens-p0-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("config.yaml");
    std::fs::write(&config_path, b"v1").unwrap();
    let cache = EnsemblePlanCache::new();
    let key = PlanKey {
        model: "m".to_string(),
        version: "1".to_string(),
    };
    let count = Arc::new(AtomicUsize::new(0));
    let load = |count: Arc<AtomicUsize>| {
        let config_path = config_path.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok::<_, AppError>(Arc::new(EnsemblePlan {
                steps: Vec::new(),
                layers: Vec::new(),
                output_step: 0,
                output_field: None,
                chains: Vec::new(),
                inputs_decl: None,
                input_modes: Vec::new(),
                conditional_refs: Vec::new(),
                step_dep_keys: Vec::new(),
                step_raw_eligible: Vec::new(),
                outputs: None,
                dag_sets: None,
                config_path,
                source_mtime: None,
            }))
        }
    };
    cache
        .get_or_load(key.clone(), || load(count.clone()))
        .await
        .unwrap();
    // Within the stat interval: no syscall, same Arc (review ②).
    let within = cache
        .get_or_load(key.clone(), || load(count.clone()))
        .await
        .unwrap();
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "hot path must not stat within the interval"
    );
    // Rewrite the file; still within interval → still served from cache.
    std::fs::write(&config_path, b"v2").unwrap();
    let stale = cache
        .get_or_load(key.clone(), || load(count.clone()))
        .await
        .unwrap();
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "file change inside the interval is not seen"
    );
    drop(within);
    drop(stale);
    // Advance past the interval: next get must stat, see the mtime change,
    // evict and re-parse (single-flight).
    tokio::time::advance(Duration::from_millis(1500)).await;
    cache
        .get_or_load(key.clone(), || load(count.clone()))
        .await
        .unwrap();
    assert_eq!(
        count.load(Ordering::SeqCst),
        2,
        "mtime change after the interval must re-parse"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// === /audit 2026-08-12: batch-0/1 defect repros (read-only; no impl changes) ===

/// Concurrency assumption (D23): unload_version fires cache invalidation
/// BEFORE registry changes, so an invalidate can race an in-flight
/// single-flight load (cold cache + concurrent unload/reload). The holder
/// must still resolve — not panic on its vanished Loading cell.
#[tokio::test]
async fn p0_cache_invalidate_during_inflight_load_must_not_panic() {
    let cache = Arc::new(EnsemblePlanCache::new());
    let key = PlanKey {
        model: "m".to_string(),
        version: "1".to_string(),
    };
    let (gate_tx, gate_rx) = oneshot::channel::<()>();
    let c2 = cache.clone();
    let k2 = key.clone();
    let holder = tokio::spawn(async move {
        c2.get_or_load(k2, || async move {
            // Hold the single-flight load open (a slow disk read/parse).
            let _ = gate_rx.await;
            Ok::<_, AppError>(test_plan("/nonexistent"))
        })
        .await
    });
    // Wait until the holder has published the Loading cell.
    for _ in 0..1000 {
        if matches!(
            cache.plans.get(&key).as_deref(),
            Some(PlanCell::Loading { .. })
        ) {
            break;
        }
        tokio::task::yield_now().await;
    }
    // D23: unload invalidates before registry changes — this races loads.
    cache.invalidate_model("m");
    let _ = gate_tx.send(());
    let joined = holder.await;
    assert!(
        joined.is_ok(),
        "invalidate racing an in-flight load panicked the holder: {joined:?}"
    );
    assert!(
        joined.unwrap().is_ok(),
        "the racing load must still resolve its plan"
    );
}

/// Order assumption (review ②): the loader stats BEFORE the read, so a
/// write landing between stat and read leaves the stored mtime OLDER than
/// the file's — the interval re-check must then re-parse (safe). The
/// reverse order (stat after read) could pin a fresh mtime onto stale
/// content and serve it indefinitely.
#[tokio::test(start_paused = true)]
async fn p0_cache_stat_after_read_must_not_pin_stale_plan() {
    let dir = std::env::temp_dir().join(format!("liteserver-ens-p0-toctou-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("config.yaml");
    std::fs::write(&config_path, b"v1").unwrap();
    // The production loader's stat-before-read mtime.
    let v1_mtime = std::fs::metadata(&config_path)
        .and_then(|m| m.modified())
        .ok();
    // Real clock: force a distinct mtime for the interleaved write.
    std::thread::sleep(std::time::Duration::from_millis(10));
    let cache = EnsemblePlanCache::new();
    let key = PlanKey {
        model: "m".to_string(),
        version: "1".to_string(),
    };
    // First load: returns the v1 plan (output_step=1) with the
    // stat-before-read mtime, but the file becomes v2 before the cache
    // stores the entry — exactly the stat→write→store interleaving the
    // mtime re-check must catch at the next interval.
    let first = cache
        .get_or_load(key.clone(), || {
            let cp = config_path.clone();
            async move {
                std::fs::write(&cp, b"v2").unwrap(); // interleaved write
                Ok::<_, AppError>(Arc::new(EnsemblePlan {
                    steps: Vec::new(),
                    layers: Vec::new(),
                    output_step: 1,
                    output_field: None,
                    chains: Vec::new(),
                    inputs_decl: None,
                    input_modes: Vec::new(),
                    conditional_refs: Vec::new(),
                    step_dep_keys: Vec::new(),
                    step_raw_eligible: Vec::new(),
                    outputs: None,
                    dag_sets: None,
                    config_path: cp,
                    source_mtime: v1_mtime,
                }))
            }
        })
        .await
        .unwrap();
    assert_eq!(first.output_step, 1);
    tokio::time::advance(Duration::from_millis(1500)).await;
    // Interval elapsed: the re-check must notice v2 (stored mtime is
    // v1's, older than the file's) and re-parse.
    let second = cache
        .get_or_load(key.clone(), || {
            let cp = config_path.clone();
            async move {
                Ok::<_, AppError>(Arc::new(EnsemblePlan {
                    steps: Vec::new(),
                    layers: Vec::new(),
                    output_step: 2,
                    output_field: None,
                    chains: Vec::new(),
                    inputs_decl: None,
                    input_modes: Vec::new(),
                    conditional_refs: Vec::new(),
                    step_dep_keys: Vec::new(),
                    step_raw_eligible: Vec::new(),
                    outputs: None,
                    dag_sets: None,
                    config_path: cp,
                    source_mtime: None,
                }))
            }
        })
        .await
        .unwrap();
    assert_eq!(
        second.output_step, 2,
        "a mid-load write must be caught at the next interval re-check"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Control-flow assumption (§4.1 rule 1): a streaming step must be in the
/// LAST topological layer. "Output not referenced" does NOT imply that —
/// Kahn layering puts a no-dependency sink in layer 0 even when later
/// layers exist. Accepting this config truncates execution at the tail's
/// layer: steps in later layers silently never run.
#[test]
fn stream_rules_streaming_step_must_be_in_last_topological_layer() {
    // a (layer 0) ← c (layer 1); b (stream, no deps → layer 0, config-last).
    let yaml = r#"
ensemble:
  steps:
    - name: a
      model: m1
      version: "1"
      inputs: {x: "$request.x"}
    - name: c
      model: m2
      version: "1"
      inputs: {y: "$a"}
    - name: b
      model: m3
      version: "1"
      stream: true
      inputs: {z: "$request.z"}
"#;
    let res = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"));
    assert!(
        res.is_err(),
        "streaming step not in the last topological layer must be rejected (rule 1); \
             accepting it silently drops step c from execution"
    );
}

/// Data assumption: an empty steps list is malformed config. It must be a
/// Config error at parse — not a `steps.len() - 1` underflow panic
/// (regression: the pre-cache code returned a request-time 500).
#[test]
fn parse_ensemble_plan_empty_steps_is_config_error_not_panic() {
    let yaml = "ensemble:\n  steps: []\n";
    let res = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"));
    assert!(
        res.is_err(),
        "empty steps must be a config error, not an arithmetic panic"
    );
}

/// Config contract (D24): the ensemble schema must deny unknown fields so
/// 0.9.0 keys (params/when/outputs/…) — or plain typos — fail fast at
/// load instead of being silently ignored.
#[test]
fn ensemble_schema_rejects_unknown_fields_d24() {
    let yaml = r#"
ensemble:
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
      strem: true
"#;
    let res = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"));
    assert!(
        res.is_err(),
        "unknown step field `strem` must be rejected (D24 deny_unknown_fields); \
             silently ignoring it disables streaming without any error"
    );
}

// ===== Batch 3 (E2/E3/E4/E5) parse-layer tests =====

/// E2: output omitted → steps.last() (historical semantics).
#[test]
fn e2_output_defaults_to_last_step() {
    let yaml = r#"
ensemble:
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "1"
      inputs: {x: "$s1"}
"#;
    let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
    assert_eq!(plan.output_step, 1);
    assert_eq!(plan.output_field, None);
}

/// E2: explicit output selects a named step.
#[test]
fn e2_output_selects_named_step() {
    let yaml = r#"
ensemble:
  output: "$s1"
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "1"
      inputs: {x: "$s1"}
"#;
    let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
    assert_eq!(plan.output_step, 0);
    assert_eq!(plan.output_field, None);
}

/// E2: `$stepN.field` Json path.
#[test]
fn e2_output_field_path() {
    let yaml = r#"
ensemble:
  output: "$s2.score"
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "1"
      inputs: {x: "$s1"}
"#;
    let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
    assert_eq!(plan.output_step, 1);
    assert_eq!(plan.output_field.as_deref(), Some("score"));
}

/// E2: unknown step / missing `$` prefix / `$request` → load-time config
/// errors, never a silent fallback.
#[test]
fn e2_output_rejects_unknown_step_and_bad_format() {
    for output in ["$nope", "s1", "$request"] {
        let yaml = format!(
            r#"
ensemble:
  output: "{output}"
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {{x: "$request"}}
"#
        );
        let res = parse_ensemble_plan(&yaml, &PathBuf::from("/nonexistent/config.yaml"));
        assert!(res.is_err(), "output '{output}' must be a config error");
    }
}

/// E2 × D11: with a streaming step, the explicit output must BE that
/// step (validated at parse — the DAG output IS the stream).
#[test]
fn e2_streaming_output_must_point_at_streaming_step() {
    let yaml = r#"
ensemble:
  output: "$s1"
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "1"
      stream: true
      inputs: {x: "$s1"}
"#;
    let res = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"));
    assert!(
        res.is_err(),
        "output pointing away from the streaming step must be rejected (D11)"
    );
}

/// E4: version omitted / "latest" → None (execution-time resolution);
/// explicit versions are kept as-is.
#[test]
fn e4_version_optional_and_latest_normalized() {
    let yaml = r#"
ensemble:
  steps:
    - name: s1
      model: m1
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "latest"
      inputs: {x: "$s1"}
    - name: s3
      model: m3
      version: "1"
      inputs: {x: "$s2"}
"#;
    let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
    assert_eq!(plan.steps[0].version, None);
    assert_eq!(plan.steps[1].version, None, "\"latest\" == omitted (E4)");
    assert_eq!(plan.steps[2].version.as_deref(), Some("1"));
}

/// E3: params parse into the step (assembly applies them in Step 3).
#[test]
fn e3_params_parse_into_step() {
    let yaml = r#"
ensemble:
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
      params:
        temperature: 0.7
        top_p: 0.9
"#;
    let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
    let params = &plan.steps[0].params;
    assert_eq!(
        params.get("temperature").and_then(|v| v.as_f64()),
        Some(0.7)
    );
    assert_eq!(params.get("top_p").and_then(|v| v.as_f64()), Some(0.9));
}

/// E5: timeout_secs parses; non-positive / non-finite rejected at load.
#[test]
fn e5_timeout_secs_parse_and_validation() {
    let ok = r#"
ensemble:
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
      timeout_secs: 2.5
"#;
    let plan = parse_ensemble_plan(ok, &PathBuf::from("/nonexistent/config.yaml")).unwrap();
    assert_eq!(plan.steps[0].timeout_secs, Some(2.5));

    for bad in ["0", "-1", ".nan"] {
        let yaml = format!(
            r#"
ensemble:
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {{x: "$request"}}
      timeout_secs: {bad}
"#
        );
        let res = parse_ensemble_plan(&yaml, &PathBuf::from("/nonexistent/config.yaml"));
        assert!(res.is_err(), "timeout_secs={bad} must be a config error");
    }
}

// ===== Batch 3 (E4/D15) snapshot resolution tests =====

fn snapshot_step(model: &str, version: Option<&str>) -> EnsembleStep {
    EnsembleStep {
        name: "s".to_string(),
        model: model.to_string(),
        version: version.map(|v| v.to_string()),
        inputs: HashMap::new(),
        stream: false,
        params: HashMap::new(),
        timeout_secs: None,
        on_error: OnErrorKind::Fail,
        retries: 0,
        outputs_decl: None,
        when: None,
    }
}

fn registry_with_active(model: &str, version: &str) -> crate::registry::ModelRegistry {
    let registry = crate::registry::ModelRegistry::new();
    registry.force_pin_active_version(model, version);
    registry
}

/// D15: the first resolution for a model wins — a later registry drift
/// must not change it within the same request.
#[test]
fn e4_snapshot_memoizes_first_resolution() {
    let registry = registry_with_active("m", "1");
    let snapshot = VersionSnapshot::default();
    let step = snapshot_step("m", None);
    assert_eq!(snapshot.resolve(&registry, &step).unwrap(), "1");
    // Active drifts to v2 AFTER the first resolution — the snapshot still
    // serves v1 (same-request consistency, D15).
    registry.force_pin_active_version("m", "2");
    assert_eq!(snapshot.resolve(&registry, &step).unwrap(), "1");
}

/// E4: explicit versions bypass the snapshot entirely.
#[test]
fn e4_explicit_version_bypasses_snapshot() {
    let registry = registry_with_active("m", "1");
    let snapshot = VersionSnapshot::default();
    let step = snapshot_step("m", Some("2"));
    assert_eq!(snapshot.resolve(&registry, &step).unwrap(), "2");
    assert!(
        snapshot.resolved.lock().unwrap().is_empty(),
        "explicit versions must not touch the snapshot"
    );
}

/// E4: an unresolved step with no active version is an execution-time
/// resolution error (ModelNotFound).
#[test]
fn e4_unresolved_without_active_version_is_error() {
    let registry = crate::registry::ModelRegistry::new();
    let snapshot = VersionSnapshot::default();
    let step = snapshot_step("m", None);
    assert!(snapshot.resolve(&registry, &step).is_err());
}

/// D15: the memoize key is the model — two unresolved steps for the same
/// model share ONE snapshot entry.
#[test]
fn e4_snapshot_keyed_by_model() {
    let registry = registry_with_active("m", "1");
    let snapshot = VersionSnapshot::default();
    let a = snapshot_step("m", None);
    let b = snapshot_step("m", None);
    assert_eq!(snapshot.resolve(&registry, &a).unwrap(), "1");
    assert_eq!(snapshot.resolve(&registry, &b).unwrap(), "1");
    assert_eq!(
        snapshot.resolved.lock().unwrap().len(),
        1,
        "one snapshot entry per model"
    );
}

// ===== Batch 3 (E1) nesting tests =====

/// E1: the ancestor chain detects a self-reference (same model+version
/// on the active nesting path).
#[test]
fn e1_ancestor_chain_detects_self_loop() {
    let chain = vec![
        ("a".to_string(), "1".to_string()),
        ("b".to_string(), "2".to_string()),
    ];
    assert!(contains_ancestor(&chain, "a", "1"));
    assert!(contains_ancestor(&chain, "b", "2"));
    assert!(!contains_ancestor(&chain, "a", "2"));
    assert!(!contains_ancestor(&chain, "other", "1"));
}

/// E1: a nested run extends the chain with the child ensemble; the
/// PARENT chain stays untouched — each branch owns its copy, the chain
/// is not request-global mutable state.
#[test]
fn e1_ancestor_chain_extends_per_branch() {
    let parent = vec![("a".to_string(), "1".to_string())];
    let child = extend_ancestor_chain(&parent, "b", "2");
    assert!(contains_ancestor(&child, "a", "1"));
    assert!(contains_ancestor(&child, "b", "2"));
    assert_eq!(parent.len(), 1, "the parent chain must stay unchanged");
    assert!(!contains_ancestor(&parent, "b", "2"));
}

/// E1: `contains_ancestor` answers for the CURRENT branch only — the
/// chain is immutable per branch (each recursion level extends its own
/// copy via [`extend_ancestor_chain`]), so a concurrent sibling's
/// in-flight child run can never appear on this branch's chain. The old
/// flat shared Vec conflated the two, turning legal same-layer fan-out
/// (two steps calling one child ensemble; D30 batch elements) into a
/// spurious "recursion detected" 400 (B1).
#[test]
fn e1_sibling_in_flight_child_is_not_recursion() {
    let parent = vec![("p".to_string(), "1".to_string())];
    // Sibling A recurses into the child ensemble and extends ITS branch...
    let _chain_a = extend_ancestor_chain(&parent, "child", "1");
    // ...sibling B checks ITS OWN chain — the sibling's in-flight child
    // run is not an ancestor of B's branch.
    assert!(
        !contains_ancestor(&parent, "child", "1"),
        "a concurrent sibling's in-flight child call is not recursion"
    );
}

/// E1: nesting depth limit — depth counts along the call tree; level 0
/// is the top-level request, so depth 8+ is rejected.
#[test]
fn e1_nesting_depth_limit() {
    assert!(ensure_nesting_depth(0).is_ok());
    assert!(ensure_nesting_depth(7).is_ok());
    assert!(ensure_nesting_depth(8).is_err());
}

/// E5: `timeout_secs` is parse-legal whenever positive & finite, but the
/// unix_ns conversion saturates the f64→i64 cast and the `now +` addition
/// then overflows: debug builds panic (arithmetic overflow), release
/// wraps negative — every request on that step then fails instantly as
/// expired. The conversion must clamp instead of overflowing.
#[test]
fn e5_huge_timeout_secs_must_not_overflow() {
    // 1e18 s (~3×10^10 years) is absurd but parse-legal (positive, finite).
    let d = step_effective_deadline(None, Some(1e18));
    assert!(
        d.is_some() && d.unwrap() > 0,
        "a huge timeout must clamp to a sane deadline, got {d:?}"
    );
}

// ===== Batch 3 (E2/E3) output selection + params tests =====

/// E2: output_field selection — None passes the whole value through;
/// Some extracts the field from a Json output.
#[test]
fn e2_select_output_field_jsons() {
    let v = EnsembleValue::Json(json!({"score": 0.9, "label": "a"}));
    let whole = select_output_field("s1", v.clone(), None).unwrap();
    assert_eq!(
        whole,
        EnsembleValue::Json(json!({"score": 0.9, "label": "a"}))
    );
    let field = select_output_field("s1", v, Some("score")).unwrap();
    assert_eq!(field, EnsembleValue::Json(json!(0.9)));
}

/// E2: a missing field is an error (the DAG's contract does not match the
/// model's output shape).
#[test]
fn e2_select_output_field_missing_is_error() {
    let v = EnsembleValue::Json(json!({"score": 0.9}));
    let res = select_output_field("s1", v, Some("label"));
    assert!(res.is_err(), "missing field must error, not default");
}

/// E2: field projection on a Binary output is rejected (no field
/// semantics on bytes — D7's rule applied to the output face).
#[test]
fn e2_select_output_field_on_binary_is_error() {
    let v = EnsembleValue::Binary(
        bytes::Bytes::from_static(b"raw"),
        "application/octet-stream".to_string(),
        None,
        None,
    );
    let res = select_output_field("s1", v, Some("score"));
    assert!(res.is_err());
}

/// E2 × D11: a streaming DAG cannot declare an output FIELD (chunks have
/// no field semantics) — parse-time rejection.
#[test]
fn e2_streaming_output_field_rejected_at_parse() {
    let yaml = r#"
ensemble:
  output: "$s2.score"
  steps:
    - name: s1
      model: m1
      version: "1"
      inputs: {x: "$request"}
    - name: s2
      model: m2
      version: "1"
      stream: true
      inputs: {x: "$s1"}
"#;
    let res = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"));
    assert!(
        res.is_err(),
        "field-projected streaming output must be rejected"
    );
}

/// E3: params merge into the assembled Json payload AFTER inputs — params
/// win on key conflicts.
#[test]
fn e3_params_merge_into_payload_params_win() {
    let resolved: HashMap<String, EnsembleValue> = [
        ("a".to_string(), EnsembleValue::Json(json!(1))),
        ("b".to_string(), EnsembleValue::Json(json!(2))),
    ]
    .into();
    let params: HashMap<String, Value> =
        [("b".to_string(), json!(3)), ("c".to_string(), json!(4))].into();
    let (bytes, ct) = assemble_step_payload("s", &resolved, &params, None).unwrap();
    assert_eq!(ct, None);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"a": 1, "b": 3, "c": 4}), "params override inputs");
}

/// E3: Binary assembly has no params semantics — a non-empty params on a
/// Binary step is rejected at assembly (the earliest point the input type
/// is decidable).
#[test]
fn e3_params_rejected_with_binary_input() {
    let resolved: HashMap<String, EnsembleValue> = [(
        "data".to_string(),
        EnsembleValue::Binary(
            bytes::Bytes::from_static(b"raw"),
            "application/octet-stream".to_string(),
            None,
            None,
        ),
    )]
    .into();
    let params: HashMap<String, Value> = [("temperature".to_string(), json!(0.7))].into();
    let res = assemble_step_payload("s", &resolved, &params, None);
    assert!(res.is_err(), "params × Binary input must be rejected (E3)");
}

// ===== Batch 3 (E5) step timeout tests =====

fn unix_ns_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

/// E5: no step timeout → the parent deadline passes through unchanged
/// (historical behaviour).
#[test]
fn e5_step_effective_deadline_passthrough() {
    assert_eq!(
        step_effective_deadline(Some(1_000_000), None),
        Some(1_000_000)
    );
    assert_eq!(step_effective_deadline(None, None), None);
}

/// E5: a step timeout produces a wall-clock cap of now + timeout_secs.
#[test]
fn e5_step_effective_deadline_timeout_cap() {
    let before = unix_ns_now();
    let deadline = step_effective_deadline(None, Some(2.0)).unwrap();
    let after = unix_ns_now() + 2_000_000_000;
    assert!(
        deadline >= before + 2_000_000_000 && deadline <= after,
        "step cap must be ~now + 2s, got {deadline}"
    );
}

/// E5: the tighter bound wins — an earlier parent deadline caps the step.
#[test]
fn e5_step_effective_deadline_min_with_parent() {
    let parent = unix_ns_now() + 1_000_000_000; // 1s from now
    let deadline = step_effective_deadline(Some(parent), Some(60.0));
    assert_eq!(
        deadline,
        Some(parent),
        "parent deadline must win when earlier"
    );
}

// === E6 (batch 4): on_error/retries ===

/// E6 (D5): a `skip` step referenced by ANY other step's inputs is a
/// parse-time rejection — the DAG must never have a dangling reference
/// when the skip fires at runtime.
#[test]
fn e6_skip_step_referenced_by_another_step_is_config_error() {
    let yaml = r#"
ensemble:
  steps:
    - name: may_skip
      model: m1
      version: "1"
      on_error: skip
      inputs: {x: "$request"}
    - name: consumer
      model: m2
      version: "1"
      inputs: {y: "$may_skip"}
"#;
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("skip step referenced downstream must be rejected at parse");
    assert!(
        err.to_string().contains("skip"),
        "error must name the skip rule, got: {err}"
    );
}

/// E6 (D5): a `skip` step referenced (even by field) by another step is
/// rejected — the field projection has the same dangling-reference risk.
#[test]
fn e6_skip_step_field_reference_is_config_error() {
    let yaml = r#"
ensemble:
  steps:
    - name: may_skip
      model: m1
      version: "1"
      on_error: skip
      inputs: {x: "$request"}
    - name: consumer
      model: m2
      version: "1"
      inputs: {y: "$may_skip.field"}
"#;
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("skip step field reference must be rejected at parse");
    assert!(
        err.to_string().contains("skip"),
        "error must name the skip rule, got: {err}"
    );
}

/// E6 (D5): `ensemble.output` pointing at a skip step is rejected — the
/// single-output contract has no null channel (only E7 outputs do).
#[test]
fn e6_skip_step_as_output_is_config_error() {
    let yaml = r#"
ensemble:
  output: "$may_skip"
  steps:
    - name: may_skip
      model: m1
      version: "1"
      on_error: skip
      inputs: {x: "$request"}
"#;
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("skip step as ensemble.output must be rejected at parse");
    assert!(
        err.to_string().contains("skip"),
        "error must name the skip rule, got: {err}"
    );
}

/// E6 (D34 rule 6): a streaming step must never be absent — `on_error:
/// skip` × `stream: true` is a parse-time rejection (the streaming
/// response contract promises a stream unconditionally).
#[test]
fn e6_streaming_step_with_skip_is_config_error() {
    let yaml = r#"
ensemble:
  steps:
    - name: tail
      model: m1
      version: "1"
      stream: true
      on_error: skip
      inputs: {x: "$request"}
"#;
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("streaming step with on_error: skip must be rejected at parse");
    assert!(
        err.to_string().contains("skip"),
        "error must name the skip rule, got: {err}"
    );
}

/// E6: an unreferenced skip step is legal — the skip simply drops the
/// step from the context and the layer continues.
#[test]
fn e6_unreferenced_skip_step_is_accepted() {
    let yaml = r#"
ensemble:
  steps:
    - name: may_skip
      model: m1
      version: "1"
      on_error: skip
      inputs: {x: "$request"}
    - name: main
      model: m2
      version: "1"
      inputs: {x: "$request"}
"#;
    let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect("unreferenced skip step must parse");
    assert_eq!(plan.steps[0].on_error, OnErrorKind::Skip);
    assert_eq!(plan.steps[1].on_error, OnErrorKind::Fail);
}

/// E6: an unknown on_error value fails deserialization (the schema
/// denies typos — a swallowed `on_error: skp` would silently disable
/// fault tolerance).
#[test]
fn e6_unknown_on_error_value_is_config_error() {
    let yaml = r#"
ensemble:
  steps:
    - name: s
      model: m1
      version: "1"
      on_error: skp
      inputs: {x: "$request"}
"#;
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("unknown on_error value must be rejected at parse");
    assert!(err.to_string().contains("on_error"), "got: {err}");
}

/// E6: retry classification — only 5xx worker errors and timeouts
/// retry; 4xx (client contract), queue pressure and crashes never do
/// (a 4xx is deterministic, a QueueFull retry makes pressure worse).
#[test]
fn e6_retryable_error_classification() {
    let err_500 = AppError::ModelError(Box::new(ModelErrorData {
        status_code: 500,
        error_type: "model_error".into(),
        detail: "boom".into(),
        code: None,
        param: None,
        headers: None,
    }));
    let err_503 = AppError::ModelError(Box::new(ModelErrorData {
        status_code: 503,
        error_type: "model_error".into(),
        detail: "overloaded".into(),
        code: None,
        param: None,
        headers: None,
    }));
    let err_400 = AppError::ModelError(Box::new(ModelErrorData {
        status_code: 400,
        error_type: "invalid".into(),
        detail: "bad input".into(),
        code: None,
        param: None,
        headers: None,
    }));
    assert!(is_retryable_error(&err_500), "5xx must retry");
    assert!(is_retryable_error(&err_503), "5xx must retry");
    assert!(!is_retryable_error(&err_400), "4xx must NOT retry");
    assert!(
        is_retryable_error(&AppError::InferenceTimeout("t".into())),
        "timeouts must retry"
    );
    assert!(
        !is_retryable_error(&AppError::QueueFull("full".into())),
        "queue pressure must NOT retry"
    );
    assert!(
        !is_retryable_error(&AppError::WorkerCrashed("crash".into())),
        "worker crashes must NOT retry"
    );
    assert!(
        !is_retryable_error(&AppError::ModelNotReady("not ready".into())),
        "readiness must NOT retry"
    );
}

// === MIMO② (batch 4③): D10 json aliases — path projection ===

/// R6: json alias path projections — default path `$.<alias>`, explicit
/// `$.a.b` paths, and refs carrying projection paths (parse + runtime).
#[test]
fn mimo2_json_alias_projection() {
    let yaml = r#"ensemble:
  inputs:
    x:
      type: json
  steps:
    - name: a
      model: m1
      version: "1"
      outputs:
        score:
          type: json
          path: "$.out.score"
        whole:
          type: json
      inputs: {x: "$inputs.x"}
    - name: b
      model: m2
      version: "1"
      inputs: {x: "$a.score"}
"#;
    let plan = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect("json alias declarations must parse (MIMO②)");
    assert_eq!(plan.input_mode(1), Some(InputMode::GroupJson));

    // Materialize: explicit nested path + default `$.<alias>` path.
    let step = EnsembleStep {
        name: "a".to_string(),
        model: "m1".to_string(),
        version: Some("1".to_string()),
        inputs: HashMap::new(),
        when: None,
        stream: false,
        params: HashMap::new(),
        timeout_secs: None,
        on_error: OnErrorKind::Fail,
        retries: 0,
        outputs_decl: Some(
            [
                (
                    "score",
                    StepOutputDecl {
                        ty: InputType::Json,
                        path: Some("$.out.score".to_string()),
                    },
                ),
                (
                    "whole",
                    StepOutputDecl {
                        ty: InputType::Json,
                        path: None,
                    },
                ),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
        ),
    };
    let out = materialize_step_outputs(
        &step,
        EnsembleValue::Json(json!({"out": {"score": 0.9}, "whole": {"a": 1}})),
    )
    .unwrap();
    assert_eq!(out.len(), 2);
    let map: HashMap<&str, &EnsembleValue> = out.iter().map(|(k, v)| (k.as_str(), v)).collect();
    assert_eq!(map["a.score"], &EnsembleValue::Json(json!(0.9)));
    assert_eq!(
        map["a.whole"],
        &EnsembleValue::Json(json!({"a": 1})),
        "default path $.whole"
    );

    // Missing path → step error (I3: declared contract unmet).
    let err =
        materialize_step_outputs(&step, EnsembleValue::Json(json!({"other": 1}))).unwrap_err();
    assert!(
        err.to_string().contains("score"),
        "missing projection path must error naming the alias, got: {err}"
    );

    // Runtime ref with a projection path on a json alias.
    let mut context = HashMap::new();
    context.insert("a.score".to_string(), EnsembleValue::Json(json!({"x": 7})));
    match resolve_ref(&plan, "$a.score.x", &context).unwrap() {
        ResolvedRef::Value(EnsembleValue::Json(v)) => assert_eq!(v, json!(7)),
        other => panic!("expected Json 7, got {other:?}"),
    }
}

/// R6: alias names must be identifiers and paths must be `$.a.b` dot
/// segments — both are parse-time rejections.
#[test]
fn mimo2_r6_alias_name_and_path_validation() {
    let bad_name = "ensemble:\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      outputs:\n        9bad:\n          type: json\n      inputs: {x: \"$request\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$a.9bad\"}\n";
    let err = parse_ensemble_plan(bad_name, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("invalid alias name must be rejected (R6)");
    assert!(err.to_string().contains("alias"), "got: {err}");
    let bad_path = "ensemble:\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      outputs:\n        score:\n          type: json\n          path: \"out[0].score\"\n      inputs: {x: \"$request\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$a.score\"}\n";
    let err = parse_ensemble_plan(bad_path, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("array-subscript paths must be rejected (D29)");
    assert!(err.to_string().contains("path"), "got: {err}");
}

// === E8-2 (batch 5): when expressions ===

/// E8-2 parse: the grammar is `<ref> <op> <literal>` — operators
/// `== != contains in`, refs from the R16/m2 whitelist
/// ($request.dag / $request.client_ip / $inputs.NAME[.path]).
#[test]
fn e8_when_parse_and_whitelist() {
    let ok = "ensemble:\n  inputs:\n    mode:\n      type: json\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      when: \"$request.dag == 'fast'\"\n      inputs: {x: \"$inputs.mode\"}\n    - name: t\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$inputs.mode\"}\n";
    let plan = parse_ensemble_plan(ok, &PathBuf::from("/nonexistent/config.yaml"))
        .expect("whitelisted when refs must parse");
    assert!(plan.steps[0].when.is_some());
    // Non-whitelisted $request field → rejected (m2).
    let bad = "ensemble:\n  inputs:\n    mode:\n      type: json\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      when: \"$request.nope == 'x'\"\n      inputs: {x: \"$inputs.mode\"}\n    - name: t\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$inputs.mode\"}\n";
    let err = parse_ensemble_plan(bad, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("non-whitelisted request field must be rejected (R16)");
    assert!(err.to_string().contains("nope"), "got: {err}");
    // Unknown input → rejected.
    let bad = "ensemble:\n  inputs:\n    mode:\n      type: json\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      when: \"$inputs.nope == 'x'\"\n      inputs: {x: \"$inputs.mode\"}\n    - name: t\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$inputs.mode\"}\n";
    let err = parse_ensemble_plan(bad, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("undeclared input in when must be rejected (R16)");
    assert!(err.to_string().contains("nope"), "got: {err}");
    // Malformed expression → rejected.
    let bad = "ensemble:\n  inputs:\n    mode:\n      type: json\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      when: \"$request.dag === 'x'\"\n      inputs: {x: \"$inputs.mode\"}\n    - name: t\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$inputs.mode\"}\n";
    let err = parse_ensemble_plan(bad, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("malformed when must be rejected");
    let _ = err; // the rejection itself is the assertion
}

/// D34 rule 6 (third arm): `when` × `stream: true` is a parse-time
/// rejection (a streaming response promises a stream unconditionally).
#[test]
fn e8_when_x_stream_rejected() {
    let yaml = "ensemble:\n  steps:\n    - name: tail\n      model: m\n      version: \"1\"\n      stream: true\n      when: \"$request.dag == 'fast'\"\n      inputs: {x: \"$request\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("when × stream must be rejected (D34)");
    assert!(err.to_string().contains("when"), "got: {err}");
}

/// E8-2: a when-step's absence must be statically provable — downstream
/// references are rejected exactly like E6-skip (D5 channel).
#[test]
fn e8_when_step_downstream_ref_rejected() {
    let yaml = "ensemble:\n  steps:\n    - name: cond\n      model: m\n      version: \"1\"\n      when: \"$request.dag == 'fast'\"\n      inputs: {x: \"$request\"}\n    - name: consumer\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$cond\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("when-step downstream ref must be rejected");
    assert!(err.to_string().contains("absent"), "got: {err}");
}

/// E8-2 evaluation: strict type equality (no coercion), contains/in on
/// strings+arrays, the absent == null check (R16).
#[test]
fn e8_when_eval_semantics() {
    let plan = parse_ensemble_plan(
            "ensemble:\n  inputs:\n    a:\n      type: json\n    opt:\n      type: json\n      required: false\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      when: \"$inputs.a == 1\"\n      inputs: {x: \"$inputs.a\"}\n    - name: t\n      model: m2\n      version: \"1\"\n      when: \"$inputs.a == '1'\"\n      inputs: {x: \"$inputs.a\"}\n    - name: u\n      model: m3\n      version: \"1\"\n      when: \"$inputs.a contains 'x'\"\n      inputs: {x: \"$inputs.a\"}\n    - name: v\n      model: m4\n      version: \"1\"\n      when: \"$inputs.a in ['x', 'y']\"\n      inputs: {x: \"$inputs.a\"}\n    - name: w\n      model: m5\n      version: \"1\"\n      when: \"$inputs.opt != null\"\n      inputs: {x: \"$inputs.opt\"}\n    - name: z\n      model: m6\n      version: \"1\"\n      when: \"$request.dag == 'fast'\"\n      inputs: {x: \"$inputs.a\"}\n    - name: out\n      model: m7\n      version: \"1\"\n      inputs: {x: \"$inputs.a\"}\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
    let mut context = HashMap::new();
    context.insert("inputs.a".to_string(), EnsembleValue::Json(json!(1)));
    // opt absent.
    let opts = EnsembleExecOpts {
        client_ip: "127.0.0.1".into(),
        deadline_unix_ns: None,
        decoupled: false,
        dag_selector: Some("fast".into()),
    };
    assert!(
        eval_when(plan.steps[0].when.as_ref().unwrap(), &opts, &context).unwrap(),
        "1 == 1"
    );
    assert!(
        !eval_when(plan.steps[1].when.as_ref().unwrap(), &opts, &context).unwrap(),
        "1 == '1' must be false (strict, no coercion)"
    );
    assert!(
        !eval_when(plan.steps[2].when.as_ref().unwrap(), &opts, &context).unwrap(),
        "number contains → false"
    );
    assert!(
        !eval_when(plan.steps[3].when.as_ref().unwrap(), &opts, &context).unwrap(),
        "1 in ['x','y'] must be false"
    );
    // absent compares AS null (§5.5.8: `!= null` is the absence check) —
    // so `$inputs.opt != null` with opt ABSENT is false (the step is
    // skipped), and `$inputs.opt == null` is true.
    assert!(
        !eval_when(plan.steps[4].when.as_ref().unwrap(), &opts, &context).unwrap(),
        "absent != null must be false (absence compares as null, §5.5.8)"
    );
    assert!(
        eval_when(plan.steps[5].when.as_ref().unwrap(), &opts, &context).unwrap(),
        "$request.dag == 'fast'"
    );
}

// === E8-1 (batch 5): named DAG sets ===

/// E8-1: the dags form forbids top-level steps/output/outputs/inputs —
/// everything lives inside the sets (no ambiguity about the default).
#[test]
fn e8_dags_form_forbids_top_level_fields() {
    let yaml = "ensemble:\n  dags:\n    default:\n      steps:\n        - name: a\n          model: m\n          version: \"1\"\n          inputs: {x: \"$request\"}\n  steps:\n    - name: b\n      model: m\n      version: \"1\"\n      inputs: {x: \"$request\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("dags + top-level steps must be rejected");
    assert!(err.to_string().contains("dags"), "got: {err}");
}

/// R15: each set validates INDEPENDENTLY — an invalid set fails the
/// load naming the set; other sets stay untouched.
#[test]
fn e8_per_set_independent_validation() {
    let yaml = "ensemble:\n  dags:\n    default:\n      steps:\n        - name: a\n          model: m\n          version: \"1\"\n          inputs: {x: \"$request\"}\n    broken:\n      steps:\n        - name: a\n          model: m\n          version: \"1\"\n          on_error: skip\n          inputs: {x: \"$request\"}\n        - name: b\n          model: m2\n          version: \"1\"\n          inputs: {x: \"$a\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("a broken set must fail the load (R15)");
    assert!(
        err.to_string().contains("broken"),
        "must name the set: {err}"
    );
}

/// E8-1: set selection — None = "default", Some = exact name; unknown
/// name → 400 (D22: never a silent default fallback); a selector on a
/// single-form plan → 400.
#[test]
fn e8_select_dag_set() {
    let plan = parse_ensemble_plan(
            "ensemble:\n  dags:\n    default:\n      steps:\n        - name: a\n          model: m\n          version: \"1\"\n          inputs: {x: \"$request\"}\n    fast:\n      steps:\n        - name: b\n          model: m\n          version: \"1\"\n          inputs: {x: \"$request\"}\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
    let plan = Arc::new(plan);
    assert_eq!(select_dag_set(&plan, None).unwrap().steps[0].name, "a");
    assert_eq!(
        select_dag_set(&plan, Some("fast")).unwrap().steps[0].name,
        "b"
    );
    let err = select_dag_set(&plan, Some("nope")).unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "unknown dag → 400 (D22), got {err:?}"
    );
    assert!(err.to_string().contains("nope"), "got: {err}");
    // Single-form plan + selector → 400.
    let single = parse_ensemble_plan(
            "ensemble:\n  steps:\n    - name: a\n      model: m\n      version: \"1\"\n      inputs: {x: \"$request\"}\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
    let err = select_dag_set(&Arc::new(single), Some("fast")).unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "got {err:?}"
    );
}

/// D22: selector value validation — non-empty, ≤64 chars,
/// `[A-Za-z0-9_-]` only.
#[test]
fn e8_d22_selector_validation() {
    assert!(validate_dag_selector("fast").is_ok());
    assert!(validate_dag_selector("fast-v2_x").is_ok());
    assert!(validate_dag_selector("").is_err(), "empty must be rejected");
    assert!(validate_dag_selector("has space").is_err());
    assert!(validate_dag_selector("has!bang").is_err());
    let long = "a".repeat(65);
    assert!(
        validate_dag_selector(&long).is_err(),
        ">64 chars must be rejected"
    );
}

/// E8-1: per-set inputs declarations are INDEPENDENT (R15) — the
/// envelope contract follows the selected set.
#[test]
fn e8_per_set_inputs_independent() {
    let plan = parse_ensemble_plan(
            "ensemble:\n  dags:\n    default:\n      steps:\n        - name: a\n          model: m\n          version: \"1\"\n          inputs: {x: \"$request\"}\n    named:\n      inputs:\n        text:\n          type: json\n      steps:\n        - name: a\n          model: m\n          version: \"1\"\n          inputs: {x: \"$inputs.text\"}\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
    let plan = Arc::new(plan);
    let default = select_dag_set(&plan, None).unwrap();
    assert!(default.inputs_decl.is_none(), "default set is legacy-form");
    let named = select_dag_set(&plan, Some("named")).unwrap();
    assert!(named.inputs_decl.is_some(), "named set declares inputs");
}

// === E7 (batch 4④): multi-sink outputs ===

/// E7: `output` and `outputs` are mutually exclusive (二选一).
#[test]
fn e7_output_x_outputs_mutually_exclusive() {
    let yaml = "ensemble:\n  output: \"$a\"\n  outputs:\n    answer: \"$a\"\n  steps:\n    - name: a\n      model: m\n      version: \"1\"\n      inputs: {x: \"$request\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("output × outputs must be rejected (E7)");
    assert!(err.to_string().contains("outputs"), "got: {err}");
}

/// R13: outputs values must be legal refs — unknown sources are
/// rejected; refs to absentable steps are ALLOWED (null channel, D5).
#[test]
fn e7_r13_outputs_ref_validation() {
    let bad = "ensemble:\n  outputs:\n    answer: \"$nope\"\n  steps:\n    - name: a\n      model: m\n      version: \"1\"\n      inputs: {x: \"$request\"}\n";
    let err = parse_ensemble_plan(bad, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("unknown outputs ref must be rejected (R13)");
    assert!(err.to_string().contains("nope"), "got: {err}");
    // Skip-step alias is the D5 null channel.
    let ok = "ensemble:\n  outputs:\n    answer: \"$may\"\n  steps:\n    - name: may\n      model: m1\n      version: \"1\"\n      on_error: skip\n      inputs: {x: \"$request\"}\n    - name: main\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$request\"}\n";
    parse_ensemble_plan(ok, &PathBuf::from("/nonexistent/config.yaml"))
        .expect("skip-step outputs alias must parse (D5)");
    // Declared-step alias refs ($stepX.ALIAS) are legal sink refs.
    let ok = "ensemble:\n  outputs:\n    thumb: \"$a.crop\"\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      outputs:\n        crop:\n          type: binary\n      inputs: {x: \"$request\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$request\"}\n";
    parse_ensemble_plan(ok, &PathBuf::from("/nonexistent/config.yaml"))
        .expect("declared-alias sink refs must parse (R13)");
}

/// R14/D11: outputs × streaming — exactly ONE alias pointing at the
/// streaming step; anything else is rejected.
#[test]
fn e7_r14_streaming_outputs_sole_alias() {
    let base = "ensemble:\n  outputs:\n{out}  steps:\n    - name: pre\n      model: m1\n      version: \"1\"\n      inputs: {x: \"$request\"}\n    - name: tail\n      model: m2\n      version: \"1\"\n      stream: true\n      inputs: {x: \"$pre\"}\n";
    // Sole alias pointing at the streaming step → ok.
    let ok = base.replace("{out}", "    answer: \"$tail\"\n");
    parse_ensemble_plan(&ok, &PathBuf::from("/nonexistent/config.yaml"))
        .expect("sole streaming alias must parse (D11)");
    // Two aliases → rejected.
    let two = base.replace("{out}", "    a: \"$tail\"\n    b: \"$pre\"\n");
    let err = parse_ensemble_plan(&two, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("multi-alias outputs on a streaming DAG must be rejected (D11)");
    assert!(err.to_string().contains("alias"), "got: {err}");
    // Alias NOT pointing at the streaming step → rejected.
    let wrong = base.replace("{out}", "    answer: \"$pre\"\n");
    let err = parse_ensemble_plan(&wrong, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("outputs not referencing the streaming step must be rejected (R14)");
    assert!(err.to_string().contains("stream"), "got: {err}");
}

/// build_response: the KServe envelope shape — json aliases in outputs[],
/// binary aliases into the tail with binary_data_size refilled, absent
/// aliases (skip/optional) → data: null + warn (D5), declaration order
/// preserved.
#[test]
fn e7_build_response_envelope() {
    let plan = parse_ensemble_plan(
            "ensemble:\n  inputs:\n    a:\n      type: json\n    opt:\n      type: json\n      required: false\n  outputs:\n    answer: \"$main\"\n    thumb: \"$enc.crop\"\n    echo: \"$inputs.a\"\n    maybe: \"$inputs.opt\"\n  steps:\n    - name: enc\n      model: m1\n      version: \"1\"\n      outputs:\n        crop:\n          type: binary\n          path: \"$.crop\"\n      inputs: {x: \"$inputs.a\"}\n    - name: main\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$inputs.a\"}\n",
            &PathBuf::from("/nonexistent/config.yaml"),
        )
        .unwrap();
    let mut context = HashMap::new();
    context.insert("main".to_string(), EnsembleValue::Json(json!({"out": 1})));
    context.insert(
        "enc.crop".to_string(),
        EnsembleValue::Binary(
            Bytes::from_static(b"\x01\x02"),
            "image/png".into(),
            Some(vec![2]),
            None,
        ),
    );
    context.insert("inputs.a".to_string(), EnsembleValue::Json(json!("in")));
    // opt absent → maybe → null.
    let outcome = build_response(&plan, "demo", &context).unwrap();
    let EnsembleOutcome::Unary(EnsembleValue::Envelope { head, tail }) = outcome else {
        panic!("multi-sink with a binary alias must be an envelope");
    };
    assert_eq!(tail.as_ref(), b"\x01\x02");
    assert_eq!(head["model_name"], json!("demo"));
    let outs = head["outputs"].as_array().unwrap();
    assert_eq!(outs.len(), 4, "{outs:?}");
    assert_eq!(outs[0], json!({"name": "answer", "data": {"out": 1}}));
    assert_eq!(
        outs[1],
        json!({"name": "thumb", "parameters": {"binary_data_size": 2}, "shape": [2]})
    );
    assert_eq!(outs[2], json!({"name": "echo", "data": "in"}));
    assert_eq!(
        outs[3],
        json!({"name": "maybe", "data": null}),
        "absent alias → null (D5)"
    );
}

/// D32 codec: LSBE-1 encode + split round-trips (the gRPC multi-sink
/// response container).
#[test]
fn e7_lsbe1_encode_split_roundtrip() {
    let head = json!({"model_name": "demo", "outputs": [{"name": "a", "data": 1}]});
    let blob = encode_lsbe1(&head, b"\x00\x01");
    let (h, t) = split_envelope(&blob).unwrap();
    assert_eq!(h, head);
    assert_eq!(t.as_deref(), Some(&b"\x00\x01"[..]));
}

// === MIMO (batch 4①): inputs declaration R1-R5, wire R18/R19, LSBE-1
// (D32), static type env R11/R12, step.outputs binary aliases R6-R8/R10 ===

fn json_decl() -> InputDecl {
    InputDecl {
        ty: InputType::Json,
        required: true,
        default: None,
        content_type: None,
        shape: None,
        datatype: None,
    }
}

/// R1: input names must be plain identifiers (the `$inputs.NAME` grammar
/// depends on it); a `type` is mandatory.
#[test]
fn mimo_r1_invalid_input_name_and_missing_type() {
    for bad in ["9lives", "has-dash", "$weird"] {
        let yaml = format!(
                "ensemble:\n  inputs:\n    {bad}:\n      type: json\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {{x: \"$inputs.{bad}\"}}\n"
            );
        let err = parse_ensemble_plan(&yaml, &PathBuf::from("/nonexistent/config.yaml"))
            .expect_err(&format!("input name '{bad}' must be rejected (R1)"));
        assert!(err.to_string().contains("input"), "got: {err}");
    }
    let yaml = "ensemble:\n  inputs:\n    a: {}\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {x: \"$inputs.a\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("missing type must be rejected (R1)");
    assert!(err.to_string().contains("type"), "got: {err}");
}

/// R2: declaration fields are type-gated — default/json only,
/// content_type/shape/datatype binary only, required+default conflict.
#[test]
fn mimo_r2_decl_field_type_gating() {
    let cases = [
        ("default", "default: 1", "json 上允许,二进制上必须拒绝"),
        ("content_type", "content_type: image/png", "json 上必须拒绝"),
        ("shape", "shape: [1, 2]", "json 上必须拒绝"),
        ("datatype", "datatype: FP32", "json 上必须拒绝"),
    ];
    for (name, extra, why) in cases {
        let binary_yaml = format!(
                "ensemble:\n  inputs:\n    {name}:\n      type: binary\n      {extra}\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {{x: \"$inputs.{name}\"}}\n"
            );
        let json_yaml = format!(
                "ensemble:\n  inputs:\n    {name}:\n      type: json\n      {extra}\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {{x: \"$inputs.{name}\"}}\n"
            );
        match name {
            "default" => {
                parse_ensemble_plan(&binary_yaml, &PathBuf::from("/nonexistent/config.yaml"))
                    .expect_err("default on binary must be rejected (R2)");
                // required defaults true and conflicts with a default —
                // the legal default-on-json form declares required: false.
                let json_optional = json_yaml.replace(
                    "      default: 1",
                    "      required: false\n      default: 1",
                );
                parse_ensemble_plan(&json_optional, &PathBuf::from("/nonexistent/config.yaml"))
                    .expect("default on json is legal (with required: false)");
            }
            _ => {
                parse_ensemble_plan(&json_yaml, &PathBuf::from("/nonexistent/config.yaml"))
                    .expect_err(why);
                parse_ensemble_plan(&binary_yaml, &PathBuf::from("/nonexistent/config.yaml"))
                    .expect("binary-only fields are legal on binary");
            }
        }
    }
    // required: true + default → semantic contradiction.
    let yaml = "ensemble:\n  inputs:\n    a:\n      type: json\n      required: true\n      default: 1\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {x: \"$inputs.a\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("required+default must be rejected (R2)");
    assert!(err.to_string().contains("default"), "got: {err}");
}

/// R3: a binary root input can only be referenced whole (I1) — any path
/// projection on it is a parse error.
#[test]
fn mimo_r3_binary_input_path_projection_rejected() {
    let yaml = "ensemble:\n  inputs:\n    img:\n      type: binary\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {x: \"$inputs.img.crop\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("binary input path projection must be rejected (R3)");
    assert!(err.to_string().contains("binary"), "got: {err}");
}

/// R4: a step referencing an optional (no-default) input is a
/// CONDITIONAL step — its absence must be statically provable, so
/// downstream references are rejected exactly like E6-skip (D13/D5).
#[test]
fn mimo_r4_conditional_step_rules() {
    // Downstream reference to a conditional step → rejected.
    let yaml = "ensemble:\n  inputs:\n    opt:\n      type: json\n      required: false\n  steps:\n    - name: cond\n      model: m1\n      version: \"1\"\n      inputs: {x: \"$inputs.opt\"}\n    - name: consumer\n      model: m2\n      version: \"1\"\n      inputs: {y: \"$cond\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("conditional step downstream reference must be rejected (R4)");
    assert!(
        err.to_string().contains("optional") || err.to_string().contains("conditional"),
        "got: {err}"
    );
    // Conditional × stream → rejected (D34 rule 6 third arm).
    let yaml = "ensemble:\n  inputs:\n    opt:\n      type: json\n      required: false\n  steps:\n    - name: tail\n      model: m1\n      version: \"1\"\n      stream: true\n      inputs: {x: \"$inputs.opt\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("conditional streaming step must be rejected (D34)");
    let _ = err; // the rejection itself is the assertion
                 // With a default the value is always present — NOT conditional.
    let yaml = "ensemble:\n  inputs:\n    opt:\n      type: json\n      required: false\n      default: \"x\"\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      inputs: {x: \"$inputs.opt\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {y: \"$a\"}\n";
    parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect("default-carrying input must not make a step conditional (R4)");
    // An unreferenced conditional step is legal (outputs alias null
    // later) — the output step stays non-conditional.
    let yaml = "ensemble:\n  inputs:\n    a:\n      type: json\n    opt:\n      type: json\n      required: false\n  steps:\n    - name: cond\n      model: m1\n      version: \"1\"\n      inputs: {x: \"$inputs.opt\"}\n    - name: main\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$inputs.a\"}\n";
    let _ = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect("an unreferenced conditional step is legal");
}

/// R5: the `$inputs` namespace requires a declaration; declared configs
/// have no anonymous root (`$request` refs are rejected).
#[test]
fn mimo_r5_namespace_gating() {
    // Legacy config referencing $inputs → error (namespace undeclared).
    let yaml = "ensemble:\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {x: \"$inputs.a\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("$inputs in a legacy config must be rejected (R5)");
    let _ = err;
    // Declared config referencing $request (anonymous root) → error.
    let yaml = "ensemble:\n  inputs:\n    a:\n      type: json\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs: {x: \"$request\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("$request in a declared config must be rejected (R5)");
    let _ = err;
}

/// R12: static input-mode dispatch — all-Json → GroupJson, exactly one
/// whole Binary → BinaryPassThrough, everything else → parse error.
#[test]
fn mimo_r12_input_mode_dispatch() {
    let steps = |refs: &str| {
        format!(
            "ensemble:\n  inputs:\n    a:\n      type: json\n    img:\n      type: binary\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      inputs:\n{refs}\n"
        )
    };
    let plan = parse_ensemble_plan(
        &steps("        x: \"$inputs.a\"\n        y: \"$inputs.a\""),
        &PathBuf::from("/nonexistent/config.yaml"),
    )
    .unwrap();
    assert_eq!(plan.input_mode(0), Some(InputMode::GroupJson));
    let plan = parse_ensemble_plan(
        &steps("        x: \"$inputs.img\""),
        &PathBuf::from("/nonexistent/config.yaml"),
    )
    .unwrap();
    assert_eq!(plan.input_mode(0), Some(InputMode::BinaryPassThrough));
    // Mixed json+binary → rejected.
    let err = parse_ensemble_plan(
        &steps("        x: \"$inputs.a\"\n        y: \"$inputs.img\""),
        &PathBuf::from("/nonexistent/config.yaml"),
    )
    .expect_err("mixed json/binary inputs must be rejected (R12)");
    assert!(err.to_string().contains("binary"), "got: {err}");
}

/// R9: params × Binary is a static error in declared configs (moved from
/// the legacy runtime check once the input mode is parse-decidable).
#[test]
fn mimo_r9_params_x_binary_static_rejection() {
    let yaml = "ensemble:\n  inputs:\n    img:\n      type: binary\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      params:\n        t: 0.7\n      inputs: {x: \"$inputs.img\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("params × binary must be rejected at parse (R9)");
    assert!(err.to_string().contains("params"), "got: {err}");
}

/// R18: envelope parsing — named inputs, binary tail slicing in header
/// order, defaults, absent optionals, marker decode.
#[test]
fn mimo_r18_envelope_parsing() {
    let decl: IndexMap<String, InputDecl> = [
        ("text", json_decl()),
        ("sys", {
            let mut d = json_decl();
            d.required = false;
            d.default = Some(json!("be terse"));
            d
        }),
        ("opt", {
            let mut d = json_decl();
            d.required = false;
            d
        }),
        (
            "img",
            InputDecl {
                ty: InputType::Binary,
                required: true,
                default: None,
                content_type: Some("image/png".into()),
                shape: Some(vec![1, 3]),
                datatype: Some("FP32".into()),
            },
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

    // Happy: head + binary tail, header-order slicing, shape carried.
    let head = json!({"id": "r1", "inputs": [
        {"name": "text", "data": {"q": "hi"}},
        {"name": "img", "parameters": {"binary_data_size": 3}}
    ]});
    let head_bytes = serde_json::to_vec(&head).unwrap();
    let mut full = head_bytes.clone();
    full.extend_from_slice(b"\x00\x01\x02");
    let root = parse_root_inputs(
        EnsembleValue::Envelope {
            head,
            tail: Bytes::from(full).slice(head_bytes.len()..),
        },
        Some(&decl),
        false,
    )
    .unwrap();
    let RootInputs::Named { values, absent } = root else {
        panic!("named expected")
    };
    assert_eq!(values["text"], EnsembleValue::Json(json!({"q": "hi"})));
    assert_eq!(
        values["sys"],
        EnsembleValue::Json(json!("be terse")),
        "default filled"
    );
    match &values["img"] {
        EnsembleValue::Binary(b, ct, shape, dt) => {
            assert_eq!(b.as_ref(), b"\x00\x01\x02");
            assert_eq!(ct, "image/png");
            assert_eq!(shape.as_deref(), Some(&vec![1, 3][..]));
            assert_eq!(dt.as_deref(), Some("FP32"));
        }
        other => panic!("binary expected, got {other:?}"),
    }
    assert_eq!(absent, vec!["opt".to_string()]);

    // Missing required → 400.
    let head = json!({"inputs": [{"name": "text", "data": 1}]});
    let err = parse_root_inputs(EnsembleValue::Json(head), Some(&decl), false).unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "got {err:?}"
    );

    // Unknown input name → 400.
    let head = json!({"inputs": [
        {"name": "text", "data": 1},
        {"name": "nope", "data": 2},
        {"name": "img", "parameters": {"binary_data_size": 0}}
    ]});
    let err = parse_root_inputs(EnsembleValue::Json(head), Some(&decl), false).unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "got {err:?}"
    );

    // Tail overrun (binary_data_size beyond the tail) → 400.
    let head = json!({"inputs": [
        {"name": "text", "data": 1},
        {"name": "img", "parameters": {"binary_data_size": 10}}
    ]});
    let err = parse_root_inputs(
        EnsembleValue::Envelope {
            head,
            tail: Bytes::from_static(b"short"),
        },
        Some(&decl),
        false,
    )
    .unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "got {err:?}"
    );

    // Leftover tail bytes (more tail than declared) → 400.
    let head = json!({"inputs": [
        {"name": "text", "data": 1},
        {"name": "img", "parameters": {"binary_data_size": 1}}
    ]});
    let err = parse_root_inputs(
        EnsembleValue::Envelope {
            head,
            tail: Bytes::from_static(b"xx"),
        },
        Some(&decl),
        false,
    )
    .unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "got {err:?}"
    );

    // $binary_b64 marker as data (secondary in-JSON path) → Binary.
    let head = json!({"inputs": [
        {"name": "text", "data": 1},
        {"name": "img", "data": {"$binary_b64": "AAEC", "content_type": "image/jpeg"}}
    ]});
    let root = parse_root_inputs(EnsembleValue::Json(head), Some(&decl), false).unwrap();
    let RootInputs::Named { values, .. } = root else {
        panic!("named expected")
    };
    match &values["img"] {
        EnsembleValue::Binary(b, ct, _, _) => {
            assert_eq!(b.as_ref(), b"\x00\x01\x02", "base64 must decode");
            assert_eq!(ct, "image/jpeg");
        }
        other => panic!("binary expected, got {other:?}"),
    }
}

/// R19: legacy payloads — `$inputs` top-level key is a reserved
/// namespace (400, D14); an envelope container without a declaration is
/// 400 (TritonBinary keeps its historical rejection semantics).
#[test]
fn mimo_r19_legacy_reserved_namespace() {
    let err = parse_root_inputs(
        EnsembleValue::Json(json!({"$inputs": [{"name": "a"}]})),
        None,
        false,
    )
    .unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "got {err:?}"
    );
    let err = parse_root_inputs(
        EnsembleValue::Envelope {
            head: json!({"inputs": []}),
            tail: Bytes::new(),
        },
        None,
        false,
    )
    .unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "got {err:?}"
    );
    // Ordinary legacy payload passes through untouched.
    let root = parse_root_inputs(EnsembleValue::Json(json!({"a": 1})), None, false).unwrap();
    assert!(matches!(root, RootInputs::Single(EnsembleValue::Json(_))));
}

/// D32: LSBE-1 in-frame container split — happy path and malformed
/// branches (magic mismatch, head-length overflow, non-JSON head).
#[test]
fn mimo_lsbe1_split_envelope() {
    let head = br#"{"inputs": [{"name": "text", "data": 1}]}"#;
    let tail: &[u8] = b"\x01\x02\x03";
    let mut blob = Vec::new();
    blob.extend_from_slice(b"LSB1");
    blob.extend_from_slice(&(head.len() as u64).to_le_bytes());
    blob.extend_from_slice(head);
    blob.extend_from_slice(tail);
    let (v, t) = split_envelope(&blob).expect("valid container must split");
    assert_eq!(v, json!({"inputs": [{"name": "text", "data": 1}]}));
    assert_eq!(t.as_deref(), Some(tail));

    // Magic mismatch → 400.
    let err = split_envelope(b"XXXX................").unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "got {err:?}"
    );
    // Head length beyond the blob → 400.
    let mut bad = Vec::new();
    bad.extend_from_slice(b"LSB1");
    bad.extend_from_slice(&999u64.to_le_bytes());
    bad.extend_from_slice(head);
    let err = split_envelope(&bad).unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "got {err:?}"
    );
    // Head is not JSON → 400.
    let mut bad = Vec::new();
    bad.extend_from_slice(b"LSB1");
    bad.extend_from_slice(&5u64.to_le_bytes());
    bad.extend_from_slice(b"nope!");
    let err = split_envelope(&bad).unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "got {err:?}"
    );
    // Truncated before the header field → 400.
    let err = split_envelope(b"LSB1\x01").unwrap_err();
    assert!(
        matches!(err, AppError::InvalidRequestBody(_)),
        "got {err:?}"
    );
}

/// R10: a streaming step must not declare step.outputs (chunks have no
/// named-output semantics, D11).
#[test]
fn mimo_r10_streaming_step_outputs_rejected() {
    let yaml = "ensemble:\n  steps:\n    - name: s\n      model: m\n      version: \"1\"\n      stream: true\n      outputs:\n        crop:\n          type: binary\n      inputs: {x: \"$request\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("streaming step.outputs must be rejected (R10)");
    assert!(err.to_string().contains("outputs"), "got: {err}");
}

/// R7/R8: first-segment disambiguation — a declared step must be
/// referenced by alias; unknown alias → error; binary alias paths → error.
#[test]
fn mimo_r7_r8_binary_alias_disambiguation() {
    // Whole ref on a declared step → rejected.
    let yaml = "ensemble:\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      outputs:\n        crop:\n          type: binary\n      inputs: {x: \"$request\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$a\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("whole ref on a declared step must be rejected (R7)");
    let _ = err;
    // Unknown alias → rejected.
    let yaml = "ensemble:\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      outputs:\n        crop:\n          type: binary\n      inputs: {x: \"$request\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$a.other\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("unknown alias must be rejected (R7)");
    let _ = err;
    // Binary alias with a further path → rejected (R8).
    let yaml = "ensemble:\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      outputs:\n        crop:\n          type: binary\n      inputs: {x: \"$request\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$a.crop.x\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("binary alias path must be rejected (R8)");
    let _ = err;
    // Legal: whole binary alias.
    let yaml = "ensemble:\n  steps:\n    - name: a\n      model: m1\n      version: \"1\"\n      outputs:\n        crop:\n          type: binary\n      inputs: {x: \"$request\"}\n    - name: b\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$a.crop\"}\n";
    parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect("whole binary alias ref must parse");
}

/// D10 binary half (MIMO①): materialization — whole-response binary for
/// a path-less alias, `$binary_b64` marker decode for a path-specified
/// alias, and the type-mismatch error (declared binary, worker JSON).
#[test]
fn mimo_materialize_binary_outputs() {
    let step = EnsembleStep {
        name: "det".to_string(),
        model: "m".to_string(),
        version: Some("1".to_string()),
        inputs: HashMap::new(),
        when: None,
        stream: false,
        params: HashMap::new(),
        timeout_secs: None,
        on_error: OnErrorKind::Fail,
        retries: 0,
        outputs_decl: Some(
            [
                (
                    "thumb",
                    StepOutputDecl {
                        ty: InputType::Binary,
                        path: Some("$.thumb".to_string()),
                    },
                ),
                (
                    "raw",
                    StepOutputDecl {
                        ty: InputType::Binary,
                        path: None,
                    },
                ),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
        ),
    };
    // JSON response + marker objects → two binary outputs.
    let raw = EnsembleValue::Json(json!({
        "thumb": {"$binary_b64": "AAEC", "content_type": "image/jpeg"}
    }));
    // (path-less alias needs the BINARY response form — separate case)
    let err = materialize_step_outputs(&step, raw).unwrap_err();
    assert!(
        err.to_string().contains("binary"),
        "path-less binary alias on a JSON response must error, got: {err}"
    );
    // Binary response → path-less alias passes through; marker alias
    // errors (it needs a JSON response).
    let raw = EnsembleValue::Binary(
        Bytes::from_static(b"\x00\x01"),
        "image/png".into(),
        None,
        None,
    );
    let err = materialize_step_outputs(&step, raw).unwrap_err();
    assert!(
        err.to_string().contains("JSON"),
        "marker alias on a binary response must error, got: {err}"
    );
    // Mixed response case: whole-response alias only.
    let step_whole = EnsembleStep {
        outputs_decl: Some(
            [(
                "raw",
                StepOutputDecl {
                    ty: InputType::Binary,
                    path: None,
                },
            )]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
        ),
        ..step.clone()
    };
    let out = materialize_step_outputs(
        &step_whole,
        EnsembleValue::Binary(
            Bytes::from_static(b"\x00\x01"),
            "image/png".into(),
            None,
            None,
        ),
    )
    .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].0, "det.raw");
    match &out[0].1 {
        EnsembleValue::Binary(b, ct, _, _) => {
            assert_eq!(b.as_ref(), b"\x00\x01");
            assert_eq!(ct, "image/png");
        }
        other => panic!("binary expected, got {other:?}"),
    }
    // Marker decode happy path (JSON response, marker alias only).
    let step_marker = EnsembleStep {
        outputs_decl: Some(
            [(
                "thumb",
                StepOutputDecl {
                    ty: InputType::Binary,
                    path: Some("$.thumb".into()),
                },
            )]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
        ),
        ..step
    };
    let out = materialize_step_outputs(
        &step_marker,
        EnsembleValue::Json(
            json!({"thumb": {"$binary_b64": "AAEC", "content_type": "image/jpeg"}}),
        ),
    )
    .unwrap();
    match &out[0].1 {
        EnsembleValue::Binary(b, ct, _, _) => {
            assert_eq!(b.as_ref(), b"\x00\x01\x02");
            assert_eq!(ct, "image/jpeg");
        }
        other => panic!("binary expected, got {other:?}"),
    }
}

// ===== audit (batch 4/5 review): defect reproduction tests =====

/// §5.5.7: legacy configs keep byte-identical payload classification.
/// The old gRPC unary / server-streaming paths parsed ANY valid JSON
/// (arrays, scalars, whitespace-prefixed objects) into Json; the
/// `{`-sniff in ensemble_payload_from_bytes re-classifies them as
/// Binary (regression, and HTTP/batch keep parsing them as Json —
/// cross-transport parity break). Only the LSBE-1 magic may pre-empt
/// JSON parsing (it can never be valid JSON).
#[test]
fn test_audit_legacy_payload_json_array_stays_json() {
    let payload = ensemble_payload_from_bytes(&Bytes::from_static(b"[1,2]"), None).unwrap();
    match payload {
        EnsembleValue::Json(v) => assert_eq!(v, json!([1, 2])),
        other => panic!("legacy JSON array must stay Json, got {other:?}"),
    }
}

/// R5/R13: `$inputs.NAME` refs in ensemble.outputs must name a DECLARED
/// input — step inputs reject undeclared names at parse, outputs refs
/// currently slip through and silently degrade to `data: null` at
/// runtime (the D5 channel is for ABSENT sources, not config typos).
#[test]
fn test_audit_outputs_ref_undeclared_input_name_rejected() {
    let yaml = r#"ensemble:
  inputs:
    text: {type: json}
  outputs:
    a: "$inputs.nope"
  steps:
    - name: s
      model: m
      version: "1"
      inputs: {x: "$inputs.text"}
"#;
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap_err();
    assert!(
        err.to_string().contains("undeclared input"),
        "an undeclared $inputs name in outputs must be a config error, got: {err}"
    );
}

/// R5: a legacy config (no inputs declaration) rejects ANY `$inputs.*`
/// ref — step inputs are rejected, outputs refs currently slip through
/// and degrade to null at runtime.
#[test]
fn test_audit_outputs_ref_inputs_namespace_rejected_legacy() {
    let yaml = r#"ensemble:
  outputs:
    a: "$inputs.x"
  steps:
    - name: s
      model: m
      version: "1"
      inputs: {x: "$request"}
"#;
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml")).unwrap_err();
    assert!(
        err.to_string().contains("inputs"),
        "a legacy $inputs namespace ref in outputs must be a config error (R5), got: {err}"
    );
}

/// §5.5.3: multi-segment paths on UNDECLARED steps stay
/// declaration-only. `$stepX.a.b` used to fail as a literal-key lookup;
/// the new resolver silently takes the FIRST segment and drops `.b`,
/// producing wrong data instead of an error.
#[test]
fn test_audit_legacy_multisegment_step_ref_not_truncated() {
    let plan = legacy_plan();
    let mut context = HashMap::new();
    context.insert("s".to_string(), EnsembleValue::Json(json!({"a": {"b": 1}})));
    assert!(
        resolve_ref(&plan, "$s.a.b", &context).is_err(),
        "multi-segment legacy step refs must be rejected, not silently truncated \
             to the first segment"
    );
}

/// Same rule for the legacy anonymous root: `$request.a.b` must not
/// silently resolve to `request["a"]`.
#[test]
fn test_audit_legacy_multisegment_request_ref_not_truncated() {
    let plan = legacy_plan();
    let mut context = HashMap::new();
    context.insert(
        "request".to_string(),
        EnsembleValue::Json(json!({"a": {"b": 1}})),
    );
    assert!(
        resolve_ref(&plan, "$request.a.b", &context).is_err(),
        "multi-segment legacy request refs must be rejected, not silently truncated"
    );
}

/// B3 fix: a tolerated (unknown) binary element in a dags-form envelope
/// must still consume its declared tail slice — header-order slicing
/// means a skip that leaves bytes unaccounted misaligns every later
/// binary element.
#[test]
fn test_audit_dags_tolerated_binary_element_consumes_tail() {
    let decl: IndexMap<String, InputDecl> = [(
        "img".to_string(),
        InputDecl {
            ty: InputType::Binary,
            required: true,
            default: None,
            content_type: Some("image/png".to_string()),
            shape: None,
            datatype: None,
        },
    )]
    .into_iter()
    .collect();
    let head = json!({"inputs": [
        {"name": "other_set_img", "parameters": {"binary_data_size": 2}},
        {"name": "img", "parameters": {"binary_data_size": 3}}
    ]});
    let root = parse_root_inputs(
        EnsembleValue::Envelope {
            head,
            tail: Bytes::from_static(b"\x00\x00\x01\x02\x03"),
        },
        Some(&decl),
        true,
    )
    .unwrap();
    let RootInputs::Named { values, .. } = root else {
        panic!("named expected")
    };
    match &values["img"] {
        EnsembleValue::Binary(b, _, _, _) => {
            assert_eq!(
                b.as_ref(),
                b"\x01\x02\x03",
                "later elements must slice after the tolerated bytes"
            );
        }
        other => panic!("binary expected, got {other:?}"),
    }
}

// === Audit (ensemble core, 2026-08-14) — defect-proof tests ===

/// AUDIT-C1 (R8): `ensemble.outputs` refs must obey the binary-alias
/// whole-only rule, exactly like step-input refs — `$stepX.BINALIAS.path`
/// where BINALIAS is declared `type: binary` is a parse-time config error
/// (R8). Step inputs enforce this in analyze_static_types; the outputs
/// validator (validate_outputs_rules) checks the alias EXISTS but never
/// checks its declared type, so the config loads and only fails (silently,
/// via the D5 null channel) at request time.
#[test]
fn audit_c1_outputs_binary_alias_path_rejected_r8() {
    let yaml = "ensemble:\n  steps:\n    - name: enc\n      model: m1\n      version: \"1\"\n      outputs:\n        crop:\n          type: binary\n          path: \"$.crop\"\n      inputs: {x: \"$request\"}\n  outputs:\n    thumb: \"$enc.crop.foo\"\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("a path projection on a declared BINARY alias must be parse-rejected (R8)");
    assert!(err.to_string().contains("R8") || err.to_string().contains("binary"), "got: {err}");
}

/// AUDIT-C2 (D5/§5.5.6): build_response's null channel is for ABSENT
/// sources only (skipped step / missing optional input). A runtime TYPE
/// error — e.g. the aliased ref does a field projection on a step that
/// returned Binary — is a real error (resolve_ref → 400-class) and must
/// propagate; today EVERY resolve_ref error is swallowed into
/// `data: null` + warn, so a broken DAG contract answers 200 with a null
/// sink instead of failing the request.
#[test]
fn audit_c2_build_response_propagates_non_absence_errors() {
    let plan = parse_ensemble_plan(
        "ensemble:\n  steps:\n    - name: s1\n      model: m1\n      version: \"1\"\n      inputs: {x: \"$request\"}\n  outputs:\n    thumb: \"$s1.thumb\"\n",
        &PathBuf::from("/nonexistent/config.yaml"),
    )
    .expect("legacy single-field outputs ref parses (R13)");
    let mut context = HashMap::new();
    // s1 ran and returned BINARY (worker non-JSON media type) — the alias
    // ref `$s1.thumb` is a field projection on bytes: a type error, NOT an
    // absence.
    context.insert(
        "s1".to_string(),
        EnsembleValue::Binary(Bytes::from_static(b"\x01\x02"), "image/png".into(), None, None),
    );
    let result = build_response(&plan, "demo", &context);
    assert!(
        result.is_err(),
        "a type error on an outputs alias must fail the request, not emit data:null (D5 covers absence only); got Ok"
    );
}

/// AUDIT-C3 (E2/B5): `ensemble.output: "$stepN.a.b"` is a multi-segment
/// field path. Legacy refs explicitly reject multi-segment paths (B5), and
/// select_output_field does a SINGLE literal key lookup (`v.get("a.b")`) —
/// so this config either 500s at request time ("field 'a.b' not found") or,
/// worse, silently returns a literal dotted key's value. It must be
/// parse-rejected like every other multi-segment legacy path.
#[test]
fn audit_c3_output_multi_segment_field_rejected() {
    let yaml = "ensemble:\n  steps:\n    - name: s1\n      model: m1\n      version: \"1\"\n      inputs: {x: \"$request\"}\n    - name: s2\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$s1\"}\n  output: \"$s2.a.b\"\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("multi-segment ensemble.output field must be parse-rejected (B5 parity)");
    assert!(err.to_string().contains("output") || err.to_string().contains("segment"), "got: {err}");
}

/// AUDIT-C5 (§4.2 regression, batch 6 P2 — FIXED): the pipeline chunk
/// consumer wraps upstream chunk bytes in RawJson; pre-P2 the chunk was
/// parsed as JSON and a non-JSON chunk failed the chain ("pipeline chunk
/// ... is not valid JSON"). Raw splicing without validation let a
/// binary/garbage chunk produce malformed (or injected-key) downstream
/// JSON. The fix validates chunks at both RawJson construction seams:
/// validate_pipeline_chunk (chain consumer) and unary_response_to_value's
/// raw-eligibility gate.
#[test]
fn audit_c5_pipeline_non_json_chunk_must_not_splice_raw() {
    // Chain consumer seam: garbage fails the chain with the historical error.
    let garbage = Bytes::from_static(b"\xff\x00not-json");
    let err = validate_pipeline_chunk("prev", &garbage).unwrap_err();
    assert!(
        err.to_string().contains("is not valid JSON"),
        "a non-JSON upstream chunk must fail the chain (historical behavior), got: {err}"
    );
    // Injection-shaped chunk (`1, "injected": true` splices into valid JSON
    // with attacker-controlled keys) — rejected as trailing data.
    let injected = Bytes::from_static(br#"1, "injected": true"#);
    validate_pipeline_chunk("prev", &injected)
        .expect_err("an injection-shaped chunk must be rejected, not spliced");
    // Valid chunks pass.
    validate_pipeline_chunk("prev", br#"{"text": "hi"}"#)
        .expect("a valid JSON chunk must pass");

    // Unary raw-residency seam: invalid JSON must not become RawJson.
    let bad = pb::SingleResponse {
        data: garbage,
        media_type: "application/json".to_string(),
        ..Default::default()
    };
    unary_response_to_value(
        "s",
        pb::Response {
            payload: Some(pb::response::Payload::Single(bad)),
            ..Default::default()
        },
        true,
    )
    .expect_err("invalid JSON must error even when raw-eligible");
}

/// AUDIT-C7 (§5.5.3 reserved namespaces): `$request` is the reserved root
/// ref. A step NAMED `request` shadows it: validate_dag/topological_layers
/// treat `$request` refs as the ROOT (never as a dep edge on that step), so
/// the step's result lands in context key `request` mid-run and every
/// LATER layer's `$request` ref silently resolves to the step's output
/// instead of the request body — order-dependent data corruption. The
/// reserved name must be rejected at parse time.
#[test]
fn audit_c7_step_named_request_is_rejected() {
    let yaml = "ensemble:\n  steps:\n    - name: request\n      model: m1\n      version: \"1\"\n      inputs: {x: \"$request\"}\n    - name: mid\n      model: m2\n      version: \"1\"\n      inputs: {x: \"$request\"}\n    - name: s3\n      model: m3\n      version: \"1\"\n      inputs: {a: \"$mid\", b: \"$request\"}\n";
    let err = parse_ensemble_plan(yaml, &PathBuf::from("/nonexistent/config.yaml"))
        .expect_err("a step named 'request' shadows the reserved root namespace");
    assert!(err.to_string().contains("request"), "got: {err}");
}

/// AUDIT-C6 (R18/D31): KServe V2 envelope input names are unique by spec.
/// A duplicate `name` currently OVERWRITES the first element silently
/// (IndexMap::insert last-wins) — the client sent two values, one is
/// dropped without any signal. Must be a 400.
#[test]
fn audit_c6_duplicate_envelope_input_names_rejected() {
    let decl: IndexMap<String, InputDecl> = [("text".to_string(), json_decl())]
        .into_iter()
        .collect();
    let dup = json!({"inputs": [
        {"name": "text", "data": 1},
        {"name": "text", "data": 2}
    ]});
    let result = parse_root_inputs(EnsembleValue::Json(dup), Some(&decl), false);
    assert!(
        result.is_err(),
        "duplicate envelope input names must be a 400 (R18), not silent last-wins overwrite; got {result:?}"
    );
}

// === D17-1/D3 aggregation coverage (2026-08-14 audit gap) ===

/// D17-1: all-Binary frames → byte concat, content-type from the FIRST frame.
#[test]
fn d17_binary_frames_concat_with_first_content_type() {
    let mut agg = BidiAggregator::new(1024);
    agg.push(Bytes::from_static(b"\x01\x02"), false, Some("image/png")).unwrap();
    agg.push(Bytes::from_static(b"\x03"), false, Some("image/jpeg")).unwrap();
    match agg.finish().unwrap() {
        EnsembleValue::Binary(b, ct, ..) => {
            assert_eq!(b.as_ref(), b"\x01\x02\x03", "byte concat (D17-1)");
            assert_eq!(ct, "image/png", "content-type from the FIRST frame (D17-1)");
        }
        other => panic!("binary expected, got {other:?}"),
    }
}

/// D17-2: all-Json frames → array; a single frame stays unwrapped
/// (historical single-frame semantics).
#[test]
fn d17_json_frames_aggregate_to_array_single_unwrapped() {
    let mut agg = BidiAggregator::new(1024);
    agg.push(Bytes::from_static(br#"{"a":1}"#), true, None).unwrap();
    let v = agg.finish().unwrap();
    assert!(
        matches!(&v, EnsembleValue::Json(j) if j["a"] == json!(1)),
        "single Json frame → the value itself (not wrapped)"
    );

    let mut agg = BidiAggregator::new(1024);
    agg.push(Bytes::from_static(br#"{"a":1}"#), true, None).unwrap();
    agg.push(Bytes::from_static(br#"{"b":2}"#), true, None).unwrap();
    match agg.finish().unwrap() {
        EnsembleValue::Json(Value::Array(items)) => {
            assert_eq!(items.len(), 2, "multi Json frames → array (D17-2)")
        }
        other => panic!("array expected, got {other:?}"),
    }
}

/// D3: the cumulative cap (max_request_body_bytes semantics) →
/// PayloadTooLarge (413/ResourceExhausted), enforced mid-aggregation.
#[test]
fn d3_aggregation_cap_is_payload_too_large() {
    let mut agg = BidiAggregator::new(4);
    agg.push(Bytes::from_static(b"ab"), false, None).unwrap();
    let err = agg.push(Bytes::from_static(b"cde"), false, None).unwrap_err();
    assert!(
        matches!(err, AppError::PayloadTooLarge { .. }),
        "D3 cap breach → PayloadTooLarge, got {err:?}"
    );
}

/// P6/E8-1 (audit): a dags-form plan's OUTER steps are empty — the warm
/// pass must iterate the SETS' steps, or a dags-form ensemble's sub-models
/// never get touch_last_used (LRU could evict a live DAG dependency).
/// Observable via lru_eviction_candidate: the warm-referenced version gets
/// a fresh stamp, so the never-used sibling becomes the candidate.
#[tokio::test]
async fn p6_warm_covers_dags_form_sub_models() {
    let tmp = std::env::temp_dir().join(format!(
        "lite-server-warm-dags-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let dir = tmp.join("ensdags").join("1");
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(
        dir.join("config.yaml"),
        "ensemble:\n  dags:\n    default:\n      steps:\n        - name: s\n          model: warm_sub\n          version: \"1\"\n          inputs: {x: \"$request\"}\n",
    )
    .await
    .unwrap();

    let registry = std::sync::Arc::new(crate::registry::ModelRegistry::new());
    for v in ["1", "2"] {
        registry
            .register(
                "warm_sub",
                v,
                crate::config::ModelConfig::default(),
                crate::registry::types::ModelType::LitAPI,
                std::env::temp_dir(),
            )
            .unwrap();
        registry.mark_ready("warm_sub", v).unwrap();
    }

    let plans = std::sync::Arc::new(EnsemblePlanCache::new());
    spawn_ensemble_warm(
        tmp.clone(),
        Some(plans),
        registry.clone(),
        "ensdags".to_string(),
        "1".to_string(),
    );

    // The warm runs detached — poll for the touch.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if registry.lru_eviction_candidate("warm_sub").as_deref() == Some("2") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "dags-form warm never touched the referenced sub-model version"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let _ = tokio::fs::remove_dir_all(&tmp).await;
}
