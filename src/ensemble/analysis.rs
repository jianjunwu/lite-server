use crate::error::AppError;
use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::*;

/// E2 (batch 3): resolve `ensemble.output` — `$stepN` or `$stepN.field` —
/// into the output step index + optional field path. Omitted = `steps.last()`
/// (historical semantics).
pub(crate) fn resolve_output(
    output: Option<&str>,
    steps: &[EnsembleStep],
) -> Result<(usize, Option<String>), AppError> {
    let Some(output) = output else {
        return Ok((steps.len() - 1, None));
    };
    let caps = REF_RE.captures(output).ok_or_else(|| {
        AppError::Config(format!(
            "invalid ensemble.output '{}': expected $stepN or $stepN.field",
            output
        ))
    })?;
    let source = caps.get(1).unwrap().as_str();
    if source == "request" {
        return Err(AppError::Config(
            "ensemble.output must reference a step, not $request".to_string(),
        ));
    }
    let idx = steps
        .iter()
        .position(|s| s.name == source)
        .ok_or_else(|| {
            AppError::Config(format!(
                "ensemble.output references unknown step '{}'",
                source
            ))
        })?;
    let field = caps.get(2).map(|m| m.as_str().to_string());
    // C3 (B5 parity): the output field is a SINGLE segment — legacy refs
    // reject multi-segment paths, and select_output_field does a single
    // literal key lookup, so `a.b` would 500 or silently match a dotted key.
    if let Some(f) = &field {
        if f.contains('.') {
            return Err(AppError::Config(format!(
                "ensemble.output '{output}': multi-segment field paths are not \
                 supported — use a single field ($stepN.field)"
            )));
        }
    }
    Ok((idx, field))
}

/// MIMO (batch 4①, R1-R5/R9/R11/R12): build the static type environment.
/// With an `inputs` declaration every ref's type is parse-decidable:
/// - R1/R2: declaration field validation (names, type gating);
/// - R3/R5: namespace rules (binary paths, legacy `$request`, undeclared `$inputs`);
/// - R9: params × Binary is a static error once the mode is decided;
/// - R4: optional-input refs make the step CONDITIONAL (runtime skip, D13);
/// - R11/R12: per-step input-mode dispatch (GroupJson / BinaryPassThrough, mixed → error).
///
/// Legacy configs (no declaration) return all-None modes (dynamic
/// dispatch, byte-identical) and reject `$inputs` (R5).
#[allow(clippy::type_complexity)] // (modes, conditional) is a natural parse pair
pub(crate) fn analyze_static_types(
    steps: &[EnsembleStep],
    inputs_decl: Option<&IndexMap<String, InputDecl>>,
) -> Result<(Vec<Option<InputMode>>, Vec<Vec<String>>), AppError> {
    let mut modes = Vec::with_capacity(steps.len());
    let mut conditional = Vec::with_capacity(steps.len());

    if let Some(decl) = inputs_decl {
        // R1/R2: declaration-level validation (once per plan).
        for (name, d) in decl {
            if !IDENT_RE.is_match(name) {
                return Err(AppError::Config(format!(
                    "ensemble.inputs name '{name}' is not a valid identifier \
                     ([A-Za-z_][A-Za-z0-9_]*, R1)"
                )));
            }
            match d.ty {
                InputType::Json => {
                    if d.content_type.is_some() || d.shape.is_some() || d.datatype.is_some() {
                        return Err(AppError::Config(format!(
                            "ensemble.inputs '{name}': content_type/shape/datatype are \
                             binary-only fields (R2)"
                        )));
                    }
                }
                InputType::Binary => {
                    if d.default.is_some() {
                        return Err(AppError::Config(format!(
                            "ensemble.inputs '{name}': default is json-only (R2)"
                        )));
                    }
                }
            }
            if d.required && d.default.is_some() {
                return Err(AppError::Config(format!(
                    "ensemble.inputs '{name}': required: true conflicts with a default (R2)"
                )));
            }
        }
    }

    for step in steps {
        // R10: streaming chunks have no named-output semantics (D11).
        if step.stream && step.outputs_decl.is_some() {
            return Err(AppError::Config(format!(
                "step '{}': stream: true cannot declare step.outputs \
                 (streaming chunks have no named outputs, R10/D11)",
                step.name
            )));
        }
        // Ref analysis. The R7/R8 disambiguation of DECLARED step refs
        // applies in both modes (step.outputs declarations are mode-
        // independent); the `$inputs`/`$request` namespace rules and the
        // static input-mode dispatch are declared-mode-only (the legacy
        // root's type is unknown at parse — Raw binary may arrive).
        let mut mode: Option<InputMode> = None;
        let mut cond: Vec<String> = Vec::new();
        for ref_str in step.inputs.values() {
            let caps = REF_RE.captures(ref_str).ok_or_else(|| {
                AppError::Config(format!("invalid reference format: {}", ref_str))
            })?;
            let source = caps.get(1).unwrap().as_str();
            let rest = caps.get(2).map(|m| m.as_str());
            match source {
                "request" => {
                    if inputs_decl.is_some() {
                        return Err(AppError::Config(format!(
                            "step '{}' references '{}' — declared-inputs ensembles have no \
                             anonymous root; use $inputs.NAME (R5)",
                            step.name, ref_str
                        )));
                    }
                    // Legacy root: dynamic type — no static mode.
                }
                "inputs" => {
                    let Some(decl) = inputs_decl else {
                        return Err(AppError::Config(format!(
                            "step '{}' references '{}' — $inputs requires an \
                             ensemble.inputs declaration (R5)",
                            step.name, ref_str
                        )));
                    };
                    let name = rest.and_then(|r| r.split('.').next()).unwrap_or("");
                    let d = decl.get(name).ok_or_else(|| {
                        AppError::Config(format!(
                            "step '{}' references undeclared input '{}' (R5)",
                            step.name, ref_str
                        ))
                    })?;
                    let has_path = rest.map(|r| r.contains('.')).unwrap_or(false);
                    match d.ty {
                        InputType::Json => {
                            if mode == Some(InputMode::BinaryPassThrough) {
                                return Err(AppError::Config(format!(
                                    "step '{}': mixed JSON/Binary inputs are forbidden (R12) \
                                     — a binary input must be the step's sole input",
                                    step.name
                                )));
                            }
                            mode = Some(InputMode::GroupJson);
                            if !d.required && d.default.is_none() {
                                cond.push(name.to_string());
                            }
                        }
                        InputType::Binary => {
                            if has_path {
                                return Err(AppError::Config(format!(
                                    "step '{}': binary input '{}' must be referenced whole \
                                     (R3 — no field projection on binary)",
                                    step.name, ref_str
                                )));
                            }
                            if mode.is_some() {
                                return Err(AppError::Config(format!(
                                    "step '{}': a binary input must be the step's SOLE whole \
                                     input (R12); '{}' combines with other inputs",
                                    step.name, ref_str
                                )));
                            }
                            mode = Some(InputMode::BinaryPassThrough);
                            if !d.required && d.default.is_none() {
                                cond.push(name.to_string());
                            }
                        }
                    }
                }
                _ => {
                    // Step ref — the static type comes from the source step's
                    // declaration (undeclared steps are json, I3).
                    let src = steps
                        .iter()
                        .find(|s| s.name == source)
                        .ok_or_else(|| {
                            AppError::Config(format!(
                                "step '{}' references unknown step '{}'",
                                step.name, source
                            ))
                        })?;
                    match &src.outputs_decl {
                        None => {
                            // Undeclared → static type json (whole or legacy
                            // field path). Legacy configs stay dynamic
                            // (mode None) — declared configs get GroupJson.
                            if inputs_decl.is_some() {
                                if mode == Some(InputMode::BinaryPassThrough) {
                                    return Err(AppError::Config(format!(
                                        "step '{}': mixed JSON/Binary inputs are forbidden (R12)",
                                        step.name
                                    )));
                                }
                                mode = Some(InputMode::GroupJson);
                            }
                        }
                        Some(alias_decl) => {
                            // R7: a declared step must be referenced by alias
                            // (both modes — the whole ref has no type).
                            let alias = rest.and_then(|r| r.split('.').next()).ok_or_else(|| {
                                AppError::Config(format!(
                                    "step '{}': '{}' — step '{}' declares outputs and must be \
                                     referenced by alias ($stepX.ALIAS, R7)",
                                    step.name, ref_str, source
                                ))
                            })?;
                            let d = alias_decl.get(alias).ok_or_else(|| {
                                AppError::Config(format!(
                                    "step '{}': '{}' — '{}' is not a declared alias of step \
                                     '{}' (R7)",
                                    step.name, ref_str, alias, source
                                ))
                            })?;
                            let has_path = rest.map(|r| r.contains('.')).unwrap_or(false);
                            match d.ty {
                                InputType::Binary => {
                                    // R8: binary aliases are whole-only.
                                    if has_path {
                                        return Err(AppError::Config(format!(
                                            "step '{}': binary alias '{}' must be referenced \
                                             whole (R8)",
                                            step.name, ref_str
                                        )));
                                    }
                                    if inputs_decl.is_some() {
                                        if mode.is_some() {
                                            return Err(AppError::Config(format!(
                                                "step '{}': a binary input must be the step's \
                                                 SOLE whole input (R12)",
                                                step.name
                                            )));
                                        }
                                        mode = Some(InputMode::BinaryPassThrough);
                                    }
                                }
                                InputType::Json => {
                                    // MIMO② (D10): json alias projections —
                                    // static type json (GroupJson).
                                    if inputs_decl.is_some() {
                                        if mode == Some(InputMode::BinaryPassThrough) {
                                            return Err(AppError::Config(format!(
                                                "step '{}': mixed JSON/Binary inputs are forbidden (R12)",
                                                step.name
                                            )));
                                        }
                                        mode = Some(InputMode::GroupJson);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        // R9: with the mode parse-decided, params × Binary is static.
        if mode == Some(InputMode::BinaryPassThrough) && !step.params.is_empty() {
            return Err(AppError::Config(format!(
                "step '{}': params cannot combine with a binary input (R9/E3)",
                step.name
            )));
        }
        modes.push(mode);
        conditional.push(cond);
    }
    Ok((modes, conditional))
}

/// E6 (batch 4, D5/D34) + MIMO R4 (batch 4①, D13): an ABSENTABLE step
/// (`on_error: skip`, or conditional — referencing an optional input) is
/// absent from the context when its error fires / its input is missing —
/// every consumer of its output must be statically provable as safe at parse
/// time:
///   1. no OTHER step may reference it (`$name` whole or `$name.field` —
///      both dangle identically at runtime);
///   2. `ensemble.output` may not point at it (the single-output contract
///      has no null channel — only E7's multi-sink outputs do, D5);
///   3. a streaming step may never be absent (D34 rule 6: the streaming
///      response contract promises a stream unconditionally).
pub(crate) fn validate_skip_rules(
    steps: &[EnsembleStep],
    output_step: usize,
    conditional_refs: &[Vec<String>],
    // E7: with ensemble.outputs present the single-output contract is
    // REPLACED — an absentable output step is fine (its aliases use the D5
    // null channel); the rule only binds the implicit single output.
    outputs_present: bool,
) -> Result<(), AppError> {
    let absentable: HashSet<&str> = steps
        .iter()
        .enumerate()
        .filter(|(i, s)| {
            s.on_error == OnErrorKind::Skip
                || !conditional_refs[*i].is_empty()
                || s.when.is_some() // E8-2: a when-false step is absent too
        })
        .map(|(_, s)| s.name.as_str())
        .collect();
    if absentable.is_empty() {
        return Ok(());
    }

    // Rule 3 first (cheapest): stream × absence is a structural
    // contradiction (skip arm names skip; conditional arm names the input).
    for (i, s) in steps.iter().enumerate() {
        if !s.stream {
            continue;
        }
        if s.on_error == OnErrorKind::Skip {
            return Err(AppError::Config(format!(
                "step '{}': on_error: skip cannot combine with stream: true \
                 (a streaming response promises a stream unconditionally, D34)",
                s.name
            )));
        }
        if !conditional_refs[i].is_empty() {
            return Err(AppError::Config(format!(
                "step '{}': a streaming step cannot reference optional inputs ({:?}) \
                 (a streaming response promises a stream unconditionally, D34)",
                s.name, conditional_refs[i]
            )));
        }
    }

    // Rule 1: any input reference whose first segment names an absentable
    // step.
    for s in steps {
        if absentable.contains(s.name.as_str()) {
            continue; // self-references are caught by validate_dag anyway
        }
        for ref_str in s.inputs.values() {
            let Some(caps) = REF_RE.captures(ref_str) else {
                continue; // malformed refs are reported elsewhere (validate_dag)
            };
            let source = caps.get(1).unwrap().as_str();
            if absentable.contains(source) {
                return Err(AppError::Config(format!(
                    "step '{}' references potentially-absent step '{}' via '{}' — \
                     an on_error: skip or optional-input step leaves the DAG dangling \
                     (D5/R4); use ensemble.outputs for a null-when-absent alias",
                    s.name, source, ref_str
                )));
            }
        }
    }

    // Rule 2: the single-output contract has no null channel (E7's
    // multi-sink outputs DO — D5 — so the rule binds only without them).
    let output_step = &steps[output_step];
    if !outputs_present && absentable.contains(output_step.name.as_str()) {
        return Err(AppError::Config(format!(
            "ensemble.output references potentially-absent step '{}' — the single-output \
             contract has no null channel; use ensemble.outputs (alias = null when \
             absent, D5)",
            output_step.name
        )));
    }

    Ok(())
}

/// Production plan loader: resolve the model dir, read config.yaml and parse
/// into a cached plan (P0). Wrapped by [`get_ensemble_plan`].
pub(crate) async fn load_ensemble_plan(
    repo_path: &std::path::Path,
    model: &str,
    version: &str,
) -> Result<Arc<EnsemblePlan>, AppError> {
    let model_dir = crate::validation::resolve_model_dir(repo_path, model, version)?;
    let config_path = model_dir.join("config.yaml");
    // Stat BEFORE the read: a write landing between stat and read leaves the
    // stored mtime OLDER than the file's, so the interval re-check re-parses
    // (safe). The reverse order could pin a fresh mtime onto stale content
    // and serve it indefinitely (the watcher-miss fallback's whole point).
    let source_mtime = std::fs::metadata(&config_path)
        .and_then(|m| m.modified())
        .ok();
    let content = tokio::fs::read_to_string(&config_path)
        .await
        .map_err(|e| AppError::Config(format!("failed to read ensemble config: {}", e)))?;
    let mut plan = parse_ensemble_plan(&content, &config_path)?;
    plan.source_mtime = source_mtime;
    Ok(Arc::new(plan))
}

pub(crate) fn validate_dag(steps: &[EnsembleStep]) -> Result<(), AppError> {
    // An empty DAG has no output step — reject at parse instead of panicking
    // on `steps.len() - 1` (pre-batch-0 this degenerated into a request-time
    // 500; with the plan cache it must be a load-time config error).
    if steps.is_empty() {
        return Err(AppError::Config("ensemble declares no steps".to_string()));
    }
    let step_names: HashSet<&str> = steps.iter().map(|s| s.name.as_str()).collect();

    // Check for duplicate names
    if step_names.len() != steps.len() {
        return Err(AppError::Config("duplicate step names in ensemble".to_string()));
    }

    // C7: `request` and `inputs` are reserved root namespaces ($request /
    // $inputs.NAME). A step with such a name is unreferenceable — refs
    // resolve to the root, never to the step — and its result overwrites
    // the reserved context key, corrupting later layers' root refs.
    for s in steps {
        if s.name == "request" || s.name == "inputs" {
            return Err(AppError::Config(format!(
                "step name '{}' is a reserved namespace ($request / $inputs) — \
                 rename the step",
                s.name
            )));
        }
    }

    // Build dependency graph
    let mut dependencies: HashMap<&str, HashSet<&str>> = HashMap::new();
    for step in steps {
        let deps = dependencies.entry(&step.name).or_default();
        for ref_str in step.inputs.values() {
            let caps = REF_RE.captures(ref_str).ok_or_else(|| {
                AppError::Config(format!("invalid reference format: {}", ref_str))
            })?;
            let source = caps.get(1).unwrap().as_str();
            // MIMO: `$inputs.NAME` is a named-input ref, not a step dep (its
            // validity is checked by analyze_static_types).
            if source != "request" && source != "inputs" && !step_names.contains(source) {
                return Err(AppError::Config(format!(
                    "step '{}' references unknown step '{}'",
                    step.name, source
                )));
            }
            if source != "request" && source != "inputs" {
                deps.insert(source);
            }
        }
    }

    // Kahn's algorithm for cycle detection
    let mut in_degree: HashMap<&str, usize> = steps.iter()
        .map(|s| (s.name.as_str(), 0))
        .collect();
    for (step_name, deps) in &dependencies {
        for _dep in deps {
            *in_degree.get_mut(*step_name).unwrap() += 1;
        }
    }

    let mut queue: Vec<&str> = in_degree.iter()
        .filter(|(_, d)| **d == 0)
        .map(|(n, _)| *n)
        .collect();
    let mut visited = 0;

    while let Some(node) = queue.pop() {
        visited += 1;
        for step in steps {
            if dependencies.get(step.name.as_str()).map(|d| d.contains(node)).unwrap_or(false) {
                let deg = in_degree.get_mut(step.name.as_str()).unwrap();
                *deg -= 1;
                if *deg == 0 {
                    queue.push(&step.name);
                }
            }
        }
    }

    if visited != steps.len() {
        return Err(AppError::Config("cycle detected in ensemble DAG".to_string()));
    }

    Ok(())
}

/// §4.0/D16 form-dispatching validation for `stream: true` steps (batch 0:
/// only the non-pipeline form is open). Rule 2 (streaming output not
/// referenced) is the definition of the form split — a referenced streaming
/// output IS the pipeline form, rejected here with an explicit
/// batch-2 message instead of being mis-killed by rules 1-3. The same
/// dispatch point opens pipeline form in batch 2 (no validator rework).
///
/// Rules (non-pipeline form):
/// 1. streaming step must be in the last topological layer — checked
///    explicitly below: "output not referenced" does NOT imply it (Kahn
///    layering puts a no-dependency sink in layer 0 even when later layers
///    exist), and accepting such a DAG would truncate execution at the
///    tail's layer, silently never running the steps in later layers.
/// 3. at most one streaming step per DAG.
/// 4. output-step semantics unified (E2 base, batch 0): with `output`
///    omitted the DAG output is `steps.last()`, which MUST be the streaming
///    step — otherwise a streaming DAG would silently produce nothing
///    streamable (B-m4: explicit `output` lands with E2 in batch 3).
///
/// A linear pipeline chain (§4.2): consecutive STREAMING steps where each
/// step's output is consumed whole by the next (P-R1/P-R2), ending at the
/// DAG output step (P-R3/D26). Steps are indices into `EnsemblePlan.steps`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chain {
    pub nodes: Vec<usize>,
}

/// §4.0/D16 pipeline-form validation + chain construction (batch 2 opens the
/// form the batch-0 validator rejected). Rules P-R1..P-R5 + D26:
///   P-R1  a streaming output has exactly ONE consumer (0 → non-pipeline;
///         ≥2 → error)
///   P-R2  streaming outputs are referenced WHOLE only ($s0; $s0.field → error)
///   P-R3  the chain tail must be the DAG output step (and streaming — the
///         unary-tail shape is rejected here, D26's "pipeline chain tail
///         must be a streaming step")
///   P-R4  chains never share steps (Kahn covers cycles; the linear walk
///         below marks every node visited — a shared step fails)
///   P-R5  the DAG's streaming set is exactly one chain OR one tail streaming
///         step — a chain plus an orphan streaming step is an error
///   D26   a chain's unary consumer has no clean chunk→unary→chunk semantics
///         → the whole form is rejected at parse time
pub(crate) fn build_chains(steps: &[EnsembleStep], output_step: usize) -> Result<Vec<Chain>, AppError> {
    let is_stream: Vec<bool> = steps.iter().map(|s| s.stream).collect();
    let stream_count = is_stream.iter().filter(|b| **b).count();
    if stream_count == 0 {
        return Ok(Vec::new());
    }

    // Consumer edges: streaming step index → (consumer index, field-ref?).
    let mut consumers: HashMap<usize, Vec<(usize, bool)>> = HashMap::new();
    for (i, step) in steps.iter().enumerate() {
        for ref_str in step.inputs.values() {
            let Some(caps) = REF_RE.captures(ref_str) else { continue };
            let source = caps.get(1).unwrap().as_str();
            let Some(src_idx) = steps.iter().position(|s| s.name == source) else { continue };
            if is_stream[src_idx] {
                consumers
                    .entry(src_idx)
                    .or_default()
                    .push((i, caps.get(2).is_some()));
            }
        }
    }

    let mut chains: Vec<Chain> = Vec::new();
    let mut visited: HashSet<usize> = HashSet::new();
    for s in 0..steps.len() {
        if !is_stream[s] || visited.contains(&s) {
            continue;
        }
        match consumers.get(&s).map(|v| v.as_slice()) {
            None => {
                // Zero consumers = non-pipeline (tail-streaming) form. An
                // orphan that is NOT the output step breaks the output
                // semantics (§4.1-4 / P-R5).
                if s != output_step {
                    return Err(AppError::Config(format!(
                        "streaming step '{}' has no consumer and is not the DAG \
                         output step — the DAG mixes a pipeline chain with an \
                         orphan streaming step (P-R5)",
                        steps[s].name
                    )));
                }
            }
            Some([(c, field_ref)]) => {
                // Chain start (P-R1: exactly one consumer).
                if *field_ref {
                    return Err(AppError::Config(format!(
                        "streaming step '{}' output must be referenced whole \
                         ($step), not field-projected (P-R2)",
                        steps[s].name
                    )));
                }
                // Walk the consumer chain. Every node must be streaming —
                // a unary consumer is the D26-rejected chunk→unary→chunk form
                // (P-R3's "pipeline chain tail must be a streaming step").
                let mut nodes = vec![s];
                let mut cur = s;
                let mut next_consumer = *c;
                loop {
                    let c = next_consumer;
                    if !is_stream[c] {
                        return Err(AppError::Config(format!(
                            "pipeline chain step '{}' consumes streaming step \
                             '{}' but is not streaming — the pipeline chain \
                             tail must be a streaming step; unary consumers \
                             on a chain are not defined (D26)",
                            steps[c].name, steps[cur].name
                        )));
                    }
                    if visited.contains(&c) {
                        return Err(AppError::Config(format!(
                            "pipeline chains share step '{}' (P-R4)",
                            steps[c].name
                        )));
                    }
                    nodes.push(c);
                    visited.insert(c);
                    match consumers.get(&c).map(|v| v.as_slice()) {
                        None => break, // chain tail
                        Some([(c2, field_ref2)]) => {
                            if *field_ref2 {
                                return Err(AppError::Config(format!(
                                    "streaming step '{}' output must be referenced \
                                     whole ($step), not field-projected (P-R2)",
                                    steps[c].name
                                )));
                            }
                            cur = c;
                            next_consumer = *c2;
                        }
                        Some(_) => {
                            return Err(AppError::Config(format!(
                                "streaming step '{}' output has multiple \
                                 consumers (P-R1)",
                                steps[c].name
                            )));
                        }
                    }
                }
                let tail = *nodes.last().unwrap();
                if tail != output_step {
                    return Err(AppError::Config(format!(
                        "pipeline chain tail '{}' must be the DAG output step \
                         (config last step, or the explicit `output:`) (P-R3)",
                        steps[tail].name
                    )));
                }
                chains.push(Chain { nodes });
            }
            Some(_) => {
                return Err(AppError::Config(format!(
                    "streaming step '{}' output has multiple consumers (P-R1)",
                    steps[s].name
                )));
            }
        }
    }
    Ok(chains)
}

pub(crate) fn validate_stream_rules(steps: &[EnsembleStep], output_step: usize) -> Result<(), AppError> {
    let stream_steps: Vec<&EnsembleStep> = steps.iter().filter(|s| s.stream).collect();
    if stream_steps.is_empty() {
        return Ok(());
    }
    if stream_steps.len() > 1 {
        return Err(AppError::Config(
            "at most one streaming step per ensemble DAG (rule 3)".to_string(),
        ));
    }
    let tail = stream_steps[0];

    // Rule 1: the streaming step must be in the LAST topological layer.
    // "Output not referenced" only proves it has no dependents — Kahn
    // layering puts a no-dependency sink in layer 0 even when later layers
    // exist, so without this check a streaming step in an early layer would
    // truncate execution (run_layers stops at the tail's layer) and steps in
    // later layers would silently never run. Load-time cost only (parse path).
    let layers = topological_layers(steps);
    let in_last_layer = layers
        .last()
        .map(|l| l.iter().any(|s| s.name == tail.name))
        .unwrap_or(false);
    if !in_last_layer {
        return Err(AppError::Config(format!(
            "streaming step '{}' must be in the last topological layer (rule 1); \
             steps in later layers would silently never execute",
            tail.name
        )));
    }

    // Rule 4 (B-m4, E2 batch 3): the DAG output step — config last step or
    // the explicit `output:` — must be the streaming step.
    if steps[output_step].name != tail.name {
        return Err(AppError::Config(format!(
            "streaming step '{}' must be the DAG output step (config last \
             step, or the explicit `output:`), or the DAG output is ambiguous",
            tail.name
        )));
    }

    Ok(())
}

pub(crate) fn topological_layers(steps: &[EnsembleStep]) -> Vec<Vec<&EnsembleStep>> {
    let mut dependencies: HashMap<&str, HashSet<&str>> = HashMap::new();
    for step in steps {
        let deps = dependencies.entry(&step.name).or_default();
        for ref_str in step.inputs.values() {
            if let Some(caps) = REF_RE.captures(ref_str) {
                let source = caps.get(1).unwrap().as_str();
                // MIMO: `$inputs.NAME` is a named-input ref, not a step dep.
                if source != "request" && source != "inputs" {
                    deps.insert(source);
                }
            }
        }
    }

    let step_map: HashMap<&str, &EnsembleStep> = steps.iter()
        .map(|s| (s.name.as_str(), s))
        .collect();

    let mut in_degree: HashMap<&str, usize> = steps.iter()
        .map(|s| (s.name.as_str(), 0))
        .collect();
    for (step_name, deps) in &dependencies {
        for _dep in deps {
            *in_degree.get_mut(*step_name).unwrap() += 1;
        }
    }

    let mut layers: Vec<Vec<&EnsembleStep>> = Vec::new();
    let mut remaining: HashSet<&str> = steps.iter().map(|s| s.name.as_str()).collect();

    while !remaining.is_empty() {
        let layer: Vec<&EnsembleStep> = remaining.iter()
            .filter(|n| in_degree.get(**n).copied().unwrap_or(0) == 0)
            .map(|n| *step_map.get(n).unwrap())
            .collect();

        if layer.is_empty() {
            break; // Should not happen if validated
        }

        for step in &layer {
            remaining.remove(step.name.as_str());
            for other in &remaining {
                if dependencies.get(*other).map(|d| d.contains(step.name.as_str())).unwrap_or(false) {
                    *in_degree.get_mut(*other).unwrap() -= 1;
                }
            }
        }

        layers.push(layer);
    }

    layers
}

/// E2 (batch 3): apply the explicit `output: "$stepN.field"` projection to
/// the DAG output value. None = whole value; Some = field extraction on a
/// Json output (Binary has no field semantics — D7's rule on the output
/// face). A missing field means the DAG's contract does not match the
/// model's output shape.
pub(crate) fn select_output_field(
    step_name: &str,
    value: EnsembleValue,
    field: Option<&str>,
) -> Result<EnsembleValue, AppError> {
    let Some(field) = field else {
        return Ok(value);
    };
    match value {
        EnsembleValue::Json(v) => {
            let field_val = v.get(field).cloned().ok_or_else(|| {
                AppError::Config(format!(
                    "ensemble.output field '{}' not found in step '{}' output",
                    field, step_name
                ))
            })?;
            Ok(EnsembleValue::Json(field_val))
        }
        // P2 (batch 6): a raw-resident output with a field projection parses
        // once (the explicit output field forces the parse at plan time too —
        // this arm is the lazy-cache backstop).
        EnsembleValue::RawJson(raw) => {
            let v = raw.parse().map_err(|e| {
                AppError::Internal(format!(
                    "ensemble step '{}' output is not valid JSON: {}",
                    step_name, e
                ))
            })?;
            let field_val = v.get(field).cloned().ok_or_else(|| {
                AppError::Config(format!(
                    "ensemble.output field '{}' not found in step '{}' output",
                    field, step_name
                ))
            })?;
            Ok(EnsembleValue::Json(field_val))
        }
        EnsembleValue::Binary(..) => Err(AppError::InvalidRequestBody(format!(
            "ensemble.output field '{}' cannot be extracted from binary step \
             output '{}' (no field semantics on bytes)",
            field, step_name
        ))),
        EnsembleValue::Envelope { .. } => unreachable!("envelope never reaches output selection"),
    }
}

