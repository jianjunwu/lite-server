use crate::config::ModelConfig;
use crate::registry::ModelRegistry;
use crate::registry::types::*;
use crate::streaming::StreamingEngine;
use crate::validation;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Runtime;

// ---------------------------------------------------------------------------
// validate_identifier
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn validate_identifier(s: &str) -> PyResult<()> {
    validation::validate_identifier(s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

// ---------------------------------------------------------------------------
// PyModelRegistry — wraps the async ModelRegistry with a tokio runtime
// ---------------------------------------------------------------------------

#[pyclass(name = "ModelRegistry")]
pub struct PyModelRegistry {
    inner: Arc<ModelRegistry>,
    rt: Runtime,
}

#[pymethods]
impl PyModelRegistry {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(ModelRegistry::new()),
            rt: Runtime::new()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?,
        })
    }

    /// register(name, version, config_dict, model_type, model_dir)
    /// model_type: "lit_api" | "ensemble"
    fn register(
        &self,
        name: &str,
        version: &str,
        config: &Bound<'_, PyDict>,
        model_type: &str,
        model_dir: &str,
    ) -> PyResult<()> {
        let config_json: serde_json::Value = python_dict_to_json(config)?;
        let mc: ModelConfig = serde_json::from_value(config_json)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let mt = match model_type {
            "lit_api" => ModelType::LitAPI,
            "ensemble" => ModelType::Ensemble,
            _ => return Err(pyo3::exceptions::PyValueError::new_err(
                format!("unknown model_type: {}", model_type),
            )),
        };
        let reg = self.inner.clone();
        self.rt.block_on(async {
            reg.register(name, version, mc, mt, PathBuf::from(model_dir)).await
        }).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// get(name, version=None) -> dict | None
    #[pyo3(signature = (name, version=None))]
    fn get(&self, name: &str, version: Option<&str>) -> PyResult<Option<PyObject>> {
        let reg = self.inner.clone();
        let mv = self.rt.block_on(async {
            reg.get(name, version).await
        });
        match mv {
            Some(mv) => Python::with_gil(|py| {
                let dict = PyDict::new(py);
                dict.set_item("version", &mv.version)?;
                dict.set_item("status", format!("{:?}", mv.status))?;
                dict.set_item("model_type", format!("{:?}", mv.model_type))?;
                dict.set_item("model_dir", mv.model_dir.to_string_lossy().to_string())?;
                dict.set_item("workers_count", mv.workers.len())?;
                Ok(Some(dict.into_any().unbind()))
            }),
            None => Ok(None),
        }
    }

    /// set_status(name, version, status)
    /// status: "Loading" | "Ready" | "Unloading" | "Error"
    fn set_status(&self, name: &str, version: &str, status: &str) -> PyResult<()> {
        let vs = parse_status(status)?;
        let reg = self.inner.clone();
        self.rt.block_on(async {
            reg.set_status(name, version, vs).await
        }).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// is_ready(name, version) -> bool
    fn is_ready(&self, name: &str, version: &str) -> PyResult<bool> {
        let reg = self.inner.clone();
        Ok(self.rt.block_on(async {
            reg.is_ready(name, Some(version)).await
        }))
    }

    /// activate_version(name, version) -> bool
    fn activate_version(&self, name: &str, version: &str) -> PyResult<bool> {
        let reg = self.inner.clone();
        self.rt.block_on(async {
            reg.activate_version(name, version).await
        }).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// deactivate(name)
    fn deactivate(&self, name: &str) -> PyResult<()> {
        let reg = self.inner.clone();
        self.rt.block_on(async { reg.deactivate(name).await });
        Ok(())
    }

    /// get_active_version(name) -> str | None
    fn get_active_version(&self, name: &str) -> PyResult<Option<String>> {
        let reg = self.inner.clone();
        Ok(self.rt.block_on(async {
            reg.get_active_version(name).await
        }))
    }

    /// list_loaded() -> list[dict]
    fn list_loaded(&self) -> PyResult<Vec<PyObject>> {
        let reg = self.inner.clone();
        let loaded = self.rt.block_on(async { reg.list_loaded().await });
        Python::with_gil(|py| {
            let mut result = Vec::with_capacity(loaded.len());
            for (name, version, mv) in loaded {
                let dict = PyDict::new(py);
                dict.set_item("name", &name)?;
                dict.set_item("version", &version)?;
                dict.set_item("status", format!("{:?}", mv.status))?;
                result.push(dict.into_any().unbind());
            }
            Ok(result)
        })
    }

    /// list_versions(name) -> list[dict]
    fn list_versions(&self, name: &str) -> PyResult<Vec<PyObject>> {
        let reg = self.inner.clone();
        let versions = self.rt.block_on(async { reg.list_versions(name).await });
        Python::with_gil(|py| {
            let mut result = Vec::with_capacity(versions.len());
            for mv in versions {
                let dict = PyDict::new(py);
                dict.set_item("version", &mv.version)?;
                dict.set_item("status", format!("{:?}", mv.status))?;
                result.push(dict.into_any().unbind());
            }
            Ok(result)
        })
    }

    /// remove(name, version)
    fn remove(&self, name: &str, version: &str) -> PyResult<()> {
        let reg = self.inner.clone();
        self.rt.block_on(async {
            reg.remove(name, version).await
        }).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// PyStreamingEngine — wraps the synchronous StreamingEngine
// ---------------------------------------------------------------------------

#[pyclass(name = "StreamingEngine")]
pub struct PyStreamingEngine {
    inner: StreamingEngine,
}

#[pymethods]
impl PyStreamingEngine {
    #[new]
    fn new() -> Self {
        Self {
            inner: StreamingEngine::new(),
        }
    }

    /// register_stream(stream_id) — registers a stream
    fn register_stream(&self, stream_id: &str) {
        // Drop the handle immediately — tests only need has_stream / cancel
        let _handle = self.inner.register_stream(stream_id.to_string());
    }

    /// has_stream(stream_id) -> bool
    fn has_stream(&self, stream_id: &str) -> bool {
        self.inner.has_stream(stream_id)
    }

    /// cancel_stream(stream_id)
    fn cancel_stream(&self, stream_id: &str) {
        self.inner.cancel_stream(stream_id);
    }

    /// finish_stream(stream_id)
    fn finish_stream(&self, stream_id: &str) {
        self.inner.finish_stream(stream_id);
    }

    /// get_sender(stream_id) -> bool (whether sender exists)
    fn has_sender(&self, stream_id: &str) -> bool {
        self.inner.get_sender(stream_id).is_some()
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn python_dict_to_json(dict: &Bound<'_, PyDict>) -> PyResult<serde_json::Value> {
    let py = dict.py();
    let json_module = py.import("json")?;
    let dumped = json_module.call_method1("dumps", (dict,))?;
    let s: String = dumped.extract()?;
    serde_json::from_str(&s)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

fn parse_status(s: &str) -> PyResult<VersionStatus> {
    match s {
        "Loading" => Ok(VersionStatus::Loading),
        "Ready" => Ok(VersionStatus::Ready),
        "Unloading" => Ok(VersionStatus::Unloading),
        "Error" => Ok(VersionStatus::Error),
        _ => Err(pyo3::exceptions::PyValueError::new_err(
            format!("unknown status: {}; expected Loading|Ready|Unloading|Error", s),
        )),
    }
}
