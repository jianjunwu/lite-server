use crate::config::ModelConfig;
use crate::registry::ModelRegistry;
use crate::registry::types::*;
use crate::validation;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::path::PathBuf;
use std::sync::Arc;

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
}

#[pymethods]
impl PyModelRegistry {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(ModelRegistry::new()),
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
        self.inner
            .register(name, version, mc, mt, PathBuf::from(model_dir))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// get(name, version=None) -> dict | None
    #[pyo3(signature = (name, version=None))]
    fn get(&self, name: &str, version: Option<&str>) -> PyResult<Option<PyObject>> {
        let mv = self.inner.get(name, version);
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
        self.inner
            .set_status(name, version, vs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// is_ready(name, version) -> bool
    fn is_ready(&self, name: &str, version: &str) -> PyResult<bool> {
        Ok(self.inner.is_ready(name, Some(version)))
    }

    /// activate_version(name, version) -> bool
    fn activate_version(&self, name: &str, version: &str) -> PyResult<bool> {
        self.inner
            .activate_version(name, version)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// deactivate(name)
    fn deactivate(&self, name: &str) -> PyResult<()> {
        self.inner.deactivate(name);
        Ok(())
    }

    /// get_active_version(name) -> str | None
    fn get_active_version(&self, name: &str) -> PyResult<Option<String>> {
        Ok(self.inner.get_active_version(name))
    }

    /// list_loaded() -> list[dict]
    fn list_loaded(&self) -> PyResult<Vec<PyObject>> {
        let loaded = self.inner.list_loaded();
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
        let versions = self.inner.list_versions(name);
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
        self.inner
            .remove(name, version)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
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
