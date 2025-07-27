use {
    gluesql_core::prelude::{Payload, PayloadVariable},
    pyo3::{PyObject, Python, pyclass},
    pythonize::pythonize,
};

#[pyclass]
pub struct PyPayload {
    pub payload: Payload,
}

pub fn convert(py: Python, payloads: Vec<PyPayload>) -> PyObject {
    let payloads = payloads
        .into_iter()
        .map(|var| var.payload.as_json())
        .collect();
    let payloads = Json::Array(payloads);

    pythonize(py, &payloads).unwrap()
}
