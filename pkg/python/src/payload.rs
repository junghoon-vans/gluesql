use {
    gluesql_core::prelude::{Payload, PayloadVariable},
    pyo3::{PyObject, Python, pyclass},
    pythonize::pythonize,
    serde_json::{Value as Json, json},
};

#[pyclass]
pub struct PyPayload {
    pub payload: Payload,
}

pub fn convert(py: Python, payloads: Vec<PyPayload>) -> PyObject {
    let payloads = payloads
        .into_iter()
        .map(|var| convert_payload(var.payload))
        .collect();
    let payloads = Json::Array(payloads);

    pythonize(py, &payloads).unwrap()
}

fn convert_payload(payload: Payload) -> Json {
    // TODO: Improve below logic.
    let type_str = payload.as_str();

    match payload {
        Payload::Create
        | Payload::AlterTable
        | Payload::CreateIndex
        | Payload::DropIndex
        | Payload::StartTransaction
        | Payload::Commit
        | Payload::Rollback
        | Payload::DropFunction => json!({ "type": $type_str }),

        Payload::DropTable(num)
        | Payload::Insert(num)
        | Payload::Update(num)
        | Payload::Delete(num) => json!({
            "type": $type_str,
            "affected": num
        }),

        Payload::Select { labels, rows } => {
            let rows = rows
                .into_iter()
                .map(|values| {
                    let row = labels
                        .iter()
                        .zip(values)
                        .map(|(label, value)| {
                            let key = label.to_owned();
                            let value = Json::try_from(value).unwrap();

                            (key, value)
                        })
                        .collect();

                    Json::Object(row)
                })
                .collect();

            json!({
                "type": $type_str,
                "rows": Json::Array(rows),
            })
        }
        Payload::SelectMap(rows) => {
            let rows = rows
                .into_iter()
                .map(|row| {
                    let row = row
                        .into_iter()
                        .map(|(key, value)| {
                            let value = Json::try_from(value).unwrap();

                            (key, value)
                        })
                        .collect();

                    Json::Object(row)
                })
                .collect();

            json!({
                "type": $type_str,
                "rows": Json::Array(rows),
            })
        }
        Payload::ShowColumns(columns) => {
            let columns = columns
                .into_iter()
                .map(|(name, data_type)| {
                    json!({
                        "name": name,
                        "type": data_type.to_string(),
                    })
                })
                .collect();

            json!({
                "type": $type_str,
                "columns": Json::Array(columns),
            })
        }
        Payload::ShowVariable(PayloadVariable::Version(version)) => {
            json!({
                "type": $type_str,
                "version": version
            })
        }
        Payload::ShowVariable(PayloadVariable::Tables(table_names)) => {
            json!({
                "type": $type_str,
                "tables": table_names
            })
        }
        Payload::ShowVariable(PayloadVariable::Functions(function_names)) => {
            json!({
                "type": $type_str,
                "functions": function_names
            })
        }
    }
}
