use {
    gluesql_core::prelude::Payload,
    serde_json,
};

#[derive(Debug, Clone)]
pub struct JavaPayload {
    pub payload: Payload,
}

pub fn convert_payload(payloads: Vec<JavaPayload>) -> Result<String, serde_json::Error> {
    let mut results = Vec::new();
    
    for java_payload in payloads {
        match java_payload.payload {
            Payload::Create => {
                results.push(serde_json::json!({
                    "type": "Create",
                    "result": "Success"
                }));
            }
            Payload::Insert(rows) => {
                results.push(serde_json::json!({
                    "type": "Insert",
                    "inserted_rows": rows
                }));
            }
            Payload::Update(rows) => {
                results.push(serde_json::json!({
                    "type": "Update", 
                    "updated_rows": rows
                }));
            }
            Payload::Delete(rows) => {
                results.push(serde_json::json!({
                    "type": "Delete",
                    "deleted_rows": rows
                }));
            }
            Payload::Select { labels, rows } => {
                let rows = rows
                    .into_iter()
                    .map(|values| {
                        let row = labels
                            .iter()
                            .zip(values)
                            .map(|(label, value)| {
                                let key = label.to_owned();
                                let value = serde_json::Value::try_from(value).unwrap();
                                (key, value)
                            })
                            .collect();
                        serde_json::Value::Object(row)
                    })
                    .collect();

                results.push(serde_json::json!({
                    "type": "Select",
                    "labels": labels,
                    "rows": serde_json::Value::Array(rows)
                }));
            }
            Payload::DropTable(count) => {
                results.push(serde_json::json!({
                    "type": "Drop",
                    "dropped_tables": count
                }));
            }
            Payload::AlterTable => {
                results.push(serde_json::json!({
                    "type": "AlterTable",
                    "result": "Success"
                }));
            }
            Payload::CreateIndex => {
                results.push(serde_json::json!({
                    "type": "CreateIndex",
                    "result": "Success"
                }));
            }
            Payload::DropIndex => {
                results.push(serde_json::json!({
                    "type": "DropIndex", 
                    "result": "Success"
                }));
            }
            Payload::ShowColumns(columns) => {
                results.push(serde_json::json!({
                    "type": "ShowColumns",
                    "columns": columns
                }));
            }
            Payload::SelectMap(rows) => {
                results.push(serde_json::json!({
                    "type": "SelectMap",
                    "rows": rows
                }));
            }
            Payload::DropFunction => {
                results.push(serde_json::json!({
                    "type": "DropFunction",
                    "result": "Success"
                }));
            }
            Payload::ShowVariable(variable) => {
                results.push(serde_json::json!({
                    "type": "ShowVariable",
                    "variable": variable
                }));
            }
            Payload::StartTransaction => {
                results.push(serde_json::json!({
                    "type": "StartTransaction",
                    "result": "Success"
                }));
            }
            Payload::Commit => {
                results.push(serde_json::json!({
                    "type": "Commit",
                    "result": "Success"
                }));
            }
            Payload::Rollback => {
                results.push(serde_json::json!({
                    "type": "Rollback",
                    "result": "Success"
                }));
            }
        }
    }

    serde_json::to_string_pretty(&results)
}