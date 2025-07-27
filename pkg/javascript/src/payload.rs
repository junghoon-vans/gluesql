#![cfg(target_arch = "wasm32")]

use {
    gloo_utils::format::JsValueSerdeExt,
    gluesql_core::prelude::{Payload, PayloadVariable},
    serde_json::{Value as Json, json},
    wasm_bindgen::prelude::JsValue,
};

pub fn convert(payloads: Vec<Payload>) -> JsValue {
    let payloads = payloads.into_iter().map(|var| var.as_json()).collect();
    let payloads = Json::Array(payloads);

    JsValue::from_serde(&payloads).unwrap()
}
