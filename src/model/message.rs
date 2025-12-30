use ::serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Verify {
    pub id: String,
    pub cate: String,
    pub media_id: u8,
    pub account_id: u64,
    pub url: String,
    pub body: Option<Value>,
}
