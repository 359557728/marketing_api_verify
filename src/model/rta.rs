use ::serde::{Deserialize, Serialize};
use serde_json::Value;

#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TtRtaInfo {
    pub interface_info: Option<Value>,
    pub rta_info: Option<Value>,
}
