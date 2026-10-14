use ::serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct GdtTarget {
    pub targeting_id: u64,
    pub targeting_name: String,
    pub targeting: Option<serde_json::Value>,
    pub description: Option<String>,
    pub is_deleted: Option<bool>,
    pub created_time: Option<u64>,
    pub last_modified_time: Option<u64>,
    pub targeting_translation: Option<String>,
    pub targeting_source_type: Option<String>,
    pub share_from_account_id: Option<u64>,
    pub share_from_targeting_id: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GdtAudience {
    pub audience_id: u64,
    pub account_id: Option<u64>,
    pub name: Option<String>,
    pub external_audience_id: Option<String>,
    pub description: Option<String>,
    pub cooperated: Option<bool>,
    #[serde(rename = "type")]
    pub audience_type: Option<String>,
    pub source: Option<String>,
    pub status: Option<String>,
    pub error_code: Option<u64>,
    pub user_count: Option<u64>,
    pub created_time: Option<String>,
    pub last_modified_time: Option<String>,
    pub audience_spec: Option<serde_json::Value>,
}
