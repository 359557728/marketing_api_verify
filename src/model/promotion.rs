use ::serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct Promotion {
    pub project_id: Option<u64>,
    pub advertiser_id: Option<u64>,
    pub promotion_id: Option<u64>,
    pub promotion_name: Option<String>,
    pub promotion_create_time: Option<String>,
    pub promotion_modify_time: Option<String>,
    pub status: Option<String>,
    pub status_first: Option<String>,
    pub status_second: Option<Value>,
    pub opt_status: Option<String>,
    pub promotion_materials: Option<Value>,
    pub source: Option<String>,
    pub budget: Option<Value>,
    pub budget_mode: Option<String>,
    pub cpa_bid: Option<Value>,
    pub deep_cpabid: Option<Value>,
    pub roi_goal: Option<Value>,
    pub schedule_time: Option<String>,
    pub native_setting: Option<Value>,
    pub bid: Option<Value>,
    pub creative_auto_generate_switch: Option<String>,
    pub config_id: Option<Value>,
    pub learning_phase: Option<String>,
    pub is_comment_disable: Option<String>,
    pub ad_download_status: Option<String>,
    pub brand_info: Option<Value>,
    pub materials_type: Option<String>,
}
