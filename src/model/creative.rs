use ::serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct DynamicCreative {
    pub adgroup_id: Option<u64>,
    pub dynamic_creative_id: Option<u64>,
    pub dynamic_creative_name: Option<String>,
    pub creative_template_id: Option<u64>,
    pub delivery_mode: Option<String>,
    pub dynamic_creative_type: Option<String>,
    pub creative_components: Option<Value>,
    pub created_time: Option<u32>,
    pub last_modified_time: Option<u32>,
    pub is_deleted: Option<bool>,
    pub configured_status: Option<String>,
    pub impression_tracking_url: Option<String>,
    pub click_tracking_url: Option<String>,
    pub page_track_url: Option<String>,
}

impl DynamicCreative {
    pub fn wechat_mini_program_page_type(&self) -> Option<String> {
        let v = match &self.creative_components {
            Some(it) => it["main_jump_info"][0]["value"]["page_type"].clone(),
            _ => Value::Null,
        };
        if let Value::String(it) = v {
            Some(it)
        } else {
            None
        }
    }

    pub fn wechat_mini_program_spec(&self) -> Option<Value> {
        let v = match &self.creative_components {
            Some(it) => {
                it["main_jump_info"][0]["value"]["page_spec"]["wechat_mini_program_spec"].clone()
            }
            _ => Value::Null,
        };
        if v.is_null() { None } else { Some(v) }
    }
}
