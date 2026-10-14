use ::serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct Project {
    pub project_id: Option<u64>,
    pub advertiser_id: Option<u64>,
    pub delivery_mode: Option<String>,
    pub landing_type: Option<String>,
    pub app_promotion_type: Option<String>,
    pub marketing_goal: Option<String>,
    pub ad_type: Option<String>,
    pub opt_status: Option<String>,
    pub status: Option<String>,
    pub status_first: Option<String>,
    pub status_second: Option<serde_json::Value>,
    pub name: Option<String>,
    pub project_create_time: Option<String>,
    pub project_modify_time: Option<String>,
    pub pricing: Option<String>,
    pub package_name: Option<String>,
    pub app_name: Option<String>,
    pub related_product: Option<serde_json::Value>,
    pub asset_type: Option<String>,
    pub download_url: Option<String>,
    pub download_type: Option<String>,
    pub download_mode: Option<String>,
    pub launch_type: Option<String>,
    pub open_url: Option<String>,
    pub ulink_url: Option<String>,
    pub subscribe_url: Option<String>,
    pub optimize_goal: Option<serde_json::Value>,
    pub delivery_range: Option<serde_json::Value>,
    pub audience: Option<serde_json::Value>,
    pub delivery_setting: Option<serde_json::Value>,
    pub track_url_setting: Option<serde_json::Value>,
    pub audience_extend: Option<String>,
}

impl Project {
    pub fn external_action(&self) -> Option<String> {
        get_str_from_json(&self.optimize_goal, "external_action")
    }

    pub fn deep_external_action(&self) -> Option<String> {
        get_str_from_json(&self.optimize_goal, "deep_external_action")
    }

    pub fn bid(&self) -> Option<Value> {
        get_value_from_json(&self.delivery_setting, "bid")
    }

    pub fn cpa_bid(&self) -> Option<Value> {
        get_value_from_json(&self.delivery_setting, "cpa_bid")
    }

    pub fn roi_goal(&self) -> Option<Value> {
        get_value_from_json(&self.delivery_setting, "roi_goal")
    }

    pub fn deep_cpabid(&self) -> Option<Value> {
        get_value_from_json(&self.delivery_setting, "deep_cpabid")
    }

    pub fn audience_package_id(&self) -> Option<Value> {
        get_value_from_json(&self.audience, "audience_package_id")
    }
}

fn get_value_from_json(json: &Option<serde_json::Value>, key: &str) -> Option<Value> {
    let v = match json {
        Some(it) if it[key].is_number() => it[key].clone(),
        _ => Value::Null,
    };
    if let Value::Number(it) = v {
        Some(Value::Number(it))
    } else {
        None
    }
}

fn get_str_from_json(json: &Option<serde_json::Value>, key: &str) -> Option<String> {
    let v = match json {
        Some(it) => it[key].clone(),
        _ => Value::Null,
    };
    if let Value::String(it) = v {
        Some(it)
    } else {
        None
    }
}
