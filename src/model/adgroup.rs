use ::serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AdGroup {
    pub adgroup_id: Option<u64>,
    pub adgroup_name: Option<String>,
    pub site_set: Option<Value>,
    pub automatic_site_enabled: Option<bool>,
    pub optimization_goal: Option<String>,
    pub bid_amount: Option<u64>,
    pub daily_budget: Option<u64>,
    pub targeting: Option<Value>,
    pub targeting_translation: Option<String>,
    pub scene_spec: Option<Value>,
    pub begin_date: Option<String>,
    pub first_day_begin_time: Option<String>,
    pub end_date: Option<String>,
    pub time_series: Option<String>,
    pub configured_status: Option<String>,
    pub created_time: Option<u64>,
    pub last_modified_time: Option<u64>,
    pub user_action_sets: Option<Value>,
    pub is_deleted: Option<bool>,
    pub deep_conversion_spec: Option<Value>,
    pub poi_list: Option<Value>,
    pub conversion_id: Option<u64>,
    pub deep_conversion_behavior_bid: Option<u64>,
    pub deep_conversion_worth_rate: Option<f64>,
    pub system_status: Option<String>,
    pub bid_mode: Option<String>,
    pub auto_acquisition_enabled: Option<bool>,
    pub auto_acquisition_budget: Option<u64>,
    pub auto_derived_creative_enabled: Option<bool>,
    pub smart_bid_type: Option<String>,
    pub smart_cost_cap: Option<u64>,
    pub marketing_scene: Option<String>,
    pub marketing_goal: Option<String>,
    pub marketing_sub_goal: Option<String>,
    pub marketing_carrier_type: Option<String>,
    pub marketing_carrier_detail: Option<Value>,
    pub marketing_target_type: Option<String>,
    pub marketing_target_detail: Option<Value>,
    pub marketing_target_id: Option<u64>,
    pub bid_strategy: Option<String>,
    pub deep_conversion_worth_advanced_rate: Option<Value>,
    pub deep_conversion_behavior_advanced_bid: Option<u64>,
    pub search_expand_targeting_switch: Option<String>,
    pub auto_derived_landing_page_switch: Option<bool>,
    pub data_model_version: Option<u64>,
    pub bid_scene: Option<String>,
    pub marketing_target_ext: Option<Value>,
    pub deep_optimization_type: Option<String>,
    pub flow_optimization_enabled: Option<bool>,
    pub marketing_target_attachment: Option<Value>,
    pub negative_word_cnt: Option<Value>,
    pub search_expansion_switch: Option<String>,
    pub marketing_asset_id: Option<u64>,
    pub promoted_asset_type: Option<String>,
    pub material_package_id: Option<u64>,
    pub marketing_asset_outer_spec: Option<Value>,
    pub exploration_strategy: Option<String>,
    pub priority_site_set: Option<Value>,
    pub ecom_pkam_switch: Option<String>,
    pub forward_link_assist: Option<String>,
    pub conversion_name: Option<String>,
    pub auto_acquisition_status: Option<String>,
    pub cost_constraint_scene: Option<String>,
    pub custom_cost_cap: Option<u64>,
    pub mpa_spec: Option<Value>,
    pub smart_delivery_platform: Option<String>,
    pub smart_delivery_scene_spec: Option<Value>,
    pub project_ability_list: Option<Value>,
    pub smart_targeting_status: Option<String>,
}

impl AdGroup {
    pub fn configured_status_bit(&self) -> u8 {
        return match self.configured_status {
            Some(ref compare) if "AD_STATUS_NORMAL" == compare => 1,
            _ => 0,
        };
    }

    pub fn deep_conversion_behavior_goal(&self) -> Option<String> {
        let v = match &self.deep_conversion_spec {
            Some(it) => it["deep_conversion_behavior_spec"]["goal"].clone(),
            _ => Value::Null,
        };
        if let Value::String(it) = v {
            Some(it)
        } else {
            None
        }
    }

    pub fn deep_conversion_worth_goal(&self) -> Option<String> {
        let v = match &self.deep_conversion_spec {
            Some(it) => it["deep_conversion_worth_spec"]["goal"].clone(),
            _ => Value::Null,
        };
        if let Value::String(it) = v {
            Some(it)
        } else {
            None
        }
    }

    pub fn deep_conversion_worth_advanced_goal(&self) -> Option<String> {
        let v = match &self.deep_conversion_spec {
            Some(it) => it["deep_conversion_worth_advanced_spec"]["goal"].clone(),
            _ => Value::Null,
        };
        if let Value::String(it) = v {
            Some(it)
        } else {
            None
        }
    }

    pub fn deep_conversion_behavior_advanced_goal(&self) -> Option<String> {
        let v = match &self.deep_conversion_spec {
            Some(it) => it["deep_conversion_behavior_advanced_spec"]["goal"].clone(),
            _ => Value::Null,
        };
        if let Value::String(it) = v {
            Some(it)
        } else {
            None
        }
    }

    pub fn deep_conversion_worth_expected_roi(&self) -> Option<f64> {
        let v = match &self.deep_conversion_spec {
            Some(it) => it["deep_conversion_worth_spec"]["expected_roi"].clone(),
            _ => Value::Null,
        };
        if let Value::Number(roi) = v {
            roi.as_f64()
        } else {
            None
        }
    }

    pub fn deep_conversion_worth_advanced_expected_roi(&self) -> Option<f64> {
        let v = match &self.deep_conversion_spec {
            Some(it) => it["deep_conversion_worth_advanced_spec"]["expected_roi"].clone(),
            _ => Value::Null,
        };
        if let Value::Number(roi) = v {
            roi.as_f64()
        } else {
            None
        }
    }
}
