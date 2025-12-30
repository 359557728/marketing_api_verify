use std::collections::HashMap;
use std::sync::Arc;

use crate::model::adgroup::AdGroup;
use crate::model::advertiser::GdtAdvertiser;
use crate::model::balance::GdtBalance;
use crate::model::base::{ApiData, ApiRes};
use crate::model::creative::DynamicCreative;
use crate::model::error::{Error, Result};
use crate::model::message::Verify;
use crate::model::report as rt;
use crate::model::target::{GdtAudience, GdtTarget};
use crate::share::common::{
    CORE_POOL, CREATIVE_HOURLY_REPORT_FILED_TL_REQUEST_V3, EMPTY,
    GDT_ACCOUNT_DAILY_SQL_TL_REPORTING_V3, GDT_ACCOUNT_FIELD,
    GDT_ACCOUNT_HOURLY_REPORT_FILED_TL_REPORTING_V3, GDT_ACCOUNT_HOURLY_SQL_TL_REPORTING_V3,
    GDT_ACCOUNT_REPORT_DAILY_GROUPBY_V3, GDT_ACCOUNT_REPORT_HOURLY_GROUPBY_V3,
    GDT_ADGROUP_DAILY_SQL_TL_REQUEST_V3, GDT_ADGROUP_FILED_V3,
    GDT_ADGROUP_HOURLY_REPORT_FILED_TL_REQUEST_V3, GDT_ADGROUP_REPORT_DAILY_GROUPBY_V3,
    GDT_DYNAMIC_CREATIVE_DAILY_SQL_TL_REQUEST_V3, GDT_DYNAMIC_CREATIVE_FILED_V3,
    GDT_DYNAMIC_CREATIVE_REPORT_DAILY_GROUPBY_V3, GDT_DYNAMIC_CREATIVE_SQL_V3, HTTP_CLIENT,
    Limiter, TIDB_POOL, account_token, gdt_params, gdt_wait, rate_limiter, tasks_handle,
    until_ready, verify_rt,
};
use log::info;
use mysql::prelude::Queryable;
use mysql::*;
use retry_macro::retry;

pub async fn sync((cate, items): (String, Vec<Verify>)) -> Result<()> {
    match cate.as_str() {
        "adgroup_v3" => {
            adgroup_sync(items).await;
        }
        "dynamic_creative_v3" => {
            creative_sync(items).await;
        }
        "adgroup_daily_request_part_v3" => {
            report_sync(&cate, items).await;
        }
        "advertiser_reporting_part_v3" => {
            report_sync(&cate, items).await;
        }
        "dynamic_creative_daily_request_part_v3" => {
            report_sync(&cate, items).await;
        }
        "advertiser_daily_reporting_part_v3" => {
            report_sync(&cate, items).await;
        }
        "advertiser" => {
            account_sync(items).await;
        }
        "target" => {
            target_sync(items).await;
        }
        "audience" => {
            audience_sync(items).await;
        }
        "balance" => {
            balance_sync(items).await;
        }
        _ => {
            info!("Unknown Category: {}", cate);
        }
    }
    Ok(())
}

#[retry]
async fn adgroup_api(verify: Verify, limiter: &Limiter) -> Result<()> {
    let url = &verify.url;
    let account_id = verify.account_id;
    let media_id = verify.media_id;
    let token = account_token(account_id, media_id).await;
    if let Some(mut token) = token {
        let mut params = gdt_params(&token);
        params.insert("page_size", String::from("100"));
        params.insert("fields", GDT_ADGROUP_FILED_V3.to_string());
        until_ready(limiter).await;
        let res = HTTP_CLIENT.get(url).query(&params).send().await?;
        let res_parsed = res.json::<ApiRes<ApiData<AdGroup>>>().await?;
        match res_parsed {
            ApiRes {
                code: 0,
                data: Some(ApiData {
                    list: Some(items), ..
                }),
                ..
            } => {
                if !items.is_empty() {
                    let mut con = TIDB_POOL.get_conn()?;
                    con.exec_batch("INSERT INTO `synads_gdt_adgroup_v3` (`account_id`, `targeting`, `adgroup_id`, `targeting_translation`, `configured_status`, `created_time`, `last_modified_time`, `is_deleted`, `system_status`, `adgroup_name`, `marketing_goal`, `marketing_sub_goal`, `marketing_carrier_type`, `marketing_carrier_detail`, `marketing_target_type`, `marketing_target_detail`, `marketing_target_id`, `begin_date`, `end_date`, `first_day_begin_time`, `bid_amount`, `optimization_goal`, `time_series`, `automatic_site_enabled`, `site_set`, `daily_budget`, `scene_spec`, `user_action_sets`, `bid_strategy`, `deep_conversion_spec`, `conversion_id`, `deep_conversion_behavior_bid`, `deep_conversion_worth_rate`, `deep_conversion_worth_advanced_rate`, `deep_conversion_behavior_advanced_bid`, `bid_mode`, `auto_acquisition_enabled`, `auto_acquisition_budget`, `smart_bid_type`, `smart_cost_cap`, `auto_derived_creative_enabled`, `search_expand_targeting_switch`, `auto_derived_landing_page_switch`, `data_model_version`, `bid_scene`, `marketing_target_ext`, `deep_optimization_type`, `flow_optimization_enabled`, `marketing_target_attachment`, `negative_word_cnt`, `search_expansion_switch`, `marketing_asset_id`, `promoted_asset_type`, `material_package_id`, `marketing_asset_outer_spec`, `poi_list`, `marketing_scene`, `exploration_strategy`, `priority_site_set`, `ecom_pkam_switch`, `forward_link_assist`, `conversion_name`, `auto_acquisition_status`, `cost_constraint_scene`, `custom_cost_cap`, `mpa_spec`, `adgroup_create_date`, `adgroup_create_hour`, deep_conversion_behavior_goal, deep_conversion_worth_goal, deep_conversion_worth_advanced_goal, deep_conversion_behavior_advanced_goal, deep_conversion_worth_expected_roi, deep_conversion_worth_advanced_expected_roi, smart_delivery_platform, smart_delivery_scene_spec, project_ability_list, smart_targeting_status) VALUES (:account_id, :targeting, :adgroup_id, :targeting_translation, :configured_status, :created_time, :last_modified_time, :is_deleted, :system_status, :adgroup_name, :marketing_goal, :marketing_sub_goal, :marketing_carrier_type, :marketing_carrier_detail, :marketing_target_type, :marketing_target_detail, :marketing_target_id, :begin_date, :end_date, :first_day_begin_time, :bid_amount, :optimization_goal, :time_series, :automatic_site_enabled, :site_set, :daily_budget, :scene_spec, :user_action_sets, :bid_strategy, :deep_conversion_spec, :conversion_id, :deep_conversion_behavior_bid, :deep_conversion_worth_rate, :deep_conversion_worth_advanced_rate, :deep_conversion_behavior_advanced_bid, :bid_mode, :auto_acquisition_enabled, :auto_acquisition_budget, :smart_bid_type, :smart_cost_cap, :auto_derived_creative_enabled, :search_expand_targeting_switch, :auto_derived_landing_page_switch, :data_model_version, :bid_scene, :marketing_target_ext, :deep_optimization_type, :flow_optimization_enabled, :marketing_target_attachment, :negative_word_cnt, :search_expansion_switch, :marketing_asset_id, :promoted_asset_type, :material_package_id, :marketing_asset_outer_spec, :poi_list, :marketing_scene, :exploration_strategy, :priority_site_set, :ecom_pkam_switch, :forward_link_assist, :conversion_name, :auto_acquisition_status, :cost_constraint_scene, :custom_cost_cap, :mpa_spec, DATE(FROM_UNIXTIME(:created_time)), HOUR(FROM_UNIXTIME(:created_time)), :deep_conversion_behavior_goal, :deep_conversion_worth_goal, :deep_conversion_worth_advanced_goal, :deep_conversion_behavior_advanced_goal, :deep_conversion_worth_expected_roi, :deep_conversion_worth_advanced_expected_roi, :smart_delivery_platform, :smart_delivery_scene_spec, :project_ability_list, :smart_targeting_status) ON DUPLICATE KEY UPDATE `targeting` = :targeting, `targeting_translation` = :targeting_translation, `configured_status` = :configured_status, `created_time` = :created_time, `last_modified_time` = :last_modified_time, `is_deleted` = :is_deleted, `system_status` = :system_status, `adgroup_name` = :adgroup_name, `marketing_goal` = :marketing_goal, `marketing_sub_goal` = :marketing_sub_goal, `marketing_carrier_type` = :marketing_carrier_type, `marketing_carrier_detail` = :marketing_carrier_detail, `marketing_target_type` = :marketing_target_type, `marketing_target_detail` = :marketing_target_detail, `marketing_target_id` = :marketing_target_id, `begin_date` = :begin_date, `end_date` = :end_date, `first_day_begin_time` = :first_day_begin_time, `bid_amount` = :bid_amount, `optimization_goal` = :optimization_goal, `time_series` = :time_series, `automatic_site_enabled` = :automatic_site_enabled, `site_set` = :site_set, `daily_budget` = :daily_budget, `scene_spec` = :scene_spec, `user_action_sets` = :user_action_sets, `bid_strategy` = :bid_strategy, `deep_conversion_spec` = :deep_conversion_spec, `conversion_id` = :conversion_id, `deep_conversion_behavior_bid` = :deep_conversion_behavior_bid, `deep_conversion_worth_rate` = :deep_conversion_worth_rate, `deep_conversion_worth_advanced_rate` = :deep_conversion_worth_advanced_rate, `deep_conversion_behavior_advanced_bid` = :deep_conversion_behavior_advanced_bid, `bid_mode` = :bid_mode, `auto_acquisition_enabled` = :auto_acquisition_enabled, `auto_acquisition_budget` = :auto_acquisition_budget, `smart_bid_type` = :smart_bid_type, `smart_cost_cap` = :smart_cost_cap, `auto_derived_creative_enabled` = :auto_derived_creative_enabled, `search_expand_targeting_switch` = :search_expand_targeting_switch, `auto_derived_landing_page_switch` = :auto_derived_landing_page_switch, `data_model_version` = :data_model_version, `bid_scene` = :bid_scene, `marketing_target_ext` = :marketing_target_ext, `deep_optimization_type` = :deep_optimization_type, `flow_optimization_enabled` = :flow_optimization_enabled, `marketing_target_attachment` = :marketing_target_attachment, `negative_word_cnt` = :negative_word_cnt, `search_expansion_switch` = :search_expansion_switch, `marketing_asset_id` = :marketing_asset_id, `promoted_asset_type` = :promoted_asset_type, `material_package_id` = :material_package_id, `marketing_asset_outer_spec` = :marketing_asset_outer_spec, `poi_list` = :poi_list, `marketing_scene` = :marketing_scene, `exploration_strategy` = :exploration_strategy, `priority_site_set` = :priority_site_set, `ecom_pkam_switch` = :ecom_pkam_switch, `forward_link_assist` = :forward_link_assist, `conversion_name` = :conversion_name, `auto_acquisition_status` = :auto_acquisition_status, `cost_constraint_scene` = :cost_constraint_scene, `custom_cost_cap` = :custom_cost_cap, `mpa_spec` = :mpa_spec, `deep_conversion_behavior_goal` = :deep_conversion_behavior_goal, `deep_conversion_worth_goal` = :deep_conversion_worth_goal, `deep_conversion_worth_advanced_goal` = :deep_conversion_worth_advanced_goal, `deep_conversion_behavior_advanced_goal` = :deep_conversion_behavior_advanced_goal, `deep_conversion_worth_expected_roi` = :deep_conversion_worth_expected_roi, `deep_conversion_worth_advanced_expected_roi` = :deep_conversion_worth_advanced_expected_roi, smart_delivery_platform = :smart_delivery_platform, smart_delivery_scene_spec = :smart_delivery_scene_spec, project_ability_list = :project_ability_list, smart_targeting_status = :smart_targeting_status,`sync_time` = NOW()",
                        items.iter().map(|p| {
                            params! {
                                "adgroup_id" => &p.adgroup_id,
                                "adgroup_name" => &p.adgroup_name,
                                "account_id" => &account_id,
                                "site_set" => &p.site_set,
                                "automatic_site_enabled" => &p.automatic_site_enabled,
                                "optimization_goal" => &p.optimization_goal,
                                "bid_amount" => &p.bid_amount,
                                "daily_budget" => &p.daily_budget,
                                "targeting" => &p.targeting,
                                "targeting_translation" => &p.targeting_translation,
                                "scene_spec" => &p.scene_spec,
                                "begin_date" => &p.begin_date,
                                "first_day_begin_time" => &p.first_day_begin_time,
                                "end_date" => &p.end_date,
                                "time_series" => &p.time_series,
                                "configured_status" => &p.configured_status_bit(),
                                "created_time" => &p.created_time,
                                "last_modified_time" => &p.last_modified_time,
                                "user_action_sets" => &p.user_action_sets,
                                "is_deleted" => &p.is_deleted,
                                "deep_conversion_spec" => &p.deep_conversion_spec,
                                "poi_list" => &p.poi_list,
                                "conversion_id" => &p.conversion_id,
                                "deep_conversion_behavior_bid" => &p.deep_conversion_behavior_bid,
                                "deep_conversion_worth_rate" => &p.deep_conversion_worth_rate,
                                "system_status" => &p.system_status,
                                "bid_mode" => &p.bid_mode,
                                "auto_acquisition_enabled" => &p.auto_acquisition_enabled,
                                "auto_acquisition_budget" => &p.auto_acquisition_budget,
                                "auto_derived_creative_enabled" => &p.auto_derived_creative_enabled,
                                "smart_bid_type" => &p.smart_bid_type,
                                "smart_cost_cap" => &p.smart_cost_cap,
                                "marketing_scene" => &p.marketing_scene,
                                "marketing_goal" => &p.marketing_goal,
                                "marketing_sub_goal" => &p.marketing_sub_goal,
                                "marketing_carrier_type" => &p.marketing_carrier_type,
                                "marketing_carrier_detail" => &p.marketing_carrier_detail,
                                "marketing_target_type" => &p.marketing_target_type,
                                "marketing_target_detail" => &p.marketing_target_detail,
                                "marketing_target_id" => &p.marketing_target_id,
                                "bid_strategy" => &p.bid_strategy,
                                "deep_conversion_worth_advanced_rate" => &p.deep_conversion_worth_advanced_rate,
                                "deep_conversion_behavior_advanced_bid" => &p.deep_conversion_behavior_advanced_bid,
                                "search_expand_targeting_switch" => &p.search_expand_targeting_switch,
                                "auto_derived_landing_page_switch" => &p.auto_derived_landing_page_switch,
                                "data_model_version" => &p.data_model_version,
                                "bid_scene" => &p.bid_scene,
                                "marketing_target_ext" => &p.marketing_target_ext,
                                "deep_optimization_type" => &p.deep_optimization_type,
                                "flow_optimization_enabled" => &p.flow_optimization_enabled,
                                "marketing_target_attachment" => &p.marketing_target_attachment,
                                "negative_word_cnt" => &p.negative_word_cnt,
                                "search_expansion_switch" => &p.search_expansion_switch,
                                "marketing_asset_id" => &p.marketing_asset_id,
                                "promoted_asset_type" => &p.promoted_asset_type,
                                "material_package_id" => &p.material_package_id,
                                "marketing_asset_outer_spec" => &p.marketing_asset_outer_spec,
                                "exploration_strategy" => &p.exploration_strategy,
                                "priority_site_set" => &p.priority_site_set,
                                "ecom_pkam_switch" => &p.ecom_pkam_switch,
                                "forward_link_assist" => &p.forward_link_assist,
                                "conversion_name" => &p.conversion_name,
                                "auto_acquisition_status" => &p.auto_acquisition_status,
                                "cost_constraint_scene" => &p.cost_constraint_scene,
                                "custom_cost_cap" => &p.custom_cost_cap,
                                "mpa_spec" => &p.mpa_spec,
                                "deep_conversion_behavior_goal" => &p.deep_conversion_behavior_goal(),
                                "deep_conversion_worth_goal" => &p.deep_conversion_worth_goal(),
                                "deep_conversion_worth_advanced_goal" => &p.deep_conversion_worth_advanced_goal(),
                                "deep_conversion_behavior_advanced_goal" => &p.deep_conversion_behavior_advanced_goal(),
                                "deep_conversion_worth_expected_roi" => &p.deep_conversion_worth_expected_roi(),
                                "deep_conversion_worth_advanced_expected_roi" => &p.deep_conversion_worth_advanced_expected_roi(),
                                "smart_delivery_platform" => &p.smart_delivery_platform,
                                "smart_delivery_scene_spec" => &p.smart_delivery_scene_spec,
                                "project_ability_list" => &p.project_ability_list,
                                "smart_targeting_status" => &p.smart_targeting_status,
                            }
                        }),
                    )?;
                }
                return Ok(());
            }
            ApiRes { code, message, .. } => {
                gdt_wait(code, &mut token, account_id, media_id).await;
                return Err(Error::Api {
                    code,
                    message: message.unwrap_or_default(),
                });
            }
        }
    }
    Err(Error::Custom("Failed To Get Token".to_string()))
}

async fn adgroup_sync(verifies: Vec<Verify>) {
    let limiter = rate_limiter(10);
    let mut handles = vec![];
    for verify in verifies.into_iter() {
        let limiter = Arc::clone(&limiter);
        handles.push(tokio::spawn(async move {
            let rt = adgroup_api(verify.clone(), &limiter).await;
            let _rt = verify_rt(&verify, rt);
        }));
    }
    tasks_handle(handles).await;
}

#[retry]
async fn creative_api(verify: Verify, limiter: &Limiter) -> Result<()> {
    let url = &verify.url;
    let account_id = verify.account_id;
    let media_id = verify.media_id;
    let token = account_token(account_id, media_id).await;
    if let Some(mut token) = token {
        let mut params = gdt_params(&token);
        params.insert("page_size", String::from("100"));
        params.insert("fields", GDT_DYNAMIC_CREATIVE_FILED_V3.to_string());
        until_ready(limiter).await;
        let res = HTTP_CLIENT.get(url).query(&params).send().await?;
        let res_parsed = res.json::<ApiRes<ApiData<DynamicCreative>>>().await?;
        match res_parsed {
            ApiRes {
                code: 0,
                data: Some(ApiData {
                    list: Some(items), ..
                }),
                ..
            } => {
                if !items.is_empty() {
                    let mut con = TIDB_POOL.get_conn()?;
                    con.exec_batch(
                        GDT_DYNAMIC_CREATIVE_SQL_V3,
                        items.iter().map(|p| {
                            params! {
                                "account_id" => &account_id,
                                "dynamic_creative_id" => &p.dynamic_creative_id,
                                "dynamic_creative_name" => &p.dynamic_creative_name,
                                "dynamic_creative_type" => &p.dynamic_creative_type,
                                "created_time" => &p.created_time,
                                "last_modified_time" => &p.last_modified_time,
                                "is_deleted" => &p.is_deleted,
                                "adgroup_id" => &p.adgroup_id,
                                "creative_template_id" => &p.creative_template_id,
                                "delivery_mode" => &p.delivery_mode,
                                "creative_components" => &p.creative_components,
                                "configured_status" => &p.configured_status,
                                "wechat_mini_program_page_type" => &p.wechat_mini_program_page_type(),
                                "wechat_mini_program_spec" => &p.wechat_mini_program_spec(),
                                "impression_tracking_url" => &p.impression_tracking_url,
                                "click_tracking_url" => &p.click_tracking_url,
                                "page_track_url" => &p.page_track_url,
                            }
                        }),
                    )?;
                }
                return Ok(());
            }
            ApiRes { code, message, .. } => {
                gdt_wait(code, &mut token, account_id, media_id).await;
                return Err(Error::Api {
                    code,
                    message: message.unwrap_or_default(),
                });
            }
        }
    }
    Err(Error::Custom("Failed To Get Token".to_string()))
}

async fn creative_sync(verifies: Vec<Verify>) {
    let limiter = rate_limiter(10);
    let mut handles = vec![];
    for verify in verifies.into_iter() {
        let limiter = Arc::clone(&limiter);
        handles.push(tokio::spawn(async move {
            let rt = creative_api(verify.clone(), &limiter).await;
            let _rt = verify_rt(&verify, rt);
        }));
    }
    tasks_handle(handles).await;
}

#[retry]
async fn report_api(
    url: &str,
    account_id: u64,
    mut token: String,
    params: &mut HashMap<&'static str, String>,
    sql: &str,
    page: u64,
    limiter: &Limiter,
) -> Result<u64> {
    until_ready(limiter).await;
    params.insert("page", page.to_string());
    let res = HTTP_CLIENT.get(url).query(&params).send().await?;
    let res_parsed = res.json::<ApiRes<ApiData<rt::GdtReportV3>>>().await?;
    match res_parsed {
        ApiRes {
            code: 0,
            data:
                Some(ApiData {
                    list: Some(items),
                    page_info: Some(page_info),
                    ..
                }),
            ..
        } => {
            if !items.is_empty() {
                let mut con = TIDB_POOL.get_conn()?;
                con.exec_batch(sql,
                    items.iter().map(|p| params! {
                        "date" => &p.date,
                        "hour" => &p.hour,
                        "account_id" => &p.account_id,
                        "site_set" => &p.site_set,
                        "material_id" => &p.material_id,
                        "dynamic_creative_id" => &p.dynamic_creative_id,
                        "dynamic_creative_name" => &p.dynamic_creative_name,
                        "adgroup_id" => &p.adgroup_id,
                        "adgroup_name" => &p.adgroup_name,
                        "app_retention_d7_pv" => &p.app_retention_d7_pv,
                        "clk_right_grid_pv" => &p.clk_right_grid_pv,
                        "scan_follow_count" => &p.scan_follow_count,
                        "biz_follow_uv" => &p.biz_follow_uv,
                        "purchase_clk_30d_pv" => &p.purchase_clk_30d_pv,
                        "video_outer_play7s_count" => &p.video_outer_play7s_count,
                        "reservation_amount" => &p.reservation_amount,
                        "cvs_share_exp_to_feed_pv" => &p.cvs_share_exp_to_feed_pv,
                        "exp_first_spread_uv" => &p.exp_first_spread_uv,
                        "reg_dedup_pv" => &p.reg_dedup_pv,
                        "clk_story_btn_uv" => &p.clk_story_btn_uv,
                        "lan_jump_button_clickers" => &p.lan_jump_button_clickers,
                        "video_live_subscribe_count" => &p.video_live_subscribe_count,
                        "mini_game_paying_amount_d14" => &p.mini_game_paying_amount_d14,
                        "order_24h_by_click_count" => &p.order_24h_by_click_count,
                        "after_add_wecom_consult_dedup_pv" => &p.after_add_wecom_consult_dedup_pv,
                        "app_retention_d4_pv" => &p.app_retention_d4_pv,
                        "wechat_deep_conversions_count_stage1" => &p.wechat_deep_conversions_count_stage1,
                        "platform_page_navigation_count" => &p.platform_page_navigation_count,
                        "cheout_tw" => &p.cheout_tw,
                        "purchase_imp_pv" => &p.purchase_imp_pv,
                        "lan_button_click_count" => &p.lan_button_click_count,
                        "clk_tag_content_uv" => &p.clk_tag_content_uv,
                        "mini_game_paying_amount_d30" => &p.mini_game_paying_amount_d30,
                        "inte_phone_count" => &p.inte_phone_count,
                        "clk_poi_pv" => &p.clk_poi_pv,
                        "tool_consult_count" => &p.tool_consult_count,
                        "biz_consult_count" => &p.biz_consult_count,
                        "minigame_24h_pay_amount" => &p.minigame_24h_pay_amount,
                        "income_pv_1d_pla" => &p.income_pv_1d_pla,
                        "engage_uv" => &p.engage_uv,
                        "active_d3_pay_count" => &p.active_d3_pay_count,
                        "interact_succ_uv" => &p.interact_succ_uv,
                        "mini_game_ad_monetization_users" => &p.mini_game_ad_monetization_users,
                        "mini_game_pay_d14_uv" => &p.mini_game_pay_d14_uv,
                        "click_detail_count" => &p.click_detail_count,
                        "clk_break_pv" => &p.clk_break_pv,
                        "income_val_7" => &p.income_val_7,
                        "phone_consult_count" => &p.phone_consult_count,
                        "guide_to_follow_page_interaction_amount" => &p.guide_to_follow_page_interaction_amount,
                        "biz_credit_uv" => &p.biz_credit_uv,
                        "purchase_pla_active_3d_amount" => &p.purchase_pla_active_3d_amount,
                        "effective_leads_count" => &p.effective_leads_count,
                        "class_participated_fisrt_uv" => &p.class_participated_fisrt_uv,
                        "video_follow_count" => &p.video_follow_count,
                        "live_stream_crt_click_cnt" => &p.live_stream_crt_click_cnt,
                        "preview_deep_conversions_count" => &p.preview_deep_conversions_count,
                        "mini_game_bf_purchase_amount" => &p.mini_game_bf_purchase_amount,
                        "biz_order_uv" => &p.biz_order_uv,
                        "join_chat_group_number_of_people" => &p.join_chat_group_number_of_people,
                        "cheer_banner_exp_pv" => &p.cheer_banner_exp_pv,
                        "cvs_fav_pv" => &p.cvs_fav_pv,
                        "video_outer_play95_count" => &p.video_outer_play95_count,
                        "active_page_interaction_users" => &p.active_page_interaction_users,
                        "effective_reserve_count" => &p.effective_reserve_count,
                        "clk_redpocket_btn_jump_pv" => &p.clk_redpocket_btn_jump_pv,
                        "break_frame_play_uv" => &p.break_frame_play_uv,
                        "first_day_first_pay_count" => &p.first_day_first_pay_count,
                        "mini_game_paying_amount_d7" => &p.mini_game_paying_amount_d7,
                        "mini_game_first_pay_pla_amount" => &p.mini_game_first_pay_pla_amount,
                        "stay_duration_all_5_10_uv" => &p.stay_duration_all_5_10_uv,
                        "basic_info_client_count" => &p.basic_info_client_count,
                        "wechat_deep_conversions_count_stage2" => &p.wechat_deep_conversions_count_stage2,
                        "mini_game_pay_d30_pla_uv" => &p.mini_game_pay_d30_pla_uv,
                        "clk_shortcut_menus_pv" => &p.clk_shortcut_menus_pv,
                        "ad_paying_users_24h_pla" => &p.ad_paying_users_24h_pla,
                        "preview_conversions_count" => &p.preview_conversions_count,
                        "video_outer_play50_count" => &p.video_outer_play50_count,
                        "ad_paying_users_d1" => &p.ad_paying_users_d1,
                        "cheout_pv_7d" => &p.cheout_pv_7d,
                        "minigame_purchase_pla_clk_7d_amount" => &p.minigame_purchase_pla_clk_7d_amount,
                        "landing_commodity_detail_exp_pv" => &p.landing_commodity_detail_exp_pv,
                        "gallery_card_slider_uv" => &p.gallery_card_slider_uv,
                        "active_d7_pay_count" => &p.active_d7_pay_count,
                        "video_live_heart_count" => &p.video_live_heart_count,
                        "activated_count" => &p.activated_count,
                        "finder_topic_slider_uv" => &p.finder_topic_slider_uv,
                        "channels_share_pla_pv" => &p.channels_share_pla_pv,
                        "purchase_pla_active_30d_amount" => &p.purchase_pla_active_30d_amount,
                        "first_day_order_count" => &p.first_day_order_count,
                        "game_create_role_count" => &p.game_create_role_count,
                        "mini_game_bf_purchase_d1_amount" => &p.mini_game_bf_purchase_d1_amount,
                        "break_frame_ip_clk_pv" => &p.break_frame_ip_clk_pv,
                        "ad_monetization_penetration_rat_d1" => &p.ad_monetization_penetration_rat_d1,
                        "coupon_click_count" => &p.coupon_click_count,
                        "cheout_om" => &p.cheout_om,
                        "mini_game_bf_purchase_d1_uv" => &p.mini_game_bf_purchase_d1_uv,
                        "account_info_click_count" => &p.account_info_click_count,
                        "video_live_comment_count" => &p.video_live_comment_count,
                        "try_out_intention_uv" => &p.try_out_intention_uv,
                        "mini_game_pay_d7_uv" => &p.mini_game_pay_d7_uv,
                        "wechat_cost_stage2" => &p.wechat_cost_stage2,
                        "comment_at_friend_pv" => &p.comment_at_friend_pv,
                        "video_live_cick_commodity_count" => &p.video_live_cick_commodity_count,
                        "stay_duration_all_above_10_uv" => &p.stay_duration_all_above_10_uv,
                        "channels_share_offline_pv" => &p.channels_share_offline_pv,
                        "brand_share_exposure_uv" => &p.brand_share_exposure_uv,
                        "mini_game_bf_purchase_uv" => &p.mini_game_bf_purchase_uv,
                        "first_day_order_by_display_amount" => &p.first_day_order_by_display_amount,
                        "ad_monetization_dedup_active_3d_pv" => &p.ad_monetization_dedup_active_3d_pv,
                        "order_pv" => &p.order_pv,
                        "purchase_member_card_dedup_pv" => &p.purchase_member_card_dedup_pv,
                        "cvs_share_exp_to_friend_uv" => &p.cvs_share_exp_to_friend_uv,
                        "video_live_heart_user_count" => &p.video_live_heart_user_count,
                        "share_friend_pv" => &p.share_friend_pv,
                        "biz_reading_count" => &p.biz_reading_count,
                        "video_outer_play3s_count" => &p.video_outer_play3s_count,
                        "landing_page_user_count" => &p.landing_page_user_count,
                        "clk_choice_left_pv" => &p.clk_choice_left_pv,
                        "video_outer_play5s_count" => &p.video_outer_play5s_count,
                        "mini_game_paying_amount_d1" => &p.mini_game_paying_amount_d1,
                        "live_stream_commodity_bubble_clk_pv" => &p.live_stream_commodity_bubble_clk_pv,
                        "brand_share_exposure_pv" => &p.brand_share_exposure_pv,
                        "mini_game_ad_monetization_amount_d7" => &p.mini_game_ad_monetization_amount_d7,
                        "mini_game_d7_pay_count" => &p.mini_game_d7_pay_count,
                        "mini_game_first_day_ad_monetization_amount" => &p.mini_game_first_day_ad_monetization_amount,
                        "mini_game_pay_d1_pla_uv" => &p.mini_game_pay_d1_pla_uv,
                        "minigame_purchase_pla_clk_3d_amount" => &p.minigame_purchase_pla_clk_3d_amount,
                        "video_comment_count" => &p.video_comment_count,
                        "ineffective_leads_uv" => &p.ineffective_leads_uv,
                        "minigame_purchase_pla_clk_14d_amount" => &p.minigame_purchase_pla_clk_14d_amount,
                        "cheout_fd" => &p.cheout_fd,
                        "cheout_pv_5d" => &p.cheout_pv_5d,
                        "mini_game_bf_income_d1_amount" => &p.mini_game_bf_income_d1_amount,
                        "cheer_status_set_succ_pv" => &p.cheer_status_set_succ_pv,
                        "from_follow_by_display_uv" => &p.from_follow_by_display_uv,
                        "clk_footer_pv" => &p.clk_footer_pv,
                        "landing_page_view_count" => &p.landing_page_view_count,
                        "channels_fav_offline_pv" => &p.channels_fav_offline_pv,
                        "income_val_1" => &p.income_val_1,
                        "video_outer_play90_count" => &p.video_outer_play90_count,
                        "guide_to_follow_page_views" => &p.guide_to_follow_page_views,
                        "reservation_check_uv" => &p.reservation_check_uv,
                        "order_24h_by_display_count" => &p.order_24h_by_display_count,
                        "mini_game_pay_d7_pla_uv" => &p.mini_game_pay_d7_pla_uv,
                        "scan_follow_user_count" => &p.scan_follow_user_count,
                        "break_frame_play_pv" => &p.break_frame_play_pv,
                        "wechat_local_pay_amount" => &p.wechat_local_pay_amount,
                        "live_stream_order_pv" => &p.live_stream_order_pv,
                        "clk_nick_pv" => &p.clk_nick_pv,
                        "potential_customer_phone_uv" => &p.potential_customer_phone_uv,
                        "break_frame_play_duration" => &p.break_frame_play_duration,
                        "comment_reply_frist_pv" => &p.comment_reply_frist_pv,
                        "clk_tag_comment_pv" => &p.clk_tag_comment_pv,
                        "click_user_count" => &p.click_user_count,
                        "mini_game_paying_users_d1" => &p.mini_game_paying_users_d1,
                        "first_day_pay_amount_arppu" => &p.first_day_pay_amount_arppu,
                        "clk_card_tag_pv" => &p.clk_card_tag_pv,
                        "coupon_issue_count" => &p.coupon_issue_count,
                        "lottery_leads_count" => &p.lottery_leads_count,
                        "stay_duration_cvs_above_10_uv" => &p.stay_duration_cvs_above_10_uv,
                        "purchase_pla_active_14d_pv" => &p.purchase_pla_active_14d_pv,
                        "withdraw_deposit_amount" => &p.withdraw_deposit_amount,
                        "purchase_pla_active_3d_pv" => &p.purchase_pla_active_3d_pv,
                        "cvs_bubble_share_clk_uv" => &p.cvs_bubble_share_clk_uv,
                        "video_outer_play100_count" => &p.video_outer_play100_count,
                        "order_clk_7d_amount" => &p.order_clk_7d_amount,
                        "clk_slider_card_btn_pv" => &p.clk_slider_card_btn_pv,
                        "clk_account_living_status_pv" => &p.clk_account_living_status_pv,
                        "interact_root_uv" => &p.interact_root_uv,
                        "mini_game_pay_d3_uv" => &p.mini_game_pay_d3_uv,
                        "clk_tag_comment_uv" => &p.clk_tag_comment_uv,
                        "first_pay_count" => &p.first_pay_count,
                        "exp_root_uv" => &p.exp_root_uv,
                        "video_live_share_count" => &p.video_live_share_count,
                        "conversions_by_display_count" => &p.conversions_by_display_count,
                        "fullsrc_slide_pv" => &p.fullsrc_slide_pv,
                        "finder_topic_slider_manual_pv" => &p.finder_topic_slider_manual_pv,
                        "income_val_3" => &p.income_val_3,
                        "biz_page_apply_uv" => &p.biz_page_apply_uv,
                        "purchase_pla_clk_1d_amount" => &p.purchase_pla_clk_1d_amount,
                        "exp_second_spread_uv" => &p.exp_second_spread_uv,
                        "zone_header_click_count" => &p.zone_header_click_count,
                        "stay_duration_all_3_4_uv" => &p.stay_duration_all_3_4_uv,
                        "clk_slider_card_btn_uv" => &p.clk_slider_card_btn_uv,
                        "conversions_count" => &p.conversions_count,
                        "click_nick_count" => &p.click_nick_count,
                        "mini_game_bf_income_uv" => &p.mini_game_bf_income_uv,
                        "stay_duration_outer" => &p.stay_duration_outer,
                        "app_retention_d5_pv" => &p.app_retention_d5_pv,
                        "clk_choice_right_pv" => &p.clk_choice_right_pv,
                        "mini_game_first_day_ad_monetization_users" => &p.mini_game_first_day_ad_monetization_users,
                        "active_page_views" => &p.active_page_views,
                        "app_ad_paying_users" => &p.app_ad_paying_users,
                        "purchase_reg_arppu" => &p.purchase_reg_arppu,
                        "stay_duration_cvs" => &p.stay_duration_cvs,
                        "clk_choice_left_uv" => &p.clk_choice_left_uv,
                        "order_by_display_count" => &p.order_by_display_count,
                        "mini_game_ad_monetization_amount_d3" => &p.mini_game_ad_monetization_amount_d3,
                        "income_val_24h_pla" => &p.income_val_24h_pla,
                        "interact_succ_pv" => &p.interact_succ_pv,
                        "app_credit_uv" => &p.app_credit_uv,
                        "post_barrage_uv" => &p.post_barrage_uv,
                        "clk_left_grid_middle_pv" => &p.clk_left_grid_middle_pv,
                        "valid_leads_uv" => &p.valid_leads_uv,
                        "biz_withdraw_deposits_uv" => &p.biz_withdraw_deposits_uv,
                        "reg_pv" => &p.reg_pv,
                        "view_count" => &p.view_count,
                        "praise_comment_share_pv" => &p.praise_comment_share_pv,
                        "purchase_pla_pv" => &p.purchase_pla_pv,
                        "mini_game_key_page_viewers" => &p.mini_game_key_page_viewers,
                        "mini_game_bf_income_d1_uv" => &p.mini_game_bf_income_d1_uv,
                        "app_retention_d3_pv" => &p.app_retention_d3_pv,
                        "phone_call_uv" => &p.phone_call_uv,
                        "income_pv_pla" => &p.income_pv_pla,
                        "channels_read_offline_pv" => &p.channels_read_offline_pv,
                        "valid_phone_uv" => &p.valid_phone_uv,
                        "live_stream_commodity_shop_list_exp_pv" => &p.live_stream_commodity_shop_list_exp_pv,
                        "praise_comment_pv" => &p.praise_comment_pv,
                        "mini_game_ad_monetization_amount_d14" => &p.mini_game_ad_monetization_amount_d14,
                        "finder_topic_slider_video_play_uv" => &p.finder_topic_slider_video_play_uv,
                        "channels_comment_offline_pv" => &p.channels_comment_offline_pv,
                        "stay_duration_cvs_3_9_uv" => &p.stay_duration_cvs_3_9_uv,
                        "biz_reservation_uv" => &p.biz_reservation_uv,
                        "video_live_comment_user_count" => &p.video_live_comment_user_count,
                        "stay_duration_all_above_5_uv" => &p.stay_duration_all_above_5_uv,
                        "ad_monetization_dedup_active_7d_pv" => &p.ad_monetization_dedup_active_7d_pv,
                        "coupon_usage_number" => &p.coupon_usage_number,
                        "add_desktop_pv" => &p.add_desktop_pv,
                        "clk_account_info_productdetail_pv" => &p.clk_account_info_productdetail_pv,
                        "minigame_3d_income_count" => &p.minigame_3d_income_count,
                        "store_visitor" => &p.store_visitor,
                        "payment_amount_activated_d30" => &p.payment_amount_activated_d30,
                        "read_count" => &p.read_count,
                        "active_d5_first_pay_uv" => &p.active_d5_first_pay_uv,
                        "view_commodity_page_uv" => &p.view_commodity_page_uv,
                        "cost" => &p.cost,
                        "stay_pay_30d_pv" => &p.stay_pay_30d_pv,
                        "first_day_pay_count" => &p.first_day_pay_count,
                        "minigame_purchase_pla_clk_1d_amount" => &p.minigame_purchase_pla_clk_1d_amount,
                        "biz_reg_uv" => &p.biz_reg_uv,
                        "reservation_uv" => &p.reservation_uv,
                        "deep_conversions_count" => &p.deep_conversions_count,
                        "click_head_count" => &p.click_head_count,
                        "ad_monetization_active_3d_pv" => &p.ad_monetization_active_3d_pv,
                        "stay_duration_cvs_above_30_uv" => &p.stay_duration_cvs_above_30_uv,
                        "video_play_count" => &p.video_play_count,
                        "comment_uv" => &p.comment_uv,
                        "retention_count" => &p.retention_count,
                        "minigame_24h_pay_uv" => &p.minigame_24h_pay_uv,
                        "break_frame_ip_exp_uv" => &p.break_frame_ip_exp_uv,
                        "click_poi_count" => &p.click_poi_count,
                        "active_d30_pay_count" => &p.active_d30_pay_count,
                        "finder_topic_slider_pv" => &p.finder_topic_slider_pv,
                        "channels_live_out_enter_pla_uv" => &p.channels_live_out_enter_pla_uv,
                        "video_live_exp_count" => &p.video_live_exp_count,
                        "clk_goods_info_pv" => &p.clk_goods_info_pv,
                        "follow_count" => &p.follow_count,
                        "video_live_commodity_bubble_exp_count" => &p.video_live_commodity_bubble_exp_count,
                        "download_count" => &p.download_count,
                        "no_interest_count" => &p.no_interest_count,
                        "cheer_status_clk_pv" => &p.cheer_status_clk_pv,
                        "minigame_purchase_pla_clk_30d_amount" => &p.minigame_purchase_pla_clk_30d_amount,
                        "key_page_view_by_display_count" => &p.key_page_view_by_display_count,
                        "clk_story_btn_pv" => &p.clk_story_btn_pv,
                        "interact_first_spread_uv" => &p.interact_first_spread_uv,
                        "page_phone_call_direct_count" => &p.page_phone_call_direct_count,
                        "valuable_click_count" => &p.valuable_click_count,
                        "mini_game_pay_d3_pla_uv" => &p.mini_game_pay_d3_pla_uv,
                        "clk_middle_showwindow_pv" => &p.clk_middle_showwindow_pv,
                        "coupon_get_pv" => &p.coupon_get_pv,
                        "clk_goods_header_pv" => &p.clk_goods_header_pv,
                        "clk_redpocket_btn_share_pv" => &p.clk_redpocket_btn_share_pv,
                        "request_conversions_count" => &p.request_conversions_count,
                        "share_feed_pv" => &p.share_feed_pv,
                        "mini_game_register_users" => &p.mini_game_register_users,
                        "activity_info_click_count" => &p.activity_info_click_count,
                        "from_follow_uv" => &p.from_follow_uv,
                        "overall_brand_exposure" => &p.overall_brand_exposure,
                        "first_day_pay_amount" => &p.first_day_pay_amount,
                        "order_clk_30d_pv" => &p.order_clk_30d_pv,
                        "purchase_pla_active_14d_amount" => &p.purchase_pla_active_14d_amount,
                        "income_val_14" => &p.income_val_14,
                        "channels_live_exit_pla_duration" => &p.channels_live_exit_pla_duration,
                        "purchase_pla_active_30d_pv" => &p.purchase_pla_active_30d_pv,
                        "platform_page_view_count" => &p.platform_page_view_count,
                        "platform_coupon_click_count" => &p.platform_coupon_click_count,
                        "add_wishlist_count" => &p.add_wishlist_count,
                        "mini_game_first_pay_amount" => &p.mini_game_first_pay_amount,
                        "add_cart_pv" => &p.add_cart_pv,
                        "clk_redpocket_btn_subscribe_pv" => &p.clk_redpocket_btn_subscribe_pv,
                        "key_behavior_conversions_count" => &p.key_behavior_conversions_count,
                        "first_day_order_by_display_count" => &p.first_day_order_by_display_count,
                        "video_live_share_user_count" => &p.video_live_share_user_count,
                        "engage_pv" => &p.engage_pv,
                        "finder_topic_slider_auto_uv" => &p.finder_topic_slider_auto_uv,
                        "cvs_bubble_share_clk_pv" => &p.cvs_bubble_share_clk_pv,
                        "clk_nick_uv" => &p.clk_nick_uv,
                        "order_clk_15d_amount" => &p.order_clk_15d_amount,
                        "key_page_uv" => &p.key_page_uv,
                        "break_frame_exp_uv" => &p.break_frame_exp_uv,
                        "active_d14_pay_count" => &p.active_d14_pay_count,
                        "register_by_click_count" => &p.register_by_click_count,
                        "clk_accountinfo_biz_pv" => &p.clk_accountinfo_biz_pv,
                        "valid_click_count" => &p.valid_click_count,
                        "biz_pre_credit_uv" => &p.biz_pre_credit_uv,
                        "purchase_clk_15d_pv" => &p.purchase_clk_15d_pv,
                        "video_outer_play75_count" => &p.video_outer_play75_count,
                        "first_day_order_by_click_count" => &p.first_day_order_by_click_count,
                        "app_apply_uv" => &p.app_apply_uv,
                        "app_retention_lt7" => &p.app_retention_lt7,
                        "sign_in_amount" => &p.sign_in_amount,
                        "order_follow_1d_pv" => &p.order_follow_1d_pv,
                        "game_authorize_count" => &p.game_authorize_count,
                        "order_clk_7d_pv" => &p.order_clk_7d_pv,
                        "app_withdraw_uv" => &p.app_withdraw_uv,
                        "guide_to_follow_page_interaction_users" => &p.guide_to_follow_page_interaction_users,
                        "effect_leads_purchase_count" => &p.effect_leads_purchase_count,
                        "effective_phone_count" => &p.effective_phone_count,
                        "clk_poi_uv" => &p.clk_poi_uv,
                        "reg_pla_pv" => &p.reg_pla_pv,
                        "video_live_click_commodity_user_count" => &p.video_live_click_commodity_user_count,
                        "install_count" => &p.install_count,
                        "minigame_1d_pay_count" => &p.minigame_1d_pay_count,
                        "purchase_pla_active_1d_amount" => &p.purchase_pla_active_1d_amount,
                        "praise_comment_share_uv" => &p.praise_comment_share_uv,
                        "deliver_count" => &p.deliver_count,
                        "pre_credit_pv" => &p.pre_credit_pv,
                        "break_frame_exp_pv" => &p.break_frame_exp_pv,
                        "ad_monetization_amount" => &p.ad_monetization_amount,
                        "live_stream_commodity_shop_bag_clk_pv" => &p.live_stream_commodity_shop_bag_clk_pv,
                        "clk_action_btn_uv" => &p.clk_action_btn_uv,
                        "mini_game_d30_pay_count" => &p.mini_game_d30_pay_count,
                        "cheout_pv_1d" => &p.cheout_pv_1d,
                        "ad_paying_users_24h" => &p.ad_paying_users_24h,
                        "forward_count" => &p.forward_count,
                        "stay_duration_cvs_0_2_uv" => &p.stay_duration_cvs_0_2_uv,
                        "first_day_order_by_click_amount" => &p.first_day_order_by_click_amount,
                        "stay_duration_cvs_10_29_uv" => &p.stay_duration_cvs_10_29_uv,
                        "page_reservation_by_display_count" => &p.page_reservation_by_display_count,
                        "channels_heart_offline_pv" => &p.channels_heart_offline_pv,
                        "finder_topic_slider_video_play_pv" => &p.finder_topic_slider_video_play_pv,
                        "lp_star_page_exp_pv" => &p.lp_star_page_exp_pv,
                        "live_stream_exp_uv" => &p.live_stream_exp_uv,
                        "sign_in_count" => &p.sign_in_count,
                        "active_page_viewers" => &p.active_page_viewers,
                        "clk_accountinfo_weapp_pv" => &p.clk_accountinfo_weapp_pv,
                        "overall_leads_purchase_count" => &p.overall_leads_purchase_count,
                        "clk_middle_gridview_pv" => &p.clk_middle_gridview_pv,
                        "video_outer_play_count" => &p.video_outer_play_count,
                        "clk_detail_uv" => &p.clk_detail_uv,
                        "credit_pv" => &p.credit_pv,
                        "video_inner_play_count" => &p.video_inner_play_count,
                        "cheout_pv_3d" => &p.cheout_pv_3d,
                        "gallery_card_slider_pv" => &p.gallery_card_slider_pv,
                        "clk_card_tag_uv" => &p.clk_card_tag_uv,
                        "exp_spread_pv" => &p.exp_spread_pv,
                        "cheer_status_clk_uv" => &p.cheer_status_clk_uv,
                        "break_frame_ip_clk_uv" => &p.break_frame_ip_clk_uv,
                        "channels_praise_pla_pv" => &p.channels_praise_pla_pv,
                        "scan_code_add_fans_count" => &p.scan_code_add_fans_count,
                        "video_outer_play_user_count" => &p.video_outer_play_user_count,
                        "live_stream_order_amount" => &p.live_stream_order_amount,
                        "clk_goods_recommend_pv" => &p.clk_goods_recommend_pv,
                        "finder_topic_slider_card_clk_pv" => &p.finder_topic_slider_card_clk_pv,
                        "payment_amount_activated_d7" => &p.payment_amount_activated_d7,
                        "purchase_pla_active_7d_pv" => &p.purchase_pla_active_7d_pv,
                        "key_page_view_count" => &p.key_page_view_count,
                        "page_consult_count" => &p.page_consult_count,
                        "clk_related_video_pv" => &p.clk_related_video_pv,
                        "order_amount" => &p.order_amount,
                        "apply_pv" => &p.apply_pv,
                        "biz_reg_order_amount" => &p.biz_reg_order_amount,
                        "post_barrage_pv" => &p.post_barrage_pv,
                        "order_24h_count" => &p.order_24h_count,
                        "interact_second_spread_uv" => &p.interact_second_spread_uv,
                        "consult_leave_info_users" => &p.consult_leave_info_users,
                        "withdraw_deposit_pv" => &p.withdraw_deposit_pv,
                        "register_by_display_count" => &p.register_by_display_count,
                        "comment_at_friend_uv" => &p.comment_at_friend_uv,
                        "share_uv" => &p.share_uv,
                        "potential_consult_count" => &p.potential_consult_count,
                        "stay_duration_all" => &p.stay_duration_all,
                        "video_heart_count" => &p.video_heart_count,
                        "coupon_get_count" => &p.coupon_get_count,
                        "order_24h_by_click_amount" => &p.order_24h_by_click_amount,
                        "cheout_td" => &p.cheout_td,
                        "page_reservation_by_click_count" => &p.page_reservation_by_click_count,
                        "try_out_user" => &p.try_out_user,
                        "cheout_15d" => &p.cheout_15d,
                        "app_commodity_page_view_by_click_count" => &p.app_commodity_page_view_by_click_count,
                        "ad_monetization_active_7d_pv" => &p.ad_monetization_active_7d_pv,
                        "app_retention_d6_pv" => &p.app_retention_d6_pv,
                        "clk_account_info_producttab_pv" => &p.clk_account_info_producttab_pv,
                        "web_apply_uv" => &p.web_apply_uv,
                        "mini_game_d3_pay_count" => &p.mini_game_d3_pay_count,
                        "clk_activity_news_pv" => &p.clk_activity_news_pv,
                        "order_follow_1d_amount" => &p.order_follow_1d_amount,
                        "clk_middle_section_pv" => &p.clk_middle_section_pv,
                        "order_by_click_count" => &p.order_by_click_count,
                        "wechat_local_payuser_count" => &p.wechat_local_payuser_count,
                        "after_add_wecom_intention_dedup_pv" => &p.after_add_wecom_intention_dedup_pv,
                        "app_retention_d2_pv" => &p.app_retention_d2_pv,
                        "invite_friends_to_watch_fireworks_pv" => &p.invite_friends_to_watch_fireworks_pv,
                        "stay_pay_15d_pv" => &p.stay_pay_15d_pv,
                        "join_chat_group_amount" => &p.join_chat_group_amount,
                        "potential_phone_count" => &p.potential_phone_count,
                        "view_user_count" => &p.view_user_count,
                        "clk_middle_btn_pv" => &p.clk_middle_btn_pv,
                        "biz_follow_count" => &p.biz_follow_count,
                        "income_pv_24h_pla" => &p.income_pv_24h_pla,
                        "biz_reg_count" => &p.biz_reg_count,
                        "minigame_3d_income_uv" => &p.minigame_3d_income_uv,
                        "mini_game_bf_income_amount" => &p.mini_game_bf_income_amount,
                        "own_page_navigation_count" => &p.own_page_navigation_count,
                        "add_cart_amount" => &p.add_cart_amount,
                        "purchase_pla_active_7d_amount" => &p.purchase_pla_active_7d_amount,
                        "clk_head_uv" => &p.clk_head_uv,
                        "pre_credit_amount" => &p.pre_credit_amount,
                        "mini_game_pay_d14_pla_uv" => &p.mini_game_pay_d14_pla_uv,
                        "web_credit_uv" => &p.web_credit_uv,
                        "game_tutorial_finish_count" => &p.game_tutorial_finish_count,
                        "mini_game_create_role_users" => &p.mini_game_create_role_users,
                        "clk_ad_element_pv" => &p.clk_ad_element_pv,
                        "purchase_member_card_pv" => &p.purchase_member_card_pv,
                        "clk_read_comment_pv" => &p.clk_read_comment_pv,
                        "scan_code_add_fans_uv" => &p.scan_code_add_fans_uv,
                        "mini_game_retention_d1" => &p.mini_game_retention_d1,
                        "video_outer_play25_count" => &p.video_outer_play25_count,
                        "purchase_pla_amount" => &p.purchase_pla_amount,
                        "app_retention_d3_uv" => &p.app_retention_d3_uv,
                        "clk_middle_goods_pv" => &p.clk_middle_goods_pv,
                        "purchase_clk_pv" => &p.purchase_clk_pv,
                        "purchase_amount_with_coupon" => &p.purchase_amount_with_coupon,
                        "ad_monetization_arppu" => &p.ad_monetization_arppu,
                        "external_form_reservation_count" => &p.external_form_reservation_count,
                        "from_follow_by_click_uv" => &p.from_follow_by_click_uv,
                        "active_page_interaction_amount" => &p.active_page_interaction_amount,
                        "payment_amount_activated_d3" => &p.payment_amount_activated_d3,
                        "clk_material_uv" => &p.clk_material_uv,
                        "wechat_local_pay_count" => &p.wechat_local_pay_count,
                        "click_image_count" => &p.click_image_count,
                        "conversions_cost" => &p.conversions_cost,
                        "order_24h_amount" => &p.order_24h_amount,
                        "clk_accountinfo_finder_pv" => &p.clk_accountinfo_finder_pv,
                        "cheout_ow" => &p.cheout_ow,
                        "clk_left_grid_info_pv" => &p.clk_left_grid_info_pv,
                        "wechat_shallow_conversions_count_stage1" => &p.wechat_shallow_conversions_count_stage1,
                        "mini_game_first_paying_users" => &p.mini_game_first_paying_users,
                        "leads_purchase_uv" => &p.leads_purchase_uv,
                        "wechat_shallow_conversions_count_stage2" => &p.wechat_shallow_conversions_count_stage2,
                        "stay_pay_7d_pv" => &p.stay_pay_7d_pv,
                        "cvs_share_exp_to_friend_pv" => &p.cvs_share_exp_to_friend_pv,
                        "clk_redpocket_shake_pv" => &p.clk_redpocket_shake_pv,
                        "conversions_by_click_count" => &p.conversions_by_click_count,
                        "praise_uv" => &p.praise_uv,
                        "finder_topic_slider_manual_uv" => &p.finder_topic_slider_manual_uv,
                        "comment_count" => &p.comment_count,
                        "clk_action_btn_pv" => &p.clk_action_btn_pv,
                        "mini_game_paying_amount_d3" => &p.mini_game_paying_amount_d3,
                        "video_outer_play10_count" => &p.video_outer_play10_count,
                        "potential_reserve_count" => &p.potential_reserve_count,
                        "lp_star_page_exp_uv" => &p.lp_star_page_exp_uv,
                        "acquisition_cost" => &p.acquisition_cost,
                        "video_time_total_count" => &p.video_time_total_count,
                        "order_by_display_amount" => &p.order_by_display_amount,
                        "order_uv" => &p.order_uv,
                        "deep_conversions_cost" => &p.deep_conversions_cost,
                        "praise_count" => &p.praise_count,
                        "lp_star_page_clk_pv" => &p.lp_star_page_clk_pv,
                        "mini_game_bf_uv" => &p.mini_game_bf_uv,
                        "mini_game_pay_d30_uv" => &p.mini_game_pay_d30_uv,
                        "clk_btn_follow_pv" => &p.clk_btn_follow_pv,
                        "lp_star_page_clk_uv" => &p.lp_star_page_clk_uv,
                        "mini_game_d14_pay_count" => &p.mini_game_d14_pay_count,
                        "app_retention_d7_uv" => &p.app_retention_d7_uv,
                        "phone_call_count" => &p.phone_call_count,
                        "reg_all_dedup_pv" => &p.reg_all_dedup_pv,
                        "effective_consult_count" => &p.effective_consult_count,
                        "clk_redpocket_btn_get_pv" => &p.clk_redpocket_btn_get_pv,
                        "income_val_24h" => &p.income_val_24h,
                        "order_clk_30d_amount" => &p.order_clk_30d_amount,
                        "clk_choice_right_uv" => &p.clk_choice_right_uv,
                        "clk_redpocket_shake_uv" => &p.clk_redpocket_shake_uv,
                        "mini_game_ad_monetization_amount" => &p.mini_game_ad_monetization_amount,
                        "first_day_order_amount" => &p.first_day_order_amount,
                        "order_24h_by_display_amount" => &p.order_24h_by_display_amount,
                        "clk_tag_content_pv" => &p.clk_tag_content_pv,
                        "clk_blessing_card_pv" => &p.clk_blessing_card_pv,
                        "break_frame_ip_exp_pv" => &p.break_frame_ip_exp_pv,
                        "clk_brand_pedia_pv" => &p.clk_brand_pedia_pv,
                        "payment_amount_activated_d14" => &p.payment_amount_activated_d14,
                        "mini_game_paying_users_pla_d1" => &p.mini_game_paying_users_pla_d1,
                        "cheer_status_set_succ_uv" => &p.cheer_status_set_succ_uv,
                        "credit_amount" => &p.credit_amount,
                        "purchase_amount" => &p.purchase_amount,
                        "key_page_view_by_click_count" => &p.key_page_view_by_click_count,
                        "minigame_7d_income_uv" => &p.minigame_7d_income_uv,
                        "finder_topic_slider_card_exp_pv" => &p.finder_topic_slider_card_exp_pv,
                        "comment_reply_frist_uv" => &p.comment_reply_frist_uv,
                        "free_exposure_pv" => &p.free_exposure_pv,
                        "purchase_pv" => &p.purchase_pv,
                        "stay_duration_all_0_2_uv" => &p.stay_duration_all_0_2_uv,
                        "app_commodity_page_view_by_display_count" => &p.app_commodity_page_view_by_display_count,
                        "minigame_7d_income_count" => &p.minigame_7d_income_count,
                        "consult_uv_count" => &p.consult_uv_count,
                        "platform_shop_navigation_count" => &p.platform_shop_navigation_count,
                        "wecom_add_personal_dedup_pv" => &p.wecom_add_personal_dedup_pv,
                        "app_retention_d5_uv" => &p.app_retention_d5_uv,
                        "page_phone_call_back_count" => &p.page_phone_call_back_count,
                        "platform_key_page_view_user_count" => &p.platform_key_page_view_user_count,
                        "quit_chat_group_amount" => &p.quit_chat_group_amount,
                        "order_by_click_amount" => &p.order_by_click_amount,
                        "page_reservation_count" => &p.page_reservation_count,
                        "real_cost_top" => &p.real_cost_top,
                        "order_clk_15d_pv" => &p.order_clk_15d_pv,
                        "wechat_cost_stage1" => &p.wechat_cost_stage1,
                        "guide_to_follow_page_viewers" => &p.guide_to_follow_page_viewers,
                        "app_pre_credit_uv" => &p.app_pre_credit_uv,
                        "video_outer_play_time_count" => &p.video_outer_play_time_count
                    })
                )?;
            }
            return Ok(page_info.total_page);
        }
        ApiRes { code, message, .. } => {
            gdt_wait(code, &mut token, account_id, 2).await;
            return Err(Error::Api {
                code,
                message: message.unwrap_or_default(),
            });
        }
    }
}

pub async fn report_sync(cate: &str, verifies: Vec<Verify>) {
    let time_line = match cate {
        "adgroup_daily_request_part_v3" => "REQUEST_TIME",
        "advertiser_reporting_part_v3" => "REPORTING_TIME",
        "dynamic_creative_daily_request_part_v3" => "REQUEST_TIME",
        "advertiser_daily_reporting_part_v3" => "REPORTING_TIME",
        _ => "",
    };
    let sql = match cate {
        "adgroup_daily_request_part_v3" => GDT_ADGROUP_DAILY_SQL_TL_REQUEST_V3,
        "advertiser_reporting_part_v3" => GDT_ACCOUNT_HOURLY_SQL_TL_REPORTING_V3,
        "dynamic_creative_daily_request_part_v3" => GDT_DYNAMIC_CREATIVE_DAILY_SQL_TL_REQUEST_V3,
        "advertiser_daily_reporting_part_v3" => GDT_ACCOUNT_DAILY_SQL_TL_REPORTING_V3,
        _ => EMPTY,
    };
    let fields = match cate {
        "advertiser_reporting_part_v3" => GDT_ACCOUNT_HOURLY_REPORT_FILED_TL_REPORTING_V3,
        "advertiser_daily_reporting_part_v3" => GDT_ACCOUNT_HOURLY_REPORT_FILED_TL_REPORTING_V3,
        "dynamic_creative_daily_request_part_v3" => CREATIVE_HOURLY_REPORT_FILED_TL_REQUEST_V3,
        "adgroup_daily_request_part_v3" => GDT_ADGROUP_HOURLY_REPORT_FILED_TL_REQUEST_V3,
        _ => EMPTY,
    };
    let group_by = match cate {
        "advertiser_reporting_part_v3" => GDT_ACCOUNT_REPORT_HOURLY_GROUPBY_V3,
        "advertiser_daily_reporting_part_v3" => GDT_ACCOUNT_REPORT_DAILY_GROUPBY_V3,
        "dynamic_creative_daily_request_part_v3" => GDT_DYNAMIC_CREATIVE_REPORT_DAILY_GROUPBY_V3,
        "adgroup_daily_request_part_v3" => GDT_ADGROUP_REPORT_DAILY_GROUPBY_V3,
        _ => EMPTY,
    };
    let level = match cate {
        "advertiser_reporting_part_v3" => "REPORT_LEVEL_ADVERTISER",
        "advertiser_daily_reporting_part_v3" => "REPORT_LEVEL_ADVERTISER",
        "dynamic_creative_daily_request_part_v3" => "REPORT_LEVEL_DYNAMIC_CREATIVE",
        "adgroup_daily_request_part_v3" => "REPORT_LEVEL_ADGROUP",
        _ => EMPTY,
    };
    if group_by.is_empty() || fields.is_empty() || sql.is_empty() || level.is_empty() {
        return;
    }
    let limiter = rate_limiter(10);
    let mut handles = vec![];
    for verify in verifies {
        let url = &verify.url;
        let account_id = verify.account_id;
        let media_id = verify.media_id;
        let token = account_token(account_id, media_id).await;
        if let Some(token) = token {
            let mut params = gdt_params(&token);
            params.insert("page_size", String::from("500"));
            params.insert("fields", fields.to_string());
            params.insert("group_by", group_by.to_string());
            params.insert("account_id", account_id.to_string());
            params.insert("level", level.to_string());
            params.insert("time_line", time_line.to_string());
            let limiter = Arc::clone(&limiter);
            let url = url.clone();
            handles.push(tokio::spawn(async move {
                let rt = report_api(
                    &url,
                    account_id,
                    token.clone(),
                    &mut params,
                    sql,
                    1,
                    &limiter,
                )
                .await;
                let rt = match rt {
                    Ok(page) if page > 1 => {
                        let mut tasks = vec![];
                        for pg in 2..=page {
                            let token = token.clone();
                            let mut params = params.clone();
                            let url = url.clone();
                            let limiter = Arc::clone(&limiter);
                            tasks.push(tokio::spawn(async move {
                                let _rt = report_api(
                                    &url,
                                    account_id,
                                    token,
                                    &mut params,
                                    sql,
                                    pg,
                                    &limiter,
                                )
                                .await;
                            }));
                        }
                        tasks_handle(tasks).await;
                        Ok(())
                    }
                    Err(err) => Err(err),
                    Ok(_pg) => Ok(()),
                };
                let _rt = verify_rt(&verify, rt);
            }));
        }
    }
    tasks_handle(handles).await;
}

#[retry]
async fn account_api(verify: Verify, limiter: &Limiter) -> Result<()> {
    let url = &verify.url;
    let account_id = verify.account_id;
    let media_id = verify.media_id;
    let token = account_token(account_id, media_id).await;
    if let Some(mut token) = token {
        let mut params = gdt_params(&token);
        params.insert("fields", GDT_ACCOUNT_FIELD.to_string());
        until_ready(limiter).await;
        let res = HTTP_CLIENT.get(url).query(&params).send().await?;
        let res_parsed = res.json::<ApiRes<ApiData<GdtAdvertiser>>>().await?;
        match res_parsed {
            ApiRes {
                code: 0,
                data: Some(ApiData {
                    list: Some(items), ..
                }),
                ..
            } => {
                if !items.is_empty() {
                    let mut con = CORE_POOL.get_conn()?;
                    con.exec_batch("INSERT INTO gdt_advertiser (reject_message, uid, daily_budget, corporation, create_time, status, deleted, platform_agency_id, system_industry_id) VALUES (:reject_message, :uid, :daily_budget, :corporation, NOW(), :status, 0, :platform_agency_id, :system_industry_id) ON DUPLICATE KEY UPDATE status = :status, corporation = :corporation, daily_budget = :daily_budget, update_time = NOW(), reject_message = :reject_message, today_granted = 1, system_industry_id = :system_industry_id",
                        items.iter().map(|p| params! {
                            "uid" => &p.account_id,
                            "corporation" => &p.corporation_name,
                            "status" => &p.system_status,
                            "reject_message" => &p.reject_message,
                            "daily_budget" => &p.daily_budget,
                            "platform_agency_id" => &p.agency_account_id,
                            "system_industry_id" => &p.system_industry_id,
                        }),
                    )?;
                }
                return Ok(());
            }
            ApiRes { code, message, .. } => {
                gdt_wait(code, &mut token, account_id, media_id).await;
                return Err(Error::Api {
                    code,
                    message: message.unwrap_or_default(),
                });
            }
        }
    }
    Err(Error::Custom("Failed To Get Token".to_string()))
}

async fn account_sync(verifies: Vec<Verify>) {
    let limiter = rate_limiter(10);
    let mut handles = vec![];
    for verify in verifies.into_iter() {
        let limiter = Arc::clone(&limiter);
        handles.push(tokio::spawn(async move {
            let rt = account_api(verify.clone(), &limiter).await;
            let _rt = verify_rt(&verify, rt);
        }));
    }
    tasks_handle(handles).await;
}

#[retry]
async fn target_api(verify: Verify, limiter: &Limiter) -> Result<()> {
    let url = &verify.url;
    let account_id = verify.account_id;
    let media_id = verify.media_id;
    let token = account_token(account_id, media_id).await;
    if let Some(mut token) = token {
        let params = gdt_params(&token);
        let body = &verify.body.clone().unwrap_or_default();
        until_ready(limiter).await;
        let res = HTTP_CLIENT
            .post(url)
            .query(&params)
            .json(body)
            .send()
            .await?;
        let res_parsed = res.json::<ApiRes<ApiData<GdtTarget>>>().await?;
        match res_parsed {
            ApiRes {
                code: 0,
                data: Some(ApiData {
                    list: Some(items), ..
                }),
                ..
            } => {
                if !items.is_empty() {
                    let mut con = TIDB_POOL.get_conn()?;
                    con.exec_batch("INSERT INTO `gdt_ad_targeting` (`account_id`, `targeting_id`, `targeting_name`, `targeting`, `description`, `is_deleted`, `created_time`, `last_modified_time`, `targeting_translation`, `targeting_source_type`, `share_from_account_id`, `share_from_targeting_id`) VALUES (:account_id, :targeting_id, :targeting_name, :targeting, :description, :is_deleted, :created_time, :last_modified_time, :targeting_translation, :targeting_source_type, :share_from_account_id, :share_from_targeting_id) ON DUPLICATE KEY UPDATE targeting_name = :targeting_name, targeting = :targeting, description = :description, is_deleted = :is_deleted, last_modified_time = :last_modified_time, targeting_translation = :targeting_translation, targeting_source_type = :targeting_source_type, share_from_account_id = :share_from_account_id, share_from_targeting_id = :share_from_targeting_id, syn_modify_time = NOW()", 
                        items.iter().map(|p| params! {
                            "targeting_id" => &p.targeting_id, 
                            "account_id" => &account_id, 
                            "is_deleted" => &p.is_deleted,
                            "targeting" => &p.targeting,
                            "targeting_source_type" => &p.targeting_source_type,
                            "description" => &p.description,
                            "last_modified_time" => &p.last_modified_time,
                            "targeting_translation" => &p.targeting_translation,
                            "targeting_name" => &p.targeting_name,
                            "share_from_targeting_id" => &p.share_from_targeting_id,
                            "created_time" => &p.created_time,
                            "share_from_account_id" => &p.share_from_account_id,
                        })
                    )?;
                }
                return Ok(());
            }
            ApiRes { code, message, .. } => {
                gdt_wait(code, &mut token, account_id, media_id).await;
                return Err(Error::Api {
                    code,
                    message: message.unwrap_or_default(),
                });
            }
        }
    }
    Err(Error::Custom("Failed To Get Token".to_string()))
}

async fn target_sync(verifies: Vec<Verify>) {
    let limiter = rate_limiter(5);
    let mut handles = vec![];
    for verify in verifies.into_iter() {
        let limiter = Arc::clone(&limiter);
        handles.push(tokio::spawn(async move {
            let rt = target_api(verify.clone(), &limiter).await;
            let _rt = verify_rt(&verify, rt);
        }));
    }
    tasks_handle(handles).await;
}

#[retry]
async fn audience_api(verify: Verify, limiter: &Limiter) -> Result<()> {
    let url = &verify.url;
    let account_id = verify.account_id;
    let media_id = verify.media_id;
    let token = account_token(account_id, media_id).await;
    if let Some(mut token) = token {
        let params = gdt_params(&token);
        until_ready(limiter).await;
        let res = HTTP_CLIENT.post(url).query(&params).send().await?;
        let res_parsed = res.json::<ApiRes<ApiData<GdtAudience>>>().await?;
        match res_parsed {
            ApiRes {
                code: 0,
                data: Some(ApiData {
                    list: Some(items), ..
                }),
                ..
            } => {
                if !items.is_empty() {
                    let mut con = TIDB_POOL.get_conn()?;
                    con.exec_batch("INSERT INTO `tx_audience_simple` (`account_id`, `platform_id`, `audience_id`, `onwer_id`, `name`, `external_audience_id`, `description`, `type`, `error_code`, `user_count`, `audience_spec`, `status`, `created_time`, `last_modified_time`, `syn_create_time`, source) VALUES (:account_id, :platform_id, :audience_id, :onwer_id, :name, :external_audience_id, :description, :type, :error_code, :user_count, :audience_spec, :status, :created_time, :last_modified_time, NOW(), :source) ON DUPLICATE KEY UPDATE onwer_id = :onwer_id, name = :name, external_audience_id = :external_audience_id, description = :description, type = :type, error_code = :error_code, user_count = :user_count, audience_spec = :audience_spec, status = :status, last_modified_time = :last_modified_time, syn_modify_time = NOW()", 
                        items.iter().map(|p| params! {
                            "account_id" => &account_id, 
                            "created_time" => &p.created_time,
                            "name" => &p.name,
                            "external_audience_id" => &p.external_audience_id,
                            "onwer_id" => &p.account_id,
                            "error_code" => &p.error_code,
                            "audience_id" => &p.audience_id,
                            "user_count" => &p.user_count,
                            "source" => &p.source,
                            "type" => &p.audience_type,
                            "audience_spec" => &p.audience_spec,
                            "platform_id" => 2,
                            "status" => &p.status,
                            "last_modified_time" => &p.last_modified_time,
                            "description" => &p.description,
                        })
                    )?;
                }
                return Ok(());
            }
            ApiRes { code, message, .. } => {
                gdt_wait(code, &mut token, account_id, media_id).await;
                return Err(Error::Api {
                    code,
                    message: message.unwrap_or_default(),
                });
            }
        }
    }
    Err(Error::Custom("Failed To Get Token".to_string()))
}

async fn audience_sync(verifies: Vec<Verify>) {
    let limiter = rate_limiter(5);
    let mut handles = vec![];
    for verify in verifies.into_iter() {
        let limiter = Arc::clone(&limiter);
        handles.push(tokio::spawn(async move {
            let rt = audience_api(verify.clone(), &limiter).await;
            let _rt = verify_rt(&verify, rt);
        }));
    }
    tasks_handle(handles).await;
}

#[retry]
async fn balance_api(verify: Verify, limiter: &Limiter) -> Result<()> {
    let url = &verify.url;
    let account_id = verify.account_id;
    let media_id = verify.media_id;
    let token = account_token(account_id, media_id).await;
    if let Some(mut token) = token {
        let params = gdt_params(&token);
        until_ready(limiter).await;
        let res = HTTP_CLIENT.get(url).query(&params).send().await?;
        let res_parsed = res.json::<ApiRes<ApiData<GdtBalance>>>().await?;
        match res_parsed {
            ApiRes {
                code: 0,
                data: Some(ApiData {
                    list: Some(items), ..
                }),
                ..
            } => {
                if !items.is_empty() {
                    let mut con = TIDB_POOL.get_conn()?;
                    con.exec_batch("INSERT INTO synrpt_gdt_advertiser_ab (`Report_Date`, `account_id`, `fund_type`, `balance`, `fund_status`, `realtime_cost`, `sync_time`) VALUES (CURDATE(), :account_id, :fund_type, :balance, :fund_status, :realtime_cost, NOW()) ON DUPLICATE KEY UPDATE sync_time = NOW(), fund_status = :fund_status, balance = :balance, realtime_cost = :realtime_cost", 
                        items.iter().map(|p| params! {
                            "account_id" => &account_id,
                            "fund_type" => &p.fund_type,
                            "balance" => &p.balance,
                            "fund_status" => &p.fund_status,
                            "realtime_cost" => &p.realtime_cost,
                        }
                    ))?;
                }
                return Ok(());
            }
            ApiRes { code, message, .. } => {
                gdt_wait(code, &mut token, account_id, media_id).await;
                return Err(Error::Api {
                    code,
                    message: message.unwrap_or_default(),
                });
            }
        }
    }
    Err(Error::Custom("Failed To Get Token".to_string()))
}

async fn balance_sync(verifies: Vec<Verify>) {
    let limiter = rate_limiter(5);
    let mut handles = vec![];
    for verify in verifies.into_iter() {
        let limiter = Arc::clone(&limiter);
        handles.push(tokio::spawn(async move {
            let rt = balance_api(verify.clone(), &limiter).await;
            let _rt = verify_rt(&verify, rt);
        }));
    }
    tasks_handle(handles).await;
}
