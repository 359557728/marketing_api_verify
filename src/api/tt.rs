use std::collections::HashMap;
use std::sync::Arc;

use crate::model::advertiser::TtAdvertiser;
use crate::model::balance::{TtBalance, TtBudget};
use crate::model::base::{ApiData, ApiRes, TtApiCustomRes};
use crate::model::error::{Error, Result};
use crate::model::message::Verify;
use crate::model::project::Project;
use crate::model::promotion::Promotion;
use crate::model::report as rt;
use crate::model::rta::TtRtaInfo;
use crate::share::common::{
    CORE_POOL, EMPTY, HTTP_CLIENT, Limiter, TIDB_POOL, TT_ADVERTISER_HOUR_DIMENSION,
    TT_ADVERTISER_HOURLY_REPORT_SQL, TT_PROJECT_FILED, TT_PROJECT_HOUR_DIMENSION,
    TT_PROJECT_HOURLY_REPORT_SQL, TT_PROJECT_REPORT_FILED, TT_PROMOTION_HOUR_DIMENSION,
    TT_PROMOTION_HOURLY_REPORT_SQL, account_token, construct_headers, rate_limiter, tasks_handle,
    tt_wait, until_ready, verify_rt,
};
use log::info;
use mysql::prelude::Queryable;
use mysql::*;
use retry_macro::retry;
use serde_json::json;

pub async fn sync((cate, items): (String, Vec<Verify>)) -> Result<()> {
    match cate.as_str() {
        "project" => {
            project_sync(items).await;
        }
        "promotion" => {
            promotion_sync(items).await;
        }
        "balance" => {
            balance_sync(items).await;
        }
        "budget" => {
            budget_sync(items).await;
        }
        "rta_info" => {
            rta_sync(items).await;
        }
        "account" => {
            account_sync(items).await;
        }
        "advertiser_hourly_report" => {
            report_sync(&cate, items).await;
        }
        "project_hourly_report" => {
            report_sync(&cate, items).await;
        }
        "promotion_hourly_report" => {
            report_sync(&cate, items).await;
        }
        _ => {
            info!("Unknown Category: {}", cate);
        }
    }
    Ok(())
}

#[retry]
async fn rta_api(verify: Verify, limiter: &Limiter) -> Result<()> {
    let url = &verify.url;
    let account_id = verify.account_id;
    let media_id = verify.media_id;
    let token = account_token(account_id, media_id).await;
    if let Some(mut token) = token {
        until_ready(limiter).await;
        let res = HTTP_CLIENT
            .get(url)
            .headers(construct_headers(&token))
            .send()
            .await?;
        let res_parsed = res.json::<ApiRes<TtRtaInfo>>().await?;
        match res_parsed {
            ApiRes {
                code: 0,
                data:
                    Some(TtRtaInfo {
                        rta_info,
                        interface_info,
                    }),
                ..
            } => {
                if (rta_info.is_some() && rta_info != Some(json!({})))
                    || (interface_info.is_some() && interface_info != Some(json!({})))
                {
                    let mut con = TIDB_POOL.get_conn()?;
                    con.exec_drop("INSERT INTO `synads_tt_rta_info` (`advertiser_id`, `interface_info`, `rta_info`) VALUES (:advertiser_id, :interface_info, :rta_info) ON DUPLICATE KEY UPDATE interface_info = :interface_info, rta_info = :rta_info, sync_time = NOW();", params! {"rta_info" => &rta_info, "interface_info" => &interface_info, "advertiser_id" => &account_id})?;
                }
                return Ok(());
            }
            ApiRes { code, message, .. } => {
                tt_wait(code, &mut token, account_id, media_id).await;
                return Err(Error::Api {
                    code,
                    message: message.unwrap_or_default(),
                });
            }
        }
    }
    Err(Error::Custom("Failed To Get Token".to_string()))
}

async fn rta_sync(verifies: Vec<Verify>) {
    let limiter = rate_limiter(5);
    let mut handles = vec![];
    for verify in verifies.into_iter() {
        let limiter = Arc::clone(&limiter);
        handles.push(tokio::spawn(async move {
            let rt = rta_api(verify.clone(), &limiter).await;
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
        until_ready(limiter).await;
        let res = HTTP_CLIENT
            .get(url)
            .headers(construct_headers(&token))
            .send()
            .await?;
        let res_parsed = res.json::<ApiRes<TtBalance>>().await?;
        match res_parsed {
            ApiRes { code: 0, data, .. } => {
                if let Some(p) = data {
                    let mut con = TIDB_POOL.get_conn()?;
                    con.exec_drop("INSERT INTO `synrpt_tt_advertiser_ab` (`Report_Date`, `advertiser_id`, `advertiser_name`, `advertiser_email`, `balance`, `valid_balance`, `cash`, `valid_cash`, `tgrant`, `valid_grant`, `return_goods_abs`, `valid_return_goods_abs`, `return_goods_cost`, `Synch_Last_Time`) VALUES (CURDATE(), :advertiser_id, :name, :email, :balance, :valid_balance, :cash, :valid_cash, :grant, :valid_grant, :return_goods_abs, :valid_return_goods_abs, :return_goods_cost, NOW()) ON DUPLICATE KEY UPDATE advertiser_name = :name, advertiser_email = :email, balance = :balance, valid_balance = :valid_balance, cash = :cash, valid_cash = :valid_cash, tgrant = :grant, valid_grant = :valid_grant, return_goods_abs = :return_goods_abs, valid_return_goods_abs = :valid_return_goods_abs, return_goods_cost = :return_goods_cost, Synch_Last_Time = NOW()", 
                        params! {
                            "advertiser_id" => &p.advertiser_id,
                            "email" => &p.email,
                            "name" => &p.name,
                            "balance" => &p.balance,
                            "valid_balance" => &p.valid_balance,
                            "cash" => &p.cash,
                            "valid_cash" => &p.valid_cash,
                            "grant" => &p.grant,
                            "valid_grant" => &p.valid_grant,
                            "return_goods_abs" => &p.return_goods_abs,
                            "valid_return_goods_abs" => &p.valid_return_goods_abs,
                            "return_goods_cost" => &p.return_goods_cost,
                        }
                    )?;
                }
                return Ok(());
            }
            ApiRes { code, message, .. } => {
                tt_wait(code, &mut token, account_id, media_id).await;
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

#[retry]
async fn budget_api(verify: Verify, limiter: &Limiter) -> Result<()> {
    let url = &verify.url;
    let account_id = verify.account_id;
    let media_id = verify.media_id;
    let token = account_token(account_id, media_id).await;
    if let Some(mut token) = token {
        until_ready(limiter).await;
        let res = HTTP_CLIENT
            .get(url)
            .headers(construct_headers(&token))
            .send()
            .await?;
        let res_parsed = res.json::<ApiRes<ApiData<TtBudget>>>().await?;
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
                    con.exec_batch("INSERT INTO tt_advertiser_budget(date, budget, budget_mode, advertiser_id, update_time) VALUES (DATE_FORMAT(CURDATE(), '%Y-%m-%d'), :budget, :budget_mode, :advertiser_id, NOW()) ON DUPLICATE KEY UPDATE budget = :budget, budget_mode = :budget_mode, update_time = NOW()",
                        items.iter().map(|p| {
                            params! {
                                "advertiser_id" => p.advertiser_id,
                                "budget_mode" => &p.budget_mode,
                                "budget" => &p.budget,
                            }
                        }),
                    )?;
                }
                return Ok(());
            }
            ApiRes { code, message, .. } => {
                tt_wait(code, &mut token, account_id, media_id).await;
                return Err(Error::Api {
                    code,
                    message: message.unwrap_or_default(),
                });
            }
        }
    }
    Err(Error::Custom("Failed To Get Token".to_string()))
}

async fn budget_sync(verifies: Vec<Verify>) {
    let limiter = rate_limiter(5);
    let mut handles = vec![];
    for verify in verifies.into_iter() {
        let limiter = Arc::clone(&limiter);
        handles.push(tokio::spawn(async move {
            let rt = budget_api(verify.clone(), &limiter).await;
            let _rt = verify_rt(&verify, rt);
        }));
    }
    tasks_handle(handles).await;
}

#[retry]
async fn project_api(verify: Verify, limiter: &Limiter) -> Result<()> {
    let url = &verify.url;
    let account_id = verify.account_id;
    let media_id = verify.media_id;
    let token = account_token(account_id, media_id).await;
    if let Some(mut token) = token {
        let mut params = HashMap::new();
        params.insert("fields", TT_PROJECT_FILED.to_string());
        until_ready(limiter).await;
        let res = HTTP_CLIENT
            .get(url)
            .headers(construct_headers(&token))
            .query(&params)
            .send()
            .await?;
        let res_parsed = res.json::<ApiRes<ApiData<Project>>>().await?;
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
                    con.exec_batch("INSERT INTO `synads_tt_project` (`project_id`, `advertiser_id`, `delivery_mode`, `landing_type`, `app_promotion_type`, `marketing_goal`, `ad_type`, `opt_status`, `status`, `status_first`, `status_second`, `name`, `project_create_time`, `project_modify_time`, `pricing`, `package_name`, `app_name`, `related_product`, `asset_type`, `download_url`, `download_type`, `download_mode`, `launch_type`, `open_url`, `ulink_url`, `subscribe_url`, `optimize_goal`, `delivery_range`, `audience`, `delivery_setting`, `track_url_setting`, bid, cpa_bid, roi_goal, audience_package_id, deep_cpabid, external_action, deep_external_action) VALUES (:project_id, :advertiser_id, :delivery_mode, :landing_type, :app_promotion_type, :marketing_goal, :ad_type, :opt_status, :status, :status_first, :status_second, :name, :project_create_time, :project_modify_time, :pricing, :package_name, :app_name, :related_product, :asset_type, :download_url, :download_type, :download_mode, :launch_type, :open_url, :ulink_url, :subscribe_url, :optimize_goal, :delivery_range, :audience, :delivery_setting, :track_url_setting, :bid, :cpa_bid, :roi_goal, :audience_package_id, :deep_cpabid, :external_action, :deep_external_action) ON DUPLICATE KEY UPDATE delivery_mode = :delivery_mode, landing_type = :landing_type, name = :name, pricing = :pricing, download_url = :download_url, app_promotion_type = :app_promotion_type, ad_type = :ad_type, download_mode = :download_mode, subscribe_url = :subscribe_url, app_name = :app_name, opt_status = :opt_status, asset_type = :asset_type, open_url = :open_url, ulink_url = :ulink_url, audience = :audience, delivery_setting = :delivery_setting, status = :status, status_first = :status_first, status_second = :status_second, project_modify_time = :project_modify_time, package_name = :package_name, track_url_setting = :track_url_setting, related_product = :related_product, optimize_goal = :optimize_goal, delivery_range = :delivery_range, launch_type = :launch_type, download_type = :download_type, project_create_time = :project_create_time, marketing_goal = :marketing_goal, bid = VALUES(bid), cpa_bid = VALUES(cpa_bid), roi_goal = VALUES(roi_goal), audience_package_id = VALUES(audience_package_id), deep_cpabid = VALUES(deep_cpabid), external_action = :external_action, deep_external_action = :deep_external_action, sync_time = NOW()",
                        items.iter().map(|p| params! {
                            "app_name" => &p.app_name,
                            "opt_status" => &p.opt_status,
                            "status" => &p.status,
                            "status_first" => &p.status_first,
                            "status_second" => &p.status_second,
                            "asset_type" => &p.asset_type,
                            "delivery_setting" => &p.delivery_setting,
                            "ulink_url" => &p.ulink_url,
                            "audience" => &p.audience,
                            "open_url" => &p.open_url,
                            "project_id" => &p.project_id,
                            "delivery_mode" => &p.delivery_mode,
                            "download_mode" => &p.download_mode,
                            "subscribe_url" => &p.subscribe_url,
                            "app_promotion_type" => &p.app_promotion_type,
                            "ad_type" => &p.ad_type,
                            "landing_type" => &p.landing_type,
                            "download_url" => &p.download_url,
                            "name" => &p.name,
                            "pricing" => &p.pricing,
                            "advertiser_id" => &p.advertiser_id,
                            "download_type" => &p.download_type,
                            "launch_type" => &p.launch_type,
                            "optimize_goal" => &p.optimize_goal,
                            "track_url_setting" => &p.track_url_setting,
                            "project_create_time" => &p.project_create_time,
                            "delivery_range" => &p.delivery_range,
                            "marketing_goal" => &p.marketing_goal,
                            "related_product" => &p.related_product,
                            "project_modify_time" => &p.project_modify_time,
                            "package_name" => &p.package_name,
                            "external_action" => &p.external_action(),
                            "deep_external_action" => &p.deep_external_action(),
                            "bid" => &p.bid(),
                            "cpa_bid" => &p.cpa_bid(),
                            "roi_goal" => &p.roi_goal(),
                            "deep_cpabid" => &p.deep_cpabid(),
                            "audience_package_id" => &p.audience_package_id(),
                            "audience_extend" => &p.audience_extend,
                        })
                    )?;
                }
                return Ok(());
            }
            ApiRes { code, message, .. } => {
                tt_wait(code, &mut token, account_id, media_id).await;
                return Err(Error::Api {
                    code,
                    message: message.unwrap_or_default(),
                });
            }
        }
    }
    Err(Error::Custom("Failed To Get Token".to_string()))
}

async fn project_sync(verifies: Vec<Verify>) {
    let limiter = rate_limiter(10);
    let mut handles = vec![];
    for verify in verifies.into_iter() {
        let limiter = Arc::clone(&limiter);
        handles.push(tokio::spawn(async move {
            let rt = project_api(verify.clone(), &limiter).await;
            let _rt = verify_rt(&verify, rt);
        }));
    }
    tasks_handle(handles).await;
}

#[retry]
async fn promotion_api(verify: Verify, limiter: &Limiter) -> Result<()> {
    let url = &verify.url;
    let account_id = verify.account_id;
    let media_id = verify.media_id;
    let token = account_token(account_id, media_id).await;
    if let Some(mut token) = token {
        until_ready(limiter).await;
        let res = HTTP_CLIENT
            .get(url)
            .headers(construct_headers(&token))
            .send()
            .await?;
        let res_parsed = res.json::<ApiRes<ApiData<Promotion>>>().await?;
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
                    con.exec_batch("INSERT INTO `synads_tt_promotion` (`project_id`, `advertiser_id`, `promotion_id`, `promotion_name`, `promotion_create_time`, `promotion_modify_time`, `status`, `status_first`, `status_second`, `opt_status`, `promotion_materials`, `source`, `budget`, `cpa_bid`, `deep_cpabid`, `roi_goal`, `native_setting`, `bid`, `creative_auto_generate_switch`, `config_id`, `learning_phase`, `is_comment_disable`, `ad_download_status`, `brand_info`, `materials_type`, `schedule_time`, `budget_mode`, `external_url_material`, `mini_program_info_url`, `title_in_title_material`) VALUES (:project_id, :advertiser_id, :promotion_id, :promotion_name, :promotion_create_time, :promotion_modify_time, :status, :status_first, :status_second, :opt_status, :promotion_materials, :source, :budget, :cpa_bid, :deep_cpabid, :roi_goal, :native_setting, :bid, :creative_auto_generate_switch, :config_id, :learning_phase, :is_comment_disable, :ad_download_status, :brand_info, :materials_type, :schedule_time, :budget_mode, JSON_UNQUOTE(JSON_EXTRACT(:promotion_materials, '$.external_url_material_list[0]')), JSON_UNQUOTE(JSON_EXTRACT(:promotion_materials, '$.mini_program_info.url')), JSON_UNQUOTE(JSON_EXTRACT(:promotion_materials, '$.title_material_list[*].title'))) ON DUPLICATE KEY UPDATE cpa_bid = :cpa_bid, promotion_create_time = :promotion_create_time, roi_goal = :roi_goal, budget = :budget,source = :source, opt_status = :opt_status, promotion_materials = :promotion_materials, deep_cpabid = :deep_cpabid, promotion_modify_time = :promotion_modify_time,promotion_name = :promotion_name,status = :status, status_first = :status_first, status_second = :status_second,`native_setting` = :native_setting, `bid` = :bid, `creative_auto_generate_switch` = :creative_auto_generate_switch, `config_id` = :config_id, `learning_phase` = :learning_phase, `is_comment_disable` = :is_comment_disable, `ad_download_status` = :ad_download_status, brand_info= :brand_info, materials_type = :materials_type, schedule_time = :schedule_time, budget_mode = :budget_mode, `external_url_material` = VALUES(external_url_material), `mini_program_info_url` = VALUES(mini_program_info_url), `title_in_title_material` = VALUES(title_in_title_material), sync_time = NOW()", 
                        items.iter().filter(|it| it.advertiser_id.is_some()).map(|p| params! {
                            "project_id" => &p.project_id,
                            "advertiser_id" => &p.advertiser_id,
                            "promotion_id" => &p.promotion_id,
                            "promotion_name" => &p.promotion_name,
                            "status" => &p.status,
                            "status_first" => &p.status_first,
                            "status_second" => &p.status_second,
                            "roi_goal" => &p.roi_goal,
                            "deep_cpabid" => &p.deep_cpabid,
                            "promotion_materials" => &p.promotion_materials,
                            "promotion_modify_time" => &p.promotion_modify_time,
                            "promotion_create_time" => &p.promotion_create_time,
                            "cpa_bid" => &p.cpa_bid,
                            "budget" => &p.budget,
                            "budget_mode" => &p.budget_mode,
                            "opt_status" => &p.opt_status,
                            "source" => &p.source,
                            "native_setting" => &p.native_setting,
                            "bid" => &p.bid,
                            "creative_auto_generate_switch" => &p.creative_auto_generate_switch,
                            "config_id" => &p.config_id,
                            "learning_phase" => &p.learning_phase,
                            "is_comment_disable" => &p.is_comment_disable,
                            "ad_download_status" => &p.ad_download_status,
                            "brand_info" => &p.brand_info,
                            "materials_type" => &p.materials_type,
                            "schedule_time" => &p.schedule_time,
                        })
                    )?;
                }
                return Ok(());
            }
            ApiRes { code, message, .. } => {
                tt_wait(code, &mut token, account_id, media_id).await;
                return Err(Error::Api {
                    code,
                    message: message.unwrap_or_default(),
                });
            }
        }
    }
    Err(Error::Custom("Failed To Get Token".to_string()))
}

async fn promotion_sync(verifies: Vec<Verify>) {
    let limiter = rate_limiter(10);
    let mut handles = vec![];
    for verify in verifies.into_iter() {
        let limiter = Arc::clone(&limiter);
        handles.push(tokio::spawn(async move {
            let rt = promotion_api(verify.clone(), &limiter).await;
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
    let res = HTTP_CLIENT
        .get(url)
        .headers(construct_headers(&token))
        .query(&params)
        .send()
        .await?;
    let res_parsed = res
        .json::<ApiRes<TtApiCustomRes<rt::TtCustomReport>>>()
        .await?;
    match res_parsed {
        ApiRes {
            code: 0,
            data:
                Some(TtApiCustomRes {
                    rows: Some(items),
                    page_info: Some(page_info),
                    ..
                }),
            ..
        } => {
            if !items.is_empty() {
                let mut con = TIDB_POOL.get_conn()?;
                let items = items
                    .into_iter()
                    .map(|sg| sg.flat())
                    .collect::<Vec<rt::TtFlat>>();
                con.exec_batch(
                    sql,
                    items.iter().map(|p| {
                        params! {
                            "advertiser_id" => &account_id,
                            "date" => &p.date,
                            "hour" => &p.hour,
                            "project_id" => &p.project_id,
                            "promotion_id" => &p.promotion_id,
                            "material_id" => &p.material_id,
                            "image_mode" => &p.image_mode,
                            "stat_cost" => &p.stat_cost,
                            "show_cnt" => &p.show_cnt,
                            "cpm_platform" => &p.cpm_platform,
                            "click_cnt" => &p.click_cnt,
                            "cpc_platform" => &p.cpc_platform,
                            "attribution_convert_cnt" => &p.attribution_convert_cnt,
                            "attribution_convert_cost" => &p.attribution_convert_cost,
                            "attribution_deep_convert_cnt" => &p.attribution_deep_convert_cnt,
                            "attribution_deep_convert_cost" => &p.attribution_deep_convert_cost,
                            "convert_cnt" => &p.convert_cnt,
                            "conversion_cost" => &p.conversion_cost,
                            "deep_convert_cnt" => &p.deep_convert_cnt,
                            "deep_convert_cost" => &p.deep_convert_cost,
                            "click_start_cnt" => &p.click_start_cnt,
                            "download_finish_cnt" => &p.download_finish_cnt,
                            "install_finish_cnt" => &p.install_finish_cnt,
                            "active" => &p.active,
                            "active_cost" => &p.active_cost,
                            "active_register" => &p.active_register,
                            "active_register_cost" => &p.active_register_cost,
                            "game_addiction" => &p.game_addiction,
                            "attribution_next_day_open_cnt" => &p.attribution_next_day_open_cnt,
                            "next_day_open" => &p.next_day_open,
                            "active_pay" => &p.active_pay,
                            "active_pay_cost" => &p.active_pay_cost,
                            "game_pay_count" => &p.game_pay_count,
                            "attribution_game_pay_7d_count" => &p.attribution_game_pay_7d_count,
                            "attribution_active_pay_7d_per_count" => &p.attribution_active_pay_7d_per_count,
                            "in_app_uv" => &p.in_app_uv,
                            "in_app_detail_uv" => &p.in_app_detail_uv,
                            "in_app_cart" => &p.in_app_cart,
                            "in_app_pay" => &p.in_app_pay,
                            "in_app_order" => &p.in_app_order,
                            "attribution_retention_2d_cnt" => &p.attribution_retention_2d_cnt,
                            "attribution_retention_3d_cnt" => &p.attribution_retention_3d_cnt,
                            "attribution_retention_4d_cnt" => &p.attribution_retention_4d_cnt,
                            "attribution_retention_5d_cnt" => &p.attribution_retention_5d_cnt,
                            "attribution_retention_6d_cnt" => &p.attribution_retention_6d_cnt,
                            "attribution_retention_7d_cnt" => &p.attribution_retention_7d_cnt,
                            "attribution_retention_7d_sum_cnt" => &p.attribution_retention_7d_sum_cnt,
                            "attribution_billing_game_pay_7d_count" => &p.attribution_billing_game_pay_7d_count,
                            "attribution_billing_game_in_app_ltv_1day" => &p.attribution_billing_game_in_app_ltv_1day,
                            "attribution_billing_game_in_app_ltv_2days" => &p.attribution_billing_game_in_app_ltv_2days,
                            "attribution_billing_game_in_app_ltv_3days" => &p.attribution_billing_game_in_app_ltv_3days,
                            "attribution_billing_game_in_app_ltv_4days" => &p.attribution_billing_game_in_app_ltv_4days,
                            "attribution_billing_game_in_app_ltv_5days" => &p.attribution_billing_game_in_app_ltv_5days,
                            "attribution_billing_game_in_app_ltv_6days" => &p.attribution_billing_game_in_app_ltv_6days,
                            "attribution_billing_game_in_app_ltv_7days" => &p.attribution_billing_game_in_app_ltv_7days,
                            "attribution_active_pay" => &p.attribution_active_pay,
                            "stat_pay_amount" => &p.stat_pay_amount,
                            "phone" => &p.phone,
                            "form" => &p.form,
                            "form_submit" => &p.form_submit,
                            "map" => &p.map,
                            "button" => &p.button,
                            "view" => &p.view,
                            "download_start" => &p.download_start,
                            "qq" => &p.qq,
                            "lottery" => &p.lottery,
                            "vote" => &p.vote,
                            "message" => &p.message,
                            "redirect" => &p.redirect,
                            "shopping" => &p.shopping,
                            "consult" => &p.consult,
                            "consult_effective" => &p.consult_effective,
                            "phone_confirm" => &p.phone_confirm,
                            "phone_connect" => &p.phone_connect,
                            "phone_effective" => &p.phone_effective,
                            "coupon" => &p.coupon,
                            "coupon_single_page" => &p.coupon_single_page,
                            "redirect_to_shop" => &p.redirect_to_shop,
                            "poi_address_click" => &p.poi_address_click,
                            "poi_collect" => &p.poi_collect,
                            "customer_effective" => &p.customer_effective,
                            "attribution_customer_effective" => &p.attribution_customer_effective,
                            "attribution_clue_pay_succeed" => &p.attribution_clue_pay_succeed,
                            "attribution_clue_interflow" => &p.attribution_clue_interflow,
                            "attribution_clue_high_intention" => &p.attribution_clue_high_intention,
                            "attribution_clue_confirm" => &p.attribution_clue_confirm,
                            "consult_clue" => &p.consult_clue,
                            "attribution_work_wechat_added_count" => &p.attribution_work_wechat_added_count,
                            "attribution_work_wechat_unfriend_count" => &p.attribution_work_wechat_unfriend_count,
                            "attribution_form" => &p.attribution_form,
                            "attribution_clue_connected_count" => &p.attribution_clue_connected_count,
                            "clue_dialed_count" => &p.clue_dialed_count,
                            "clue_connected_30s_count" => &p.clue_connected_30s_count,
                            "clue_connected_average_duration" => &p.clue_connected_average_duration,
                            "attribution_game_in_app_ltv_1day" => &p.attribution_game_in_app_ltv_1day,
                            "attribution_game_in_app_ltv_2days" => &p.attribution_game_in_app_ltv_2days,
                            "attribution_game_in_app_ltv_3days" => &p.attribution_game_in_app_ltv_3days,
                            "attribution_game_in_app_ltv_4days" => &p.attribution_game_in_app_ltv_4days,
                            "attribution_game_in_app_ltv_5days" => &p.attribution_game_in_app_ltv_5days,
                            "attribution_game_in_app_ltv_6days" => &p.attribution_game_in_app_ltv_6days,
                            "attribution_game_in_app_ltv_7days" => &p.attribution_game_in_app_ltv_7days,
                            "attribution_game_in_app_ltv_8days" => &p.attribution_game_in_app_ltv_8days,
                            "attribution_day_active_pay_count" => &p.attribution_day_active_pay_count,
                            "active_pay_intra_day_count" => &p.active_pay_intra_day_count,
                            "attribution_micro_game_0d_ltv" => &p.attribution_micro_game_0d_ltv,
                            "attribution_micro_game_3d_ltv" => &p.attribution_micro_game_3d_ltv,
                            "attribution_micro_game_7d_ltv" => &p.attribution_micro_game_7d_ltv,
                            "loan_completion" => &p.loan_completion,
                            "pre_loan_credit" => &p.pre_loan_credit,
                            "loan_credit" => &p.loan_credit,
                            "loan" => &p.loan,
                            "premium_payment_count" => &p.premium_payment_count,
                            "bankcard_information_count" => &p.bankcard_information_count,
                            "personal_information_count" => &p.personal_information_count,
                            "certification_information_count" => &p.certification_information_count,
                            "open_account_count" => &p.open_account_count,
                            "first_class_count" => &p.first_class_count,
                            "second_class_count" => &p.second_class_count,
                            "unfollow_in_wechat_count" => &p.unfollow_in_wechat_count,
                            "in_wechat_pay_count" => &p.in_wechat_pay_count,
                            "attribution_work_wechat_dialog_count" => &p.attribution_work_wechat_dialog_count,
                            "low_loan_credit_count" => &p.low_loan_credit_count,
                            "high_loan_credit_count" => &p.high_loan_credit_count,
                            "withdraw_m2_count" => &p.withdraw_m2_count,
                            "attribution_conversion_class_count" => &p.attribution_conversion_class_count,
                            "in_app_order_gmv" => &p.in_app_order_gmv,
                            "in_app_pay_gmv" => &p.in_app_pay_gmv,
                            "total_play" => &p.total_play,
                            "play_duration_3s" => &p.play_duration_3s,
                            "valid_play" => &p.valid_play,
                            "valid_play_of_mille" => &p.valid_play_of_mille,
                            "play_25_feed_break" => &p.play_25_feed_break,
                            "play_50_feed_break" => &p.play_50_feed_break,
                            "play_75_feed_break" => &p.play_75_feed_break,
                            "play_99_feed_break" => &p.play_99_feed_break,
                            "average_play_time_per_play" => &p.average_play_time_per_play,
                            "card_show" => &p.card_show,
                            "dy_like" => &p.dy_like,
                            "dy_comment" => &p.dy_comment,
                            "dy_share" => &p.dy_share,
                            "ad_dislike_cnt" => &p.ad_dislike_cnt,
                            "ad_report_cnt" => &p.ad_report_cnt,
                            "ies_challenge_click" => &p.ies_challenge_click,
                            "ies_music_click" => &p.ies_music_click,
                            "location_click" => &p.location_click,
                            "dy_home_visited" => &p.dy_home_visited,
                            "dy_follow" => &p.dy_follow,
                            "message_action" => &p.message_action,
                            "click_landing_page" => &p.click_landing_page,
                            "click_shopwindow" => &p.click_shopwindow,
                            "click_website" => &p.click_website,
                            "click_call_dy" => &p.click_call_dy,
                            "click_download" => &p.click_download,
                            "luban_live_enter_cnt" => &p.luban_live_enter_cnt,
                            "live_watch_one_minute_count" => &p.live_watch_one_minute_count,
                            "luban_live_follow_cnt" => &p.luban_live_follow_cnt,
                            "luban_live_share_cnt" => &p.luban_live_share_cnt,
                            "luban_live_comment_cnt" => &p.luban_live_comment_cnt,
                            "live_component_click_count" => &p.live_component_click_count,
                        }
                    }),
                )?;
            }
            return Ok(page_info.total_page);
        }
        ApiRes { code, message, .. } => {
            tt_wait(code, &mut token, account_id, 4).await;
            return Err(Error::Api {
                code,
                message: message.unwrap_or_default(),
            });
        }
    }
}

pub async fn report_sync(cate: &str, verifies: Vec<Verify>) {
    let dimensions = match cate {
        "advertiser_hourly_report" => TT_ADVERTISER_HOUR_DIMENSION,
        "project_hourly_report" => TT_PROJECT_HOUR_DIMENSION,
        "promotion_hourly_report" => TT_PROMOTION_HOUR_DIMENSION,
        _ => EMPTY,
    };
    let metrics = match cate {
        "advertiser_hourly_report" => TT_PROJECT_REPORT_FILED,
        "project_hourly_report" => TT_PROJECT_REPORT_FILED,
        "promotion_hourly_report" => TT_PROJECT_REPORT_FILED,
        _ => EMPTY,
    };
    let sql = match cate {
        "advertiser_hourly_report" => TT_ADVERTISER_HOURLY_REPORT_SQL,
        "project_hourly_report" => TT_PROJECT_HOURLY_REPORT_SQL,
        "promotion_hourly_report" => TT_PROMOTION_HOURLY_REPORT_SQL,
        _ => EMPTY,
    };
    let order_param = match cate {
        "advertiser_hourly_report" => r#"[{"field":"stat_cost","type":"DESC"}]"#.to_string(),
        "project_hourly_report" => r#"[{"field":"stat_cost","type":"DESC"},{"field":"cdp_project_id","type":"DESC"}]"#.to_string(),
        "promotion_hourly_report" => r#"[{"field":"stat_cost","type":"DESC"},{"field":"cdp_project_id","type":"DESC"},{"field":"cdp_promotion_id","type":"DESC"}]"#.to_string(),
        _ => EMPTY.to_string(),
    };
    let data_topic = match cate {
        "advertiser_hourly_report" => "BASIC_DATA",
        "project_hourly_report" => "BASIC_DATA",
        "promotion_hourly_report" => "BASIC_DATA",
        _ => EMPTY,
    };
    let limiter = rate_limiter(10);
    let mut handles = vec![];
    for verify in verifies {
        let url = &verify.url;
        let account_id = verify.account_id;
        let media_id = verify.media_id;
        let token = account_token(account_id, media_id).await;
        if let Some(token) = token {
            let mut params = HashMap::new();
            params.insert("page_size", String::from("100"));
            params.insert("order_by", order_param.to_string());
            params.insert("advertiser_id", account_id.to_string());
            params.insert("metrics", metrics.to_string());
            params.insert("dimensions", dimensions.to_string());
            params.insert("data_topic", data_topic.to_string());
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
        until_ready(limiter).await;
        let res = HTTP_CLIENT
            .get(url)
            .headers(construct_headers(&token))
            .send()
            .await?;
        let res_parsed = res.json::<ApiRes<Vec<TtAdvertiser>>>().await?;
        match res_parsed {
            ApiRes {
                code: 0,
                data: Some(data),
                ..
            } => {
                if !data.is_empty() {
                    let mut con = CORE_POOL.get_conn()?;
                    let sql = "UPDATE `tt_advertiser` SET update_time = NOW(), first_industry_name = :first_industry_name, second_industry_name = :second_industry_name, name = :name, company = :company, today_granted = 1 WHERE advertiser_id = :advertiser_id";
                    con.exec_batch(
                        sql,
                        data.iter().map(|p| {
                            params! {
                                "advertiser_id" => p.id,
                                "name" => &p.name,
                                "company" => &p.company,
                                "second_industry_name" => &p.second_industry_name,
                                "first_industry_name" => &p.first_industry_name,
                            }
                        }),
                    )?;
                }
                return Ok(());
            }
            ApiRes { code, message, .. } => {
                tt_wait(code, &mut token, account_id, media_id).await;
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
            println!("Account Sync Result: {:?}", rt);
            let _rt = verify_rt(&verify, rt);
        }));
    }
    tasks_handle(handles).await;
}
