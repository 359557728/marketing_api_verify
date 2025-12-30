use ::serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TtBalance {
    pub search_grant: Option<f64>,
    pub valid_balance: Option<f64>,
    pub return_goods_abs: Option<f64>,
    pub email: Option<String>,
    pub valid_cash: Option<f64>,
    pub common_grant: Option<f64>,
    pub advertiser_id: Option<u64>,
    pub grant: Option<f64>,
    pub valid_grant: Option<f64>,
    pub name: Option<String>,
    pub cash: Option<f64>,
    pub balance: Option<f64>,
    pub default_grant: Option<f64>,
    pub return_goods_cost: Option<f64>,
    pub valid_return_goods_abs: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TtBudget {
    pub advertiser_id: u64,
    pub budget: f64,
    pub budget_mode: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GdtBalance {
    pub fund_type: Option<String>,
    pub balance: Option<i64>,
    pub realtime_cost: Option<i64>,
    pub fund_status: Option<String>,
}
