use ::serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct GdtAdvertiser {
    pub account_id: Option<u64>,
    pub daily_budget: Option<i64>,
    pub system_status: Option<String>,
    pub reject_message: Option<String>,
    pub corporation_name: Option<String>,
    pub agency_account_id: Option<u64>,
    pub system_industry_id: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TtAdvertiser {
    pub id: Option<u64>,
    pub company: Option<String>,
    pub name: Option<String>,
    pub first_industry_name: Option<String>,
    pub second_industry_name: Option<String>,
}
