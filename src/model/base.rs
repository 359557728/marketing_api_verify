use ::serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EmarTokenRt {
    pub code: i32,
    pub access_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Pagination {
    pub page: u64,
    pub page_size: i64,
    pub total_number: u64,
    pub total_page: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiRes<T> {
    pub code: i32,
    pub message: Option<String>,
    pub message_cn: Option<String>,
    pub data: Option<T>,
    pub request_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiData<T> {
    pub list: Option<Vec<T>>,
    pub page_info: Option<Pagination>,
    pub cursor_info: Option<CursorDetail>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CursorDetail {
    pub total_number: Option<u64>,
    pub has_more: bool,
    pub count: u64,
    pub cursor: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TtApiCustomRes<T> {
    pub rows: Option<Vec<T>>,
    pub page_info: Option<Pagination>,
}
