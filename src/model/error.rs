use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Database Error: {0}")]
    Database(#[from] mysql::Error),

    #[error("Redis Error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Redis Pool Error: {0}")]
    RedisPool(#[from] r2d2::Error),

    #[error("HTTP Error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON Error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("API Error {code}: {message}")]
    Api { code: i32, message: String },

    #[error("{0}")]
    Custom(String),
}

pub type Result<T> = std::result::Result<T, Error>;
