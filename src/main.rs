#![recursion_limit = "1024"]
use chrono::Local;
mod api;
mod model;
mod share;
use std::io::Write;

use crate::api::router;

#[tokio::main]
async fn main() {
    let env = env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info");
    env_logger::Builder::from_env(env)
        .target(env_logger::Target::Stdout)
        .format(|buf, record| {
            writeln!(
                buf,
                "{} {} [{}] {}",
                Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.module_path().unwrap_or("<unnamed>"),
                &record.args()
            )
        })
        .init();
    router::route().await.unwrap();
}
