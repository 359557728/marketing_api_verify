use std::{collections::HashMap, time::Duration};

use crate::api::gdt as gdt_api;
use crate::api::tt as tt_api;
use crate::model::error::Error;
use crate::model::message::Verify;
use crate::share::common::REDIS_CLUSTER;
use log::error;
use r2d2::PooledConnection;
use redis::Commands;
use redis::cluster::ClusterClient;
use tokio::time::sleep;

const BATCH_SIZE: usize = 1000;

const SLEEP_DURATION: Duration = Duration::from_secs(1);

const REDIS_KEY: &str = "rule_message_thread_rule_verify";

pub async fn route() -> Result<(), Error> {
    let mut conn = REDIS_CLUSTER.get()?;

    loop {
        let verifies = fetch(&mut conn).await?;

        if verifies.is_empty() {
            sleep(SLEEP_DURATION).await;
            continue;
        }

        dispatch(verifies).await?;
    }
}

async fn fetch(conn: &mut PooledConnection<ClusterClient>) -> Result<Vec<String>, Error> {
    let mut items = Vec::with_capacity(200);

    for _ in 0..BATCH_SIZE {
        match conn.lpop(REDIS_KEY, None) {
            Ok(Some(msg)) => items.push(msg),
            Ok(None) => break,
            Err(e) => {
                error!("Redis Error: {}", e);
                break;
            }
        }
    }

    Ok(items)
}

async fn dispatch(verifies: Vec<String>) -> Result<(), Error> {
    let grouped = group(verifies);

    for (cate, verifies) in grouped {
        if let Some((media, cate)) = cate.rsplit_once(':')
            && media.parse::<u8>().is_ok()
        {
            route_by_media(media.parse::<u8>().unwrap(), (cate.to_owned(), verifies)).await?;
        }
    }

    Ok(())
}

fn group(verifies: Vec<String>) -> HashMap<String, Vec<Verify>> {
    let mut grouped: HashMap<String, Vec<Verify>> = HashMap::new();

    for msg in verifies {
        match serde_json::from_str::<Verify>(&msg) {
            Ok(verify) => {
                grouped
                    .entry(format!("{}:{}", verify.media_id, &verify.cate))
                    .or_default()
                    .push(verify);
            }
            Err(e) => {
                error!("Failed To Parse Message: {}", e);
            }
        }
    }

    grouped
}

async fn route_by_media(media_id: u8, payload: (String, Vec<Verify>)) -> Result<(), Error> {
    match media_id {
        2 => gdt_api::sync(payload).await?,
        4 => tt_api::sync(payload).await?,
        _ => error!("Media Id Not Yet Supported: {}", media_id),
    }
    Ok(())
}
