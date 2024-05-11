#[macro_use]
extern crate log;

use std::time::Duration;

use actix_web::{middleware, App, HttpServer};

use dotenv::dotenv;
use tokio::join;
use tokio::time::sleep;

use crate::app::{fetch_consumption_for_interval, fetch_production_for_interval, get_access_token};
use crate::endpoints::{health, post};
use crate::settings::time::{
    get_next_fetch_milliseconds, get_start_stop, get_time_after_duration, get_timezone,
};

mod app;
pub mod authmodels;
mod endpoints;
mod logging;
mod settings;
mod storage;

#[tokio::main]
async fn main() {
    dotenv().ok();

    logging::init_logging();

    info!("WattiVahti Logger starting");
    info!("Using time zone: {}", get_timezone().name());

    let config = settings::config::load_settings(format!("configs/{}.yaml", "production"))
        .expect("Failed to load settings file.");

    if let Err(err) = config.validate() {
        panic!("Validation error: {}", err);
    }

    let interval: u64 = dotenv::var("INTERVAL")
        .map(|var| var.parse::<u64>())
        .unwrap_or(Ok(3_600_000))
        .unwrap();

    let fetch_pt1h = dotenv::var("FETCH_PT1H_RESOLUTION")
        .map(|var| var.parse::<bool>())
        .unwrap_or(Ok(false))
        .unwrap();

    let fetch_pt15m = dotenv::var("FETCH_PT15M_RESOLUTION")
        .map(|var| var.parse::<bool>())
        .unwrap_or(Ok(false))
        .unwrap();

    let mut access_token = dotenv::var("ACCESS_TOKEN").unwrap_or("".to_string());
    let wattivahti_username = dotenv::var("WATTIVAHTI_USERNAME").unwrap_or("".to_string());
    let wattivahti_password = dotenv::var("WATTIVAHTI_PASSWORD").unwrap_or("".to_string());
    let wattivahti_token_endpoint =
        dotenv::var("WATTIVAHTI_TOKEN_ENDPOINT").unwrap_or("".to_string());
    let start = dotenv::var("START").unwrap();
    let stop = dotenv::var("STOP").unwrap();

    let run_server: bool = dotenv::var("ENABLE_REST_API")
        .unwrap_or_else(|_| String::from("false"))
        .parse()
        .unwrap_or(true);

    let run_update: bool = dotenv::var("ENABLE_AUTO_UPDATE")
        .unwrap_or_else(|_| String::from("false"))
        .parse()
        .unwrap_or(true);

    let server_task = async {
        let server = match HttpServer::new(move || {
            App::new()
                .wrap(middleware::Compress::default())
                // .app_data(web::Data::new(server_client.clone()))
                // register HTTP requests handlers
                .service(health::health_check)
                .service(post::metering_update)
        })
        .bind("0.0.0.0:9090")
        {
            Ok(value) => {
                info!("REST API started at 0.0.0.0:9090");
                value
            }
            Err(error) => panic!("Error binding to socket:{:?}", error),
        };
        let _ = server.run().await;
    };

    let update_task = async {
        loop {
            // If credentials provided, use those instead of the given access token (if access token was even given)
            if !wattivahti_username.is_empty() {
                let result = get_access_token(
                    &wattivahti_token_endpoint,
                    &wattivahti_username,
                    &wattivahti_password,
                )
                .await;
                if result.is_err() {
                    warn!("Logging {} - {} failed because of accessToken, waiting for the next fetch at {} ...", start, stop, get_time_after_duration(interval));
                    sleep(Duration::from_millis(interval)).await;
                    continue;
                }
                let result = result.unwrap();
                if result.access_token.is_none() {
                    warn!("Logging {} - {} failed because of accessToken, waiting for the next fetch at {} ...", start, stop, get_time_after_duration(interval));
                    sleep(Duration::from_millis(interval)).await;
                    continue;
                }
                access_token = result.access_token.unwrap();
            }

            let start_stop = get_start_stop();

            if fetch_pt1h {
                let _ = fetch_consumption_for_interval(
                    &access_token,
                    &start_stop.0,
                    &start_stop.1,
                    "PT1H",
                )
                .await;

                let _ = fetch_production_for_interval(
                    &access_token,
                    &start_stop.0,
                    &start_stop.1,
                    "PT1H",
                )
                .await;
            }

            if fetch_pt15m {
                let _ = fetch_consumption_for_interval(
                    &access_token,
                    &start_stop.0,
                    &start_stop.1,
                    "PT15MIN",
                )
                .await;

                let _ = fetch_production_for_interval(
                    &access_token,
                    &start_stop.0,
                    &start_stop.1,
                    "PT15MIN",
                )
                .await;
            }

            let next_fetch_interval = get_next_fetch_milliseconds() as u64;
            info!(
                "Logging {} - {} done, waiting for the next fetch at {} ...",
                start_stop.0,
                start_stop.1,
                get_time_after_duration(next_fetch_interval)
            );
            sleep(Duration::from_millis(next_fetch_interval)).await;
        }
    };

    if run_server && run_update {
        info!("Running server and auto update");
        join!(server_task, update_task);
    } else if run_server {
        info!("Running server");
        server_task.await;
    } else if run_update {
        info!("Running auto update");
        update_task.await;
    } else {
        warn!("Not running server or update. Enable at least one of them in .env file with ENABLE_REST_API or ENABLE_AUTO_UPDATE.");
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDateTime, Utc};

    use super::*;

    #[tokio::test]
    async fn test_get_start_stop() {
        let data = get_start_stop();
        info!("Result: {} - {}", data.0, data.1);
    }

    #[tokio::test]
    async fn test_get_time_after_duration() {
        let data = get_time_after_duration(21_600_000);
        info!("Result: {}", data);
    }

    #[tokio::test]
    async fn test_get_next_fetch_milliseconds() {
        let data = get_next_fetch_milliseconds();
        info!("Result: {}", data);
    }

    #[tokio::test]
    async fn test_get_consumption_energy_fee() {
        let config = settings::config::load_settings(format!("configs/{}.yaml", "test"))
            .expect("Failed to load settings file.");

        let dt = DateTime::<Utc>::from_utc(
            NaiveDateTime::parse_from_str("2014-05-31T22:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(),
            Utc,
        );
        let contract = config.consumption.get_contract(dt).unwrap();
        let energy_fee = contract.get_energy_fee(1.23, dt);
        info!("Result: {}", energy_fee);
    }
}
