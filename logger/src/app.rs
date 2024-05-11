use api::{get_consumption_data, get_production_data};
use reqwest::StatusCode;

use crate::{
    authmodels::{TokenRequest, TokenResponse},
    settings,
    storage::{
        influxdb::influx::{upsert_consumptions_into_influxdb, upsert_productions_into_influxdb},
        timescaledb::timescale::{
            self, refresh_consumption_views, refresh_production_views, upsert_consumptions_into_timescaledb, upsert_productions_into_timescaledb
        },
    },
};

pub async fn get_access_token(
    endpoint: &str,
    username: &str,
    password: &str,
) -> Result<TokenResponse, anyhow::Error> {
    info!(
        "Fetching a new access_token for WattiVahti user - {}",
        &username
    );

    let res = reqwest::Client::new()
        .post(format!("{}/wattivahti/token", endpoint))
        .json(&TokenRequest {
            username: username.to_string(),
            password: password.to_string(),
        })
        .send()
        .await?;

    let status = res.status();

    let data_str = res.text().await?;
    // info!("{}", data_str);

    if status != StatusCode::OK {
        return Err(anyhow::anyhow!(data_str));
    }

    let data: TokenResponse = serde_json::from_str(&data_str)?;
    // info!("TokenResponse: {:#?}", data);

    Ok(data)
}

pub async fn fetch_production_for_interval(
    access_token: &str,
    start: &str,
    stop: &str,
    resolution: &str,
) -> Result<(), anyhow::Error> {
    let metering_point_code = dotenv::var("PRODUCTION_METERING_POINT_CODE").unwrap();

    info!(
        "Fetching production data for interval {} - {} in metering point {} with resolution {}",
        &start, &stop, &metering_point_code, &resolution
    );

    let config = settings::config::load_settings(format!("configs/{}.yaml", "production"))
        .expect("Failed to load settings file.");

    match get_production_data(&access_token, &metering_point_code, &start, &stop, &resolution).await {
        Ok(data) => {
            let timescale_future = upsert_productions_into_timescaledb(&data, &config);
            let influx_future = upsert_productions_into_influxdb(&data, &config);

            let (timescale_result, influx_result) = tokio::join!(timescale_future, influx_future);

            if timescale_result.is_err() {
                error!("Error inserting into TimescaleDB: {:?}", timescale_result);
            }

            if influx_result.is_err() {
                error!("Error inserting into InfluxDB: {:?}", influx_result);
            }

            if timescale::is_enabled() {
                if let Err(err) = refresh_production_views().await {
                    // Handle the error here
                    error!("Error refreshing the production views: {:?}", err);
                }
            }

            Ok(())
        }
        Err(err) => Err(anyhow::anyhow!(err)),
    }
}

pub async fn fetch_consumption_for_interval(
    access_token: &str,
    start: &str,
    stop: &str,
    resolution: &str,
) -> Result<(), anyhow::Error> {
    let metering_point_code = dotenv::var("CONSUMPTION_METERING_POINT_CODE").unwrap();

    info!(
        "Fetching consumption data for interval {} - {} in metering point {} with resolution {}",
        &start, &stop, &metering_point_code, &resolution
    );

    let config = settings::config::load_settings(format!("configs/{}.yaml", "production"))
        .expect("Failed to load settings file.");

    match get_consumption_data(&access_token, &metering_point_code, &start, &stop, &resolution).await {
        Ok(data) => {
            let timescale_future = upsert_consumptions_into_timescaledb(&data, &config);
            let influx_future = upsert_consumptions_into_influxdb(&data, &config);

            let (timescale_result, influx_result) = tokio::join!(timescale_future, influx_future);

            if timescale_result.is_err() {
                error!("Error inserting into TimescaleDB: {:?}", timescale_result);
            }

            if influx_result.is_err() {
                error!("Error inserting into InfluxDB: {:?}", influx_result);
            }

            if timescale::is_enabled() {
                if let Err(err) = refresh_consumption_views().await {
                    // Handle the error here
                    error!("Error refreshing the consumption views: {:?}", err);
                }
            }

            Ok(())
        }
        Err(err) => Err(anyhow::anyhow!(err)),
    }
}

#[cfg(test)]
mod tests {
    use dotenv::dotenv;

    use super::*;

    #[tokio::test]
    async fn test_get_consumptions_and_productions() {
        dotenv().ok();

        let start = "2020-01-01T00:00:00";
        let stop = "2020-12-31T00:00:00";

        let mut access_token = dotenv::var("ACCESS_TOKEN").unwrap_or("".to_string());
        let wattivahti_username = dotenv::var("WATTIVAHTI_USERNAME").unwrap_or("".to_string());
        let wattivahti_password = dotenv::var("WATTIVAHTI_PASSWORD").unwrap_or("".to_string());
        let wattivahti_token_endpoint =
            dotenv::var("WATTIVAHTI_TOKEN_ENDPOINT").unwrap_or("".to_string());

        if !wattivahti_username.is_empty() {
            let result = get_access_token(
                &wattivahti_token_endpoint,
                &wattivahti_username,
                &wattivahti_password,
            )
            .await;

            if result.is_err() {
                panic!("Logging {} - {} failed because of accessToken", start, stop);
            }
            let result = result.unwrap();
            if result.access_token.is_none() {
                panic!("Logging {} - {} failed because of accessToken", start, stop);
            }
            access_token = result.access_token.unwrap();
        }

        if let Err(err) = fetch_consumption_for_interval(&access_token, &start, &stop, "PT1H").await
        {
            // Handle the error here
            panic!("Error fetching consumptions: {:?}", err);
        }

        if let Err(err) = fetch_production_for_interval(&access_token, &start, &stop, "PT1H").await
        {
            // Handle the error here
            panic!("Error fetching productions: {:?}", err);
        }
    }
}
