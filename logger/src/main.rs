use std::time::Duration;

use api::{SpotData, TSV};
use api::get_consumption_data;
use api::get_production_data;
use chrono::{Datelike, NaiveDateTime};
use chrono::TimeZone;
use chrono::Timelike;
use chrono::{DateTime, Utc};
use chrono_tz::Europe::Helsinki;
use chrono_tz::Tz;
use dotenv::dotenv;
use influxdb::Client;
use influxdb::InfluxDbWriteable;
use influxdb::ReadQuery;
use reqwest::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::sleep;
use actix_web::{middleware, web, App, HttpServer};

use crate::authmodels::TokenRequest;
use crate::authmodels::TokenResponse;
use crate::settings::config_model::SettingsConfig;

pub mod authmodels;
mod metering;
mod settings;

#[derive(Debug, InfluxDbWriteable, Serialize, Deserialize)]
#[allow(non_snake_case)]
struct TimeSeriesValue {
    time: DateTime<Utc>,
    #[influxdb(tag)]
    meteringpointcode_tag: String,
    #[influxdb(tag)]
    measurementtype_tag: String,

    meteringpointcode: String,
    measurementtype: String,
    unit: String,
    timestamp: String,
    value: f32,
    price: f32,

    transfer_basic_fee: Option<f32>,
    transfer_fee: Option<f32>,
    tax_fee: Option<f32>,
    basic_fee: Option<f32>,
    energy_fee: Option<f32>,
}

#[derive(Debug, InfluxDbWriteable, Serialize, Deserialize)]
#[allow(non_snake_case)]
struct PriceData {
    time: DateTime<Utc>,
    #[influxdb(tag)]
    type_tag: String,
    #[influxdb(tag)]
    in_domain_tag: String,
    #[influxdb(tag)]
    out_domain_tag: String,
    document_type: String,
    in_domain: String,
    out_domain: String,
    currency: String,
    price_measure: String,
    curve_type: String,
    timestamp: String,
    price: f32,
    dirty: Option<i32>,
}

async fn fetch_and_log_new_production_entry(
    client: &Client,
    config: &SettingsConfig,
    access_token: &str,
    metering_point_code: &str,
    start: &str,
    stop: &str,
) {
    println!("Logging new production entry for {} at {} - {}", &metering_point_code, &start, &stop);

    match get_production_data(&access_token, &metering_point_code, &start, &stop).await {
        Ok(data) => {
            for (pos, tsv) in data.getconsumptionsresult.consumptiondata.timeseries.values.tsv.iter().enumerate() {
                log_new_production_entry(client, config, &metering_point_code, "6", &data.getconsumptionsresult.consumptiondata.sum.unit, &tsv, pos).await;
            }
        },
        Err(err) => println!("Failed to fetch data | {}", err),
    }
}

async fn fetch_and_log_new_consumption_entry(
    client: &Client,
    config: &SettingsConfig,
    access_token: &str,
    metering_point_code: &str,
    start: &str,
    stop: &str,
) {
    println!("Logging new consumption entry for {} at {} - {}", &metering_point_code, &start, &stop);

    match get_consumption_data(&access_token, &metering_point_code, &start, &stop).await {
        Ok(data) => {
            for (pos, tsv) in data.getconsumptionsresult.consumptiondata.timeseries.values.tsv.iter().enumerate() {
                log_new_consumption_entry(client, config, &metering_point_code, "1", &data.getconsumptionsresult.consumptiondata.sum.unit, &tsv, pos).await;
            }
        },
        Err(err) => println!("Failed to fetch data | {}", err),
    }
}

async fn fetch_and_log_new_spot_data(
    client: &Client,
    config: &SettingsConfig,
    access_token: &str,
    metering_point_code: &str,
    start: &str,
    stop: &str,
) {
    println!("Logging new spot data {}", &metering_point_code);

    match get_consumption_data(&access_token, &metering_point_code, &start, &stop).await {
        Ok(data) => {
            if data.getconsumptionsresult.spotdata.is_some() {
                update_spot_data(client, config, &data.getconsumptionsresult.spotdata.unwrap()).await;
            }
        },
        Err(err) => println!("Failed to fetch data | {}", err),
    }
}

async fn log_new_production_entry(client: &Client,
    config: &SettingsConfig,
    meteringpointcode: &str,
    measurementtype: &str,
    unit: &str,
    time_series_value: &TSV,
    pos: usize) {
        let time = &time_series_value.get_timestamp_utc_calculated(pos);
        if time.is_none() {
            println!("Skipping logging because time couldn't be parsed");
            return;
        }

        if time_series_value.quantity.is_none() {
            // println!("Skipping logging because quantity was null");
            return;
        }

        println!("Logging production UTC: {:?} - {}", time, time_series_value.quantity.unwrap());

        let time = time.unwrap();
        let price = get_day_ahead_price(&client, &time).await;
        let transfer_fee = config.get_production_transfer_fee(time);
        let current_data = TimeSeriesValue {
            time: time,
            meteringpointcode_tag: meteringpointcode.to_string(),
            measurementtype_tag: measurementtype.to_string(),
            meteringpointcode: meteringpointcode.to_string(),
            measurementtype: measurementtype.to_string(),
            unit: unit.to_string(),
            timestamp: time.format("%Y-%m-%dT%H:%M:%S").to_string(),
            value: time_series_value.quantity.unwrap(),
            price: price / 1000.0,

            transfer_basic_fee: None,
            transfer_fee: Some(transfer_fee),
            tax_fee: None,
            basic_fee: None,
            energy_fee: None,
        };

        let write_result = client
            .query(&current_data.into_query("productions"))
            .await;
        if let Err(err) = write_result {
            eprintln!("Error writing to db: {}", err)
        }
}

async fn log_new_consumption_entry(client: &Client,
    config: &SettingsConfig,
    meteringpointcode: &str,
    measurementtype: &str,
    unit: &str,
    time_series_value: &TSV,
    pos: usize) {
        let time = &time_series_value.get_timestamp_utc_calculated(pos);
        if time.is_none() {
            println!("Skipping logging because time couldn't be parsed");
            return;
        }

        if time_series_value.quantity.is_none() {
            // println!("Skipping logging because quantity was null");
            return;
        }

        println!("Logging consumption UTC: {:?} - {}", time, time_series_value.quantity.unwrap());

        let time = time.unwrap();
        let price = get_day_ahead_price(&client, &time).await;

        let transfer_basic_fee = config.get_consumption_transfer_basic_fee(time);
        let transfer_fee = config.get_consumption_transfer_fee(time);
        let tax_fee = config.get_consumption_tax_fee(time);
        let basic_fee = config.get_consumption_basic_fee(time);
        let energy_fee = config.get_consumption_energy_fee(price, time);

        let current_data = TimeSeriesValue {
            time: time,
            meteringpointcode_tag: meteringpointcode.to_string(),
            measurementtype_tag: measurementtype.to_string(),
            meteringpointcode: meteringpointcode.to_string(),
            measurementtype: measurementtype.to_string(),
            unit: unit.to_string(),
            timestamp: time.format("%Y-%m-%dT%H:%M:%S").to_string(),
            value: time_series_value.quantity.unwrap(),
            price: price / 1000.0,

            transfer_basic_fee: Some(transfer_basic_fee),
            transfer_fee: Some(transfer_fee),
            tax_fee: Some(tax_fee),
            basic_fee: Some(basic_fee),
            energy_fee: Some(energy_fee),
        };

        let write_result = client
            .query(&current_data.into_query("consumptions"))
            .await;
        if let Err(err) = write_result {
            eprintln!("Error writing to db: {}", err)
        }
}

async fn get_day_ahead_price(client: &Client, time: &DateTime<Utc>) -> f32 {
    let read_query = ReadQuery::new(format!("SELECT * FROM dayAheadPrices WHERE type_tag='A44' AND time='{}' LIMIT 1", time.to_rfc3339()));

    let read_result = client
        .json_query(read_query)
        .await
        .and_then(|mut db_result| db_result.deserialize_next::<PriceData>());

    match read_result {
        Ok(result) => {
            if result.series.len() > 0 && result.series[0].values.len() > 0
            {
                let data = &result.series[0].values[0];
                return data.price;
            }
        },
        Err(err) => {
            eprintln!("Error reading dayAheadPrices from the db: {}", err);
        }
    }

    0.0
}

async fn has_day_ahead_price(client: &Client, time: &DateTime<Utc>) -> bool {
    let read_query = ReadQuery::new(format!("SELECT * FROM dayAheadPrices WHERE type_tag='A44' AND time='{}' LIMIT 1", time.to_rfc3339()));

    let read_result = client
        .json_query(read_query)
        .await
        .and_then(|mut db_result| db_result.deserialize_next::<PriceData>());

    match read_result {
        Ok(result) => {
            if result.series.len() > 0 && result.series[0].values.len() > 0
            {
                return true;
            }
        },
        Err(err) => {
            eprintln!("Error reading dayAheadPrices from the db: {}", err);
        }
    }

    false
}

async fn update_spot_data(client: &Client, config: &SettingsConfig, spot_data: &SpotData) {
    for (pos, tsv) in spot_data.timeseries.values.tsv.iter().enumerate() {
        let time = tsv.get_timestamp_utc_calculated(pos);
        if time.is_none() {
            println!("Skipping updating spot data because time couldn't be parsed");
        }

        let time = time.unwrap();
        let has_price = has_day_ahead_price(&client, &time).await;
        if !has_price {
            let multiplier = config.get_spot_data_vat_multiplier(time);

            if tsv.quantity.is_none() {
                continue;
            }

            // There's no flag for whether the value is provided or missing, it's 0 in both cases
            let quantity = tsv.quantity.unwrap();
            if quantity >= 0.0 && quantity <= 0.0 {
                break;
            }

            log_new_day_ahead_price(client, &time, quantity / multiplier).await;
        }
    }
}

async fn log_new_day_ahead_price(client: &Client, time: &DateTime<Utc>, price: f32) {
    println!("Logging day ahead price UTC: {:?} - {}", time, price);

    let current_data = PriceData {
        time: *time,
        type_tag: "A44".to_string(),
        in_domain_tag: "10YFI-1--------U".to_string(),
        out_domain_tag: "10YFI-1--------U".to_string(),
        document_type: "A44".to_string(),
        in_domain: "10YFI-1--------U".to_string(),
        out_domain: "10YFI-1--------U".to_string(),
        currency: "EUR".to_string(),
        price_measure: "MWH".to_string(),
        curve_type: "A01".to_string(),
        timestamp: time.format("%Y-%m-%dT%H:%MZ").to_string(),
        price: price,
        dirty: Some(1),
    };

    let write_result = client
        .query(&current_data.into_query("dayAheadPrices"))
        .await;
    if let Err(err) = write_result {
        eprintln!("Error writing to db: {}", err)
    }
}

async fn set_consumption_fees(client: &Client, config: &SettingsConfig, start: &DateTime<Utc>, end: &DateTime<Utc>) {
    let read_query = ReadQuery::new(format!("SELECT * FROM consumptions WHERE time >= '{}' AND time <= '{}'", start.to_rfc3339(), end.to_rfc3339()));

    let read_result = client
        .json_query(read_query)
        .await
        .and_then(|mut db_result| db_result.deserialize_next::<TimeSeriesValue>());

    match read_result {
        Ok(result) => {
            if result.series.len() > 0 && result.series[0].values.len() > 0
            {
                for value in result.series[0].values.iter() {
                    let price = get_day_ahead_price(&client, &value.time).await;

                    let transfer_basic_fee = config.get_consumption_transfer_basic_fee(value.time);
                    let transfer_fee = config.get_consumption_transfer_fee(value.time);
                    let tax_fee = config.get_consumption_tax_fee(value.time);
                    let basic_fee = config.get_consumption_basic_fee(value.time);
                    let energy_fee = config.get_consumption_energy_fee(price, value.time);

                    let data = TimeSeriesValue {
                        time: value.time,
                        meteringpointcode_tag: value.meteringpointcode.to_string(),
                        measurementtype_tag: value.measurementtype.to_string(),
                        meteringpointcode: value.meteringpointcode.to_string(),
                        measurementtype: value.measurementtype.to_string(),
                        unit: value.unit.to_string(),
                        timestamp: value.timestamp.to_string(),
                        value: value.value,
                        price: value.price,

                        transfer_basic_fee: Some(transfer_basic_fee),
                        transfer_fee: Some(transfer_fee),
                        tax_fee: Some(tax_fee),
                        basic_fee: Some(basic_fee),
                        energy_fee: Some(energy_fee),
                    };

                    let write_result = client
                        .query(&data.into_query("consumptions"))
                        .await;
                    if let Err(err) = write_result {
                        eprintln!("Error writing to db: {}", err)
                    }
                }
            }
        },
        Err(err) => {
            eprintln!("Error reading consumptions from the db: {}", err);
        }
    }
}

async fn set_production_fees(client: &Client, config: &SettingsConfig, start: &DateTime<Utc>, end: &DateTime<Utc>) {
    let read_query = ReadQuery::new(format!("SELECT * FROM productions WHERE time >= '{}' AND time <= '{}'", start.to_rfc3339(), end.to_rfc3339()));

    let read_result = client
        .json_query(read_query)
        .await
        .and_then(|mut db_result| db_result.deserialize_next::<TimeSeriesValue>());

    match read_result {
        Ok(result) => {
            if result.series.len() > 0 && result.series[0].values.len() > 0
            {
                for value in result.series[0].values.iter() {
                    let transfer_fee = config.get_production_transfer_fee(value.time);

                    let data = TimeSeriesValue {
                        time: value.time,
                        meteringpointcode_tag: value.meteringpointcode.to_string(),
                        measurementtype_tag: value.measurementtype.to_string(),
                        meteringpointcode: value.meteringpointcode.to_string(),
                        measurementtype: value.measurementtype.to_string(),
                        unit: value.unit.to_string(),
                        timestamp: value.timestamp.to_string(),
                        value: value.value,
                        price: value.price,

                        transfer_basic_fee: None,
                        transfer_fee: Some(transfer_fee),
                        tax_fee: None,
                        basic_fee: None,
                        energy_fee: None,
                    };

                    let write_result = client
                        .query(&data.into_query("productions"))
                        .await;
                    if let Err(err) = write_result {
                        eprintln!("Error writing to db: {}", err)
                    }
                }
            }
        },
        Err(err) => {
            eprintln!("Error reading consumptions from the db: {}", err);
        }
    }
}

fn parse_time_to_utc(time: &str) -> DateTime<Utc> {
    let naive_time = NaiveDateTime::parse_from_str(&time, "%Y-%m-%dT%H:%M:%S");
    if naive_time.is_err() {
        panic!("Invalid time | {}", time)
    }

   Utc.from_utc_datetime(&Helsinki.from_local_datetime(&naive_time.unwrap())
        .unwrap()
        .naive_utc())
}


async fn get_access_token(endpoint: &str, username: &str, password: &str) -> Result<TokenResponse, anyhow::Error> {
    println!("Fetching a new access_token for WattiVahti user - {}", &username);

    let res = reqwest::Client::new()
        .post(format!("{}/wattivahti/token", endpoint))
        .json(&TokenRequest {
            username: username.to_string(),
            password: password.to_string(),
        })
        .send()
        .await?;

    let status = res.status();

    let data_str = res
        .text()
        .await?;
    // println!("{}", data_str);

    if status != StatusCode::OK {
        return Err(anyhow::anyhow!(data_str));
    }

    let data: TokenResponse = serde_json::from_str(&data_str)?;
    // println!("TokenResponse: {:#?}", data);

    Ok(data)
}

fn get_next_fetch_milliseconds() -> i64 {
    let helsinki_now: DateTime<Tz> = Utc::now().with_timezone(&Helsinki);
    let mut next = helsinki_now + chrono::Duration::days(1);

    let fetch_hour: u32 = dotenv::var("FETCH_HOUR")
        .map(|var| var.parse::<u32>())
        .unwrap_or(Ok(6))
        .unwrap();
    let fetch_minutes: u32 = dotenv::var("FETCH_MINUTES")
        .map(|var| var.parse::<u32>())
        .unwrap_or(Ok(0))
        .unwrap();
    next = next.with_hour(fetch_hour).unwrap();
    next = next.with_minute(fetch_minutes).unwrap();
    next = next.with_second(0).unwrap();

    //next.format("%Y-%m-%dT%H:%M:%S").to_string()
    next.timestamp_millis() - helsinki_now.timestamp_millis()
}

fn get_next_spot_fetch_milliseconds() -> i64 {
    let helsinki_now: DateTime<Tz> = Utc::now().with_timezone(&Helsinki);
    let mut next = helsinki_now + chrono::Duration::days(1);

    let fetch_hour: u32 = dotenv::var("SPOT_FETCH_HOUR")
        .map(|var| var.parse::<u32>())
        .unwrap_or(Ok(14))
        .unwrap();
    let fetch_minutes: u32 = dotenv::var("SPOT_FETCH_MINUTES")
        .map(|var| var.parse::<u32>())
        .unwrap_or(Ok(15))
        .unwrap();
    next = next.with_hour(fetch_hour).unwrap();
    next = next.with_minute(fetch_minutes).unwrap();
    next = next.with_second(0).unwrap();

    //next.format("%Y-%m-%dT%H:%M:%S").to_string()
    next.timestamp_millis() - helsinki_now.timestamp_millis()
}

fn get_start_stop() -> (String, String) {
    let helsinki_now: DateTime<Tz> = Utc::now().with_timezone(&Helsinki);

    let start = helsinki_now - chrono::Duration::days(1);

    (start.format("%Y-%m-%dT00:00:00").to_string(), helsinki_now.format("%Y-%m-%dT00:00:00").to_string())
}

fn get_spot_start_stop() -> (String, String) {
    let helsinki_now: DateTime<Tz> = Utc::now().with_timezone(&Helsinki);

    let start = (helsinki_now + chrono::Duration::days(1)).with_day(1).unwrap();

    let mut end = start;
    let month = start.month();
    if month == 12 {
        end = end.with_year(end.year() + 1).unwrap().with_month(1).unwrap();
    } else {
        end = end.with_month(end.month() + 1).unwrap();
    }

    (start.format("%Y-%m-%dT00:00:00").to_string(), end.format("%Y-%m-%dT00:00:00").to_string())
}

fn get_time_after_duration(duration: u64) -> String {
    let helsinki_now: DateTime<Tz> = Utc::now().with_timezone(&Helsinki);
    let time = helsinki_now + chrono::Duration::milliseconds(duration as i64);

    time.format("%Y-%m-%dT%H:%M:%S").to_string()
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let config = settings::config::load_settings(format!("configs/{}.yaml", "production"))
        .expect("Failed to load settings file.");

    let database_url = dotenv::var("DATABASE_URL").unwrap_or("http://localhost:8086".to_string());
    let database_name = dotenv::var("DATABASE_NAME").unwrap_or("wattivahti".to_string());

    let interval: u64 = dotenv::var("INTERVAL")
        .map(|var| var.parse::<u64>())
        .unwrap_or(Ok(3_600_000))
        .unwrap();

    let fees_only: bool = dotenv::var("FEES_ONLY")
        .map(|var| var.parse::<bool>())
        .unwrap_or(Ok(false))
        .unwrap();

    let single_mode: bool = dotenv::var("SINGLE_MODE")
        .map(|var| var.parse::<bool>())
        .unwrap_or(Ok(false))
        .unwrap();

    let spot_data_mode: bool = dotenv::var("SPOT_DATA_MODE")
        .map(|var| var.parse::<bool>())
        .unwrap_or(Ok(false))
        .unwrap();

    let rest_mode: bool = dotenv::var("REST_MODE")
        .map(|var| var.parse::<bool>())
        .unwrap_or(Ok(false))
        .unwrap();

    let mut access_token = dotenv::var("ACCESS_TOKEN").unwrap_or("".to_string());
    let wattivahti_username = dotenv::var("WATTIVAHTI_USERNAME").unwrap_or("".to_string());
    let wattivahti_password = dotenv::var("WATTIVAHTI_PASSWORD").unwrap_or("".to_string());
    let wattivahti_token_endpoint = dotenv::var("WATTIVAHTI_TOKEN_ENDPOINT").unwrap_or("".to_string());
    let consumption_metering_point_code = dotenv::var("CONSUMPTION_METERING_POINT_CODE").unwrap();
    let production_metering_point_code = dotenv::var("PRODUCTION_METERING_POINT_CODE").unwrap();
    let start = dotenv::var("START").unwrap();
    let stop = dotenv::var("STOP").unwrap();

    // Connect to database
    let client = Client::new(database_url, database_name);

    if fees_only {
        let start = parse_time_to_utc(&start);
        let stop = parse_time_to_utc(&stop);

        set_consumption_fees(
            &client,
            &config,
            &start,
            &stop,
        )
        .await;

        set_production_fees(
            &client,
            &config,
            &start,
            &stop,
        )
        .await;

        println!("Fees changed for {} - {}, exiting in {}ms ...", start, stop, interval);
        sleep(Duration::from_millis(interval)).await;
    }
    else if rest_mode {
        let server = match HttpServer::new(move || {
            App::new()
                .wrap(middleware::Compress::default())
                .app_data(web::Data::new(client.clone()))
                // register HTTP requests handlers
                .service(metering::metering_update)
        })
            .bind("0.0.0.0:9090")
        {
            Ok(value) => {
                println!("REST API started at 0.0.0.0:9090");
                value
            },
            Err(error) => panic!("Error binding to socket:{:?}", error),
        };

        let _ = server
            .run()
            .await;

        return;
    }
    else if spot_data_mode {
        loop {
            // If credentials provided, use those instead of the given access token (if access token was even given)
            if !wattivahti_username.is_empty() {
                let result = get_access_token(&wattivahti_token_endpoint, &wattivahti_username, &wattivahti_password).await;
                if result.is_err() {
                    println!("Logging {} - {} failed because of accessToken, waiting for the next fetch in {}ms...", start, stop, interval);
                    sleep(Duration::from_millis(interval)).await;
                }
                let result = result.unwrap();
                if result.access_token.is_none() {
                    println!("Logging {} - {} failed because of accessToken, waiting for the next fetch in {}ms...", start, stop, interval);
                    sleep(Duration::from_millis(interval)).await;
                }
                access_token = result.access_token.unwrap();
            }

            let start_stop = get_spot_start_stop();
            fetch_and_log_new_spot_data(
                &client,
                &config,
                &access_token,
                &consumption_metering_point_code,
                &start_stop.0,
                &start_stop.1,
            )
                .await;

            let next_fetch_interval = get_next_spot_fetch_milliseconds() as u64;
            println!("Logging {} - {} done, waiting for the next fetch at {} ...", start_stop.0, start_stop.1, get_time_after_duration(next_fetch_interval));
            sleep(Duration::from_millis(next_fetch_interval)).await;
        }
    }
    else if single_mode {
        // If credentials provided, use those instead of the given access token (if access token was even given)
        if !wattivahti_username.is_empty() {
            let result = get_access_token(&wattivahti_token_endpoint, &wattivahti_username, &wattivahti_password).await;
            if result.is_err() {
                println!("Logging {} - {} failed because of accessToken, waiting for the next fetch in {}ms...", start, stop, interval);
                sleep(Duration::from_millis(interval)).await;
            }
            let result = result.unwrap();
            if result.access_token.is_none() {
                println!("Logging {} - {} failed because of accessToken, waiting for the next fetch in {}ms...", start, stop, interval);
                sleep(Duration::from_millis(interval)).await;
            }
            access_token = result.access_token.unwrap();
        }

        fetch_and_log_new_production_entry(
            &client,
            &config,
            &access_token,
            &production_metering_point_code,
            &start,
            &stop,
        )
        .await;

        fetch_and_log_new_consumption_entry(
            &client,
            &config,
            &access_token,
            &consumption_metering_point_code,
            &start,
            &stop,
        )
        .await;

        println!("Logging {} - {} done, exiting in {}ms ...", start, stop, interval);
        sleep(Duration::from_millis(interval)).await;
    }
    else {
        loop {
            // If credentials provided, use those instead of the given access token (if access token was even given)
            if !wattivahti_username.is_empty() {
                let result = get_access_token(&wattivahti_token_endpoint, &wattivahti_username, &wattivahti_password).await;
                if result.is_err() {
                    println!("Logging {} - {} failed because of accessToken, waiting for the next fetch at {} ...", start, stop, get_time_after_duration(interval));
                    sleep(Duration::from_millis(interval)).await;
                    continue;
                }
                let result = result.unwrap();
                if result.access_token.is_none() {
                    println!("Logging {} - {} failed because of accessToken, waiting for the next fetch at {} ...", start, stop, get_time_after_duration(interval));
                    sleep(Duration::from_millis(interval)).await;
                    continue;
                }
                access_token = result.access_token.unwrap();
            }

            let start_stop = get_start_stop();
            fetch_and_log_new_production_entry(
                &client,
                &config,
                &access_token,
                &production_metering_point_code,
                &start_stop.0,
                &start_stop.1,
            )
            .await;

            fetch_and_log_new_consumption_entry(
                &client,
                &config,
                &access_token,
                &consumption_metering_point_code,
                &start_stop.0,
                &start_stop.1,
            )
            .await;

            let next_fetch_interval = get_next_fetch_milliseconds() as u64;
            println!("Logging {} - {} done, waiting for the next fetch at {} ...", start_stop.0, start_stop.1, get_time_after_duration(next_fetch_interval));
            sleep(Duration::from_millis(next_fetch_interval)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_start_stop() {
        let data = get_start_stop();
        println!("Result: {} - {}", data.0, data.1);
    }

    #[tokio::test]
    async fn test_get_spot_start_stop() {
        let data = get_spot_start_stop();
        println!("Result: {} - {}", data.0, data.1);
    }

    #[tokio::test]
    async fn test_get_time_after_duration() {
        let data = get_time_after_duration(21_600_000);
        println!("Result: {}", data);
    }

    #[tokio::test]
    async fn test_get_next_fetch_milliseconds() {
        let data = get_next_fetch_milliseconds();
        println!("Result: {}", data);
    }

    #[tokio::test]
    async fn test_get_consumption_energy_fee() {
        let config = settings::config::load_settings(format!("configs/{}.yaml", "test"))
            .expect("Failed to load settings file.");

        let dt = DateTime::<Utc>::from_utc(NaiveDateTime::parse_from_str("2019-12-31T22:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(), Utc);
        let energy_fee = config.get_consumption_energy_fee(2.93, dt);
        println!("Result: {}", energy_fee);
    }
}

