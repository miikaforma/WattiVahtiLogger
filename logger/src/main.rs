use std::time::Duration;

use api::TSV;
use api::get_consumption_data;
use api::get_production_data;
use chrono::{DateTime, Utc};
use dotenv::dotenv;
use influxdb::Client;
use influxdb::InfluxDbWriteable;
use tokio::time::sleep;

#[derive(InfluxDbWriteable)]
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
}

async fn fetch_and_log_new_production_entry(
    client: &Client,
    access_token: &str,
    metering_point_code: &str, 
    start: &str, 
    stop: &str,
) {
    println!("Logging new production entry for {}", &metering_point_code);

    match get_production_data(&access_token, &metering_point_code, &start, &stop).await {
        Ok(data) => {
            for tsv in data.getconsumptionsresult.consumptiondata.timeseries.values.tsv.iter() {
                log_new_production_entry(client, &metering_point_code, "6", &data.getconsumptionsresult.consumptiondata.sum.unit, &tsv).await;
            }
        },
        Err(err) => println!("Failed to fetch data | {}", err),
    }
}

async fn fetch_and_log_new_consumption_entry(
    client: &Client,
    access_token: &str,
    metering_point_code: &str, 
    start: &str, 
    stop: &str,
) {
    println!("Logging new consumption entry for {}", &metering_point_code);

    match get_consumption_data(&access_token, &metering_point_code, &start, &stop).await {
        Ok(data) => {
            for tsv in data.getconsumptionsresult.consumptiondata.timeseries.values.tsv.iter() {
                log_new_consumption_entry(client, &metering_point_code, "1", &data.getconsumptionsresult.consumptiondata.sum.unit, &tsv).await;
            }
        },
        Err(err) => println!("Failed to fetch data | {}", err),
    }
}

async fn log_new_production_entry(client: &Client, 
    meteringpointcode: &str, 
    measurementtype: &str, 
    unit: &str, 
    time_series_value: &TSV) {
        let time = &time_series_value.get_timestamp_utc();
        if time.is_none() {
            println!("Skipping logging because time couldn't be parsed");
        }

        println!("Logging production UTC: {:?} - {}", time, time_series_value.q);

        let time = time.unwrap();
        let current_data = TimeSeriesValue {
            time: time,
            meteringpointcode_tag: meteringpointcode.to_string(),
            measurementtype_tag: measurementtype.to_string(),
            meteringpointcode: meteringpointcode.to_string(),
            measurementtype: measurementtype.to_string(),
            unit: unit.to_string(),
            timestamp: time.format("%Y-%m-%dT%H:%M:%S").to_string(),
            value: time_series_value.q,
        };

        let write_result = client
            .query(&current_data.into_query("productions"))
            .await;
        if let Err(err) = write_result {
            eprintln!("Error writing to db: {}", err)
        }
}

async fn log_new_consumption_entry(client: &Client, 
    meteringpointcode: &str, 
    measurementtype: &str, 
    unit: &str, 
    time_series_value: &TSV) {
        let time = &time_series_value.get_timestamp_utc();
        if time.is_none() {
            println!("Skipping logging because time couldn't be parsed");
        }

        println!("Logging consumption UTC: {:?} - {}", time, time_series_value.q);

        let time = time.unwrap();
        let current_data = TimeSeriesValue {
            time: time,
            meteringpointcode_tag: meteringpointcode.to_string(),
            measurementtype_tag: measurementtype.to_string(),
            meteringpointcode: meteringpointcode.to_string(),
            measurementtype: measurementtype.to_string(),
            unit: unit.to_string(),
            timestamp: time.format("%Y-%m-%dT%H:%M:%S").to_string(),
            value: time_series_value.q,
        };

        let write_result = client
            .query(&current_data.into_query("consumptions"))
            .await;
        if let Err(err) = write_result {
            eprintln!("Error writing to db: {}", err)
        }
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let database_url = dotenv::var("DATABASE_URL").unwrap_or("http://localhost:8086".to_string());
    let database_name = dotenv::var("DATABASE_NAME").unwrap_or("wattivahti".to_string());

    let interval: u64 = dotenv::var("INTERVAL")
        .map(|var| var.parse::<u64>())
        .unwrap_or(Ok(3_600_000))
        .unwrap();

    let access_token = dotenv::var("ACCESS_TOKEN").unwrap();
    let consumption_metering_point_code = dotenv::var("CONSUMPTION_METERING_POINT_CODE").unwrap();
    let production_metering_point_code = dotenv::var("PRODUCTION_METERING_POINT_CODE").unwrap();
    let start = dotenv::var("START").unwrap();
    let stop = dotenv::var("STOP").unwrap();

    // Connect to database
    let client = Client::new(database_url, database_name);

    loop {
        fetch_and_log_new_production_entry(
            &client,
            &access_token,
            &production_metering_point_code,
            &start,
            &stop,
        )
        .await;

        fetch_and_log_new_consumption_entry(
            &client,
            &access_token,
            &consumption_metering_point_code,
            &start,
            &stop,
        )
        .await;

        println!("Logging done, waiting for the next fetch...");
        sleep(Duration::from_millis(interval)).await;
    }
}
