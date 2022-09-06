use std::time::Duration;

use api::TSV;
use api::get_consumption_data;
use api::get_production_data;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Timelike;
use chrono::{DateTime, Utc};
use chrono_tz::Europe::Helsinki;
use dotenv::dotenv;
use influxdb::Client;
use influxdb::InfluxDbWriteable;
use influxdb::ReadQuery;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::sleep;

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
        let price = get_day_ahead_price(&client, &time).await;
        let transfer_fee = get_production_transfer_fee();
        let current_data = TimeSeriesValue {
            time: time,
            meteringpointcode_tag: meteringpointcode.to_string(),
            measurementtype_tag: measurementtype.to_string(),
            meteringpointcode: meteringpointcode.to_string(),
            measurementtype: measurementtype.to_string(),
            unit: unit.to_string(),
            timestamp: time.format("%Y-%m-%dT%H:%M:%S").to_string(),
            value: time_series_value.q,
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
        let price = get_day_ahead_price(&client, &time).await;

        let transfer_basic_fee = get_consumption_transfer_basic_fee();
        let transfer_fee = get_consumption_transfer_fee(&time);
        let tax_fee = get_consumption_tax_fee();
        let basic_fee = get_consumption_basic_fee();
        let energy_fee = get_consumption_energy_fee();

        let current_data = TimeSeriesValue {
            time: time,
            meteringpointcode_tag: meteringpointcode.to_string(),
            measurementtype_tag: measurementtype.to_string(),
            meteringpointcode: meteringpointcode.to_string(),
            measurementtype: measurementtype.to_string(),
            unit: unit.to_string(),
            timestamp: time.format("%Y-%m-%dT%H:%M:%S").to_string(),
            value: time_series_value.q,
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

async fn set_consumption_fees(client: &Client, start: &DateTime<Utc>, end: &DateTime<Utc>) {
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
                    let transfer_basic_fee = get_consumption_transfer_basic_fee();
                    let transfer_fee = get_consumption_transfer_fee(&value.time);
                    let tax_fee = get_consumption_tax_fee();
                    let basic_fee = get_consumption_basic_fee();
                    let energy_fee = get_consumption_energy_fee();

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

async fn set_production_fees(client: &Client, start: &DateTime<Utc>, end: &DateTime<Utc>) {
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
                    let transfer_fee = get_production_transfer_fee();

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

fn get_consumption_transfer_basic_fee() -> f32 {
    let fee: f32 = dotenv::var("CONSUMPTION_TRANSFER_BASIC_FEE")
        .map(|var| var.parse::<f32>())
        .unwrap_or(Ok(0.0))
        .unwrap();
    fee
}

fn get_consumption_basic_fee() -> f32 {
    let fee: f32 = dotenv::var("CONSUMPTION_BASIC_FEE")
        .map(|var| var.parse::<f32>())
        .unwrap_or(Ok(0.0))
        .unwrap();
    fee
}

fn get_consumption_transfer_fee(time: &DateTime<Utc>) -> f32 {
    let time_or_seasonal: bool = dotenv::var("CONSUMPTION_TIME_OR_SEASONAL")
        .map(|var| var.parse::<bool>())
        .unwrap_or(Ok(false))
        .unwrap();

    let time_start: u32 = dotenv::var("CONSUMPTION_TIME_START")
        .map(|var| var.parse::<u32>())
        .unwrap_or(Ok(22))
        .unwrap();
    let time_end: u32 = dotenv::var("CONSUMPTION_TIME_END")
        .map(|var| var.parse::<u32>())
        .unwrap_or(Ok(7))
        .unwrap();

    let mut fee = 0.0;
    if time_or_seasonal {
        let local = time.with_timezone(&Helsinki);
        let hour = local.hour();
        if hour <= time_end || hour >= time_start {
            fee = dotenv::var("CONSUMPTION_TRANSFER_FEE_NIGHT")
                .map(|var| var.parse::<f32>())
                .unwrap_or(Ok(0.0))
                .unwrap();
        }
        else {
            fee = dotenv::var("CONSUMPTION_TRANSFER_FEE_DAY")
                .map(|var| var.parse::<f32>())
                .unwrap_or(Ok(0.0))
                .unwrap();
        }
    }
    else {
        fee = dotenv::var("CONSUMPTION_TRANSFER_FEE")
            .map(|var| var.parse::<f32>())
            .unwrap_or(Ok(0.0))
            .unwrap();
    }

    fee
}

fn get_consumption_tax_fee() -> f32 {
    let fee: f32 = dotenv::var("CONSUMPTION_TAX_FEE")
        .map(|var| var.parse::<f32>())
        .unwrap_or(Ok(0.0))
        .unwrap();
    fee
}

fn get_consumption_energy_fee() -> f32 {
    let fee: f32 = dotenv::var("CONSUMPTION_ENERGY_FEE")
        .map(|var| var.parse::<f32>())
        .unwrap_or(Ok(0.0))
        .unwrap();
    fee
}

fn get_production_transfer_fee() -> f32 {
    let fee: f32 = dotenv::var("PRODUCTION_TRANSFER_FEE")
        .map(|var| var.parse::<f32>())
        .unwrap_or(Ok(0.0))
        .unwrap();
    fee
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

#[tokio::main]
async fn main() {
    dotenv().ok();

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

    let access_token = dotenv::var("ACCESS_TOKEN").unwrap_or("".to_string());
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
            &start,
            &stop,
        )
        .await;

        set_production_fees(
            &client,
            &start,
            &stop,
        )
        .await;

        println!("Fees changed, exiting soon...");
        sleep(Duration::from_millis(interval)).await;
    }
    else {
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
}
