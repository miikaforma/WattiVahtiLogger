use api::ConsumptionsResult;
use chrono::{DateTime, Utc};
use influxdb::{Client, InfluxDbWriteable, ReadQuery};

use crate::{settings::config_model::SettingsConfig, storage::influxdb::time_series_value::TimeSeriesValue};

use super::price_data::PriceData;

pub fn is_enabled() -> bool {
    dotenv::var("INFLUXDB_ENABLED")
        .map(|var| var.parse::<bool>())
        .unwrap_or(Ok(false))
        .unwrap()
}

pub async fn upsert_productions_into_influxdb(
    data: &ConsumptionsResult,
    config: &SettingsConfig,
) -> Result<(), anyhow::Error> {
    if !is_enabled() {
        return Ok(());
    }

    let mut messages = Vec::new();

    let client = connect_to_db().await;
    for (pos, tsv) in data
        .getconsumptionsresult
        .consumptiondata
        .timeseries
        .values
        .tsv
        .iter()
        .enumerate()
    {
        let time = &tsv.get_timestamp_utc_calculated(pos);
        if time.is_none() {
            warn!("InfluxDB | Skipping production logging because time couldn't be parsed");
            continue;
        }

        if tsv.quantity.is_none() {
            // warn!("InfluxDB | Skipping production logging because quantity was null");
            continue;
        }

        let time = time.unwrap();
        let price = get_day_ahead_price(&client, &time).await;
        let contract = config.production.get_contract(time);
        if contract.is_none() {
            warn!("InfluxDB | Skipping production logging because contract couldn't be found");
            continue;
        }
        let contract = contract.unwrap();
        let transfer_fee = contract.get_transfer_fee(time);

        let meteringpointcode = &data.getconsumptionsresult.consumptiondata.meteringpointcode;
        let measurementtype = "6";
        let unit = &data.getconsumptionsresult.consumptiondata.sum.unit;
        let value = tsv.quantity.unwrap();
        let tax_percentage = contract.get_tax_percentage();

        let current_data = TimeSeriesValue {
            time: time,
            meteringpointcode_tag: meteringpointcode.to_string(),
            measurementtype_tag: measurementtype.to_string(),
            meteringpointcode: meteringpointcode.to_string(),
            measurementtype: measurementtype.to_string(),
            unit: unit.to_string(),
            timestamp: time.format("%Y-%m-%dT%H:%M:%S").to_string(),
            value: value,
            price: price / 1000.0,

            transfer_basic_fee: None,
            transfer_fee: Some(transfer_fee),
            tax_fee: None,
            basic_fee: None,
            energy_fee: None,

            contract_type: contract.contract_type.clone().into(),
            spot_margin: None,
            tax_percentage: Some(tax_percentage),
        };

        let write_result = client.query(&current_data.into_query("productions")).await;
        if let Err(err) = write_result {
            error!("Error writing to db: {}", err)
        }

        messages.push(format!("InfluxDB | Production {} - {:.2}", time, value));
    }

    let all_messages = messages.join("\n");
    info!("{}", all_messages);

    Ok(())
}

pub async fn upsert_consumptions_into_influxdb(
    data: &ConsumptionsResult,
    config: &SettingsConfig,
) -> Result<(), anyhow::Error> {
    if !is_enabled() {
        return Ok(());
    }

    let mut messages = Vec::new();

    let client = connect_to_db().await;
    for (pos, tsv) in data
        .getconsumptionsresult
        .consumptiondata
        .timeseries
        .values
        .tsv
        .iter()
        .enumerate()
    {
        let time = &tsv.get_timestamp_utc_calculated(pos);
        if time.is_none() {
            warn!("InfluxDB | Skipping consumption logging because time couldn't be parsed");
            continue;
        }

        if tsv.quantity.is_none() {
            // warn!("InfluxDB | Skipping consumption logging because quantity was null");
            continue;
        }

        let time = time.unwrap();
        let price = get_day_ahead_price(&client, &time).await;
        let contract = config.consumption.get_contract(time);
        if contract.is_none() {
            warn!("InfluxDB | Skipping consumption logging because contract couldn't be found");
            continue;
        }
        let contract = contract.unwrap();
        let transfer_basic_fee = contract.get_transfer_basic_fee();
        let transfer_fee = contract.get_transfer_fee(time);
        let tax_fee = contract.get_transfer_tax_fee();
        let basic_fee = contract.get_energy_basic_fee();
        let energy_fee = contract.get_energy_fee(price, time);

        let meteringpointcode = &data.getconsumptionsresult.consumptiondata.meteringpointcode;
        let measurementtype = "1";
        let unit = &data.getconsumptionsresult.consumptiondata.sum.unit;
        let value = tsv.quantity.unwrap();

        let current_data = TimeSeriesValue {
            time: time,
            meteringpointcode_tag: meteringpointcode.to_string(),
            measurementtype_tag: measurementtype.to_string(),
            meteringpointcode: meteringpointcode.to_string(),
            measurementtype: measurementtype.to_string(),
            unit: unit.to_string(),
            timestamp: time.format("%Y-%m-%dT%H:%M:%S").to_string(),
            value: value,
            price: price / 1000.0,

            transfer_basic_fee: Some(transfer_basic_fee),
            transfer_fee: Some(transfer_fee),
            tax_fee: Some(tax_fee),
            basic_fee: Some(basic_fee),
            energy_fee: Some(energy_fee),

            contract_type: contract.contract_type.clone().into(),
            spot_margin: contract.get_spot_margin(),
            tax_percentage: Some(contract.get_tax_percentage()),
        };

        let write_result = client.query(&current_data.into_query("consumptions")).await;
        if let Err(err) = write_result {
            error!("Error writing to db: {}", err)
        }

        messages.push(format!("InfluxDB | Consumption {} - {:.2}", time, value));
    }

    let all_messages = messages.join("\n");
    info!("{}", all_messages);

    Ok(())
}

async fn get_day_ahead_price(client: &Client, time: &DateTime<Utc>) -> f32 {
    let read_query = ReadQuery::new(format!(
        "SELECT * FROM dayAheadPrices WHERE type_tag='A44' AND time='{}' LIMIT 1",
        time.to_rfc3339()
    ));

    let read_result = client
        .json_query(read_query)
        .await
        .and_then(|mut db_result| db_result.deserialize_next::<PriceData>());

    match read_result {
        Ok(result) => {
            if result.series.len() > 0 && result.series[0].values.len() > 0 {
                let data = &result.series[0].values[0];
                return data.price;
            }
        }
        Err(err) => {
            error!("Error reading dayAheadPrices from the db: {}", err);
        }
    }

    0.0
}

async fn connect_to_db() -> Client {
    let database_url = dotenv::var("DATABASE_URL").unwrap_or("http://localhost:8086".to_string());
    let database_name = dotenv::var("DATABASE_NAME").unwrap_or("entsoe".to_string());

    Client::new(&database_url, &database_name)
}
