use api::{ConsumptionsResult, ResolutionDuration};
use tokio_postgres::{Error, NoTls};

use crate::settings::config_model::{ContractType, SettingsConfig};

pub fn is_enabled() -> bool {
    dotenv::var("TIMESCALEDB_ENABLED")
        .map(|var| var.parse::<bool>())
        .unwrap_or(Ok(false))
        .unwrap()
}

pub async fn upsert_productions_into_timescaledb(
    data: &ConsumptionsResult,
    config: &SettingsConfig,
) -> Result<(), Error> {
    if !is_enabled() {
        return Ok(());
    }

    let mut messages = Vec::new();

    let mut client = connect_to_db().await?;
    let trans = client.transaction().await?;
    let resolution = &data
        .getconsumptionsresult
        .consumptiondata
        .timeseries
        .resolution;
    let resolution_duration = ResolutionDuration::from_str(resolution);

    for (pos, tsv) in data
        .getconsumptionsresult
        .consumptiondata
        .timeseries
        .values
        .tsv
        .iter()
        .enumerate()
    {
        let time = &tsv.get_timestamp_utc_calculated(pos, &resolution_duration);
        if time.is_none() {
            warn!("TimescaleDB | Skipping production logging because time couldn't be parsed");
            continue;
        }

        if tsv.quantity.is_none() {
            // warn!("TimescaleDB | Skipping production logging because quantity was null");
            continue;
        }

        let time = time.unwrap();
        let contract = config.production.get_contract(time);
        if contract.is_none() {
            warn!("TimescaleDB | Skipping production logging because contract couldn't be found");
            continue;
        }

        let contract = contract.unwrap();
        let contract_type: i16 = contract.contract_type.clone().into();
        let transfer_fee = contract.get_transfer_fee(time);
        let meteringpointcode = &data.getconsumptionsresult.consumptiondata.meteringpointcode;
        let measurementtype: i32 = 6;
        let unit = &data.getconsumptionsresult.consumptiondata.sum.unit;
        let value = tsv.quantity.unwrap();

        // time, metering_point_code, measure_type, contract_type, source, measure_unit, value, energy_basic_fee, energy_fee, energy_margin, transfer_basic_fee, transfer_fee, tax_fee, tax_percentage

        let tax_percentage = contract.get_tax_percentage();

        let transfer_basic_fee = contract.get_transfer_basic_fee();
        let transfer_tax_fee = contract.get_transfer_tax_fee();
        let energy_basic_fee = contract.get_energy_basic_fee();
        let energy_margin = contract.get_energy_margin();

        let _ = trans
            .execute("INSERT INTO energies (time, metering_point_code, measure_type, contract_type, source, measure_unit, value, energy_basic_fee, energy_margin, transfer_basic_fee, transfer_fee, transfer_tax_fee, tax_percentage, spot_price, resolution_duration)
                                VALUES ($1, $2, $3, $4, 'wattivahti', $5, $6, $7, $8, $9, $10, $11, $12, COALESCE((SELECT (price / 10.) FROM day_ahead_prices WHERE time = $1), (SELECT (price / 10.) FROM day_ahead_prices WHERE time = date_trunc('hour', $1))), $13)
                                ON CONFLICT (time, metering_point_code, measure_type, resolution_duration) DO UPDATE
                                    SET contract_type = $4, source = 'wattivahti', measure_unit = $5, value = $6, energy_basic_fee = $7, energy_margin = $8, transfer_basic_fee = $9, transfer_fee = $10, transfer_tax_fee = $11, tax_percentage = $12, spot_price = EXCLUDED.spot_price, resolution_duration = $13",
            &[&time, &meteringpointcode.to_string(), &measurementtype, &contract_type, &unit.to_string(), &value, &energy_basic_fee, &energy_margin, &transfer_basic_fee, &transfer_fee, &transfer_tax_fee, &tax_percentage, resolution])
        .await?;

        messages.push(format!("TimescaleDB | Production {} - {:.2}", time, value));
    }

    trans.commit().await?;

    let all_messages = messages.join("\n");
    info!("{}", all_messages);

    Ok(())
}

pub async fn upsert_consumptions_into_timescaledb(
    data: &ConsumptionsResult,
    config: &SettingsConfig,
) -> Result<(), Error> {
    if !is_enabled() {
        return Ok(());
    }

    let mut messages = Vec::new();

    let mut client = connect_to_db().await?;
    let trans = client.transaction().await?;
    let resolution = &data
        .getconsumptionsresult
        .consumptiondata
        .timeseries
        .resolution;
    let resolution_duration = ResolutionDuration::from_str(resolution);

    for (pos, tsv) in data
        .getconsumptionsresult
        .consumptiondata
        .timeseries
        .values
        .tsv
        .iter()
        .enumerate()
    {
        let time = &tsv.get_timestamp_utc_calculated(pos, &resolution_duration);
        if time.is_none() {
            warn!("TimescaleDB | Skipping consumption logging because time couldn't be parsed");
            continue;
        }

        if tsv.quantity.is_none() {
            // warn!("TimescaleDB | Skipping consumption logging because quantity was null");
            continue;
        }

        let time = time.unwrap();
        let contract = config.consumption.get_contract(time);
        if contract.is_none() {
            warn!("TimescaleDB | Skipping consumption logging because contract couldn't be found");
            continue;
        }

        let contract = contract.unwrap();
        let contract_type: i16 = contract.contract_type.clone().into();
        let meteringpointcode = &data.getconsumptionsresult.consumptiondata.meteringpointcode;
        let measurementtype: i32 = 1;
        let unit = &data.getconsumptionsresult.consumptiondata.sum.unit;
        let value = tsv.quantity.unwrap();
        let is_night = contract.get_is_night(time);
        let tax_percentage = contract.get_tax_percentage();

        match contract.contract_type {
            ContractType::Fixed | ContractType::Hybrid => {
                let transfer_basic_fee = contract.get_transfer_basic_fee();
                let transfer_fee = contract.get_transfer_fee(time);
                let transfer_tax_fee = contract.get_transfer_tax_fee();
                let energy_basic_fee = contract.get_energy_basic_fee();
                let energy_fee = contract.get_energy_fee_fixed(time);

                // time, metering_point_code, measure_type, contract_type, source, measure_unit, value, energy_basic_fee, energy_fee, energy_margin, transfer_basic_fee, transfer_fee, transfer_tax_fee, tax_percentage

                
                let _ = trans
                    .execute("INSERT INTO energies (time, metering_point_code, measure_type, contract_type, source, measure_unit, value, energy_basic_fee, energy_fee, transfer_basic_fee, transfer_fee, transfer_tax_fee, tax_percentage, night, spot_price, resolution_duration) 
                                        VALUES ($1, $2, $3, $4, 'wattivahti', $5, $6, $7, $8, $9, $10, $11, $12, $13, COALESCE((SELECT (price / 10.) FROM day_ahead_prices WHERE time = $1), (SELECT (price / 10.) FROM day_ahead_prices WHERE time = date_trunc('hour', $1))), $14)
                                        ON CONFLICT (time, metering_point_code, measure_type, resolution_duration) DO UPDATE
                                            SET contract_type = $4, source = 'wattivahti', measure_unit = $5, value = $6, energy_basic_fee = $7, energy_fee = $8, transfer_basic_fee = $9, transfer_fee = $10, transfer_tax_fee = $11, tax_percentage = $12, night = $13, spot_price = EXCLUDED.spot_price, resolution_duration = $14",
                    &[&time, &meteringpointcode.to_string(), &measurementtype, &contract_type, &unit.to_string(), &value, &energy_basic_fee, &energy_fee, &transfer_basic_fee, &transfer_fee, &transfer_tax_fee, &tax_percentage, &is_night, resolution])
                .await?;
            }
            ContractType::Spot => {
                let transfer_basic_fee = contract.get_transfer_basic_fee();
                let transfer_fee = contract.get_transfer_fee(time);
                let transfer_tax_fee = contract.get_transfer_tax_fee();
                let energy_basic_fee = contract.get_energy_basic_fee();
                let energy_margin = contract.get_energy_margin();

                let _ = trans
                    .execute("INSERT INTO energies (time, metering_point_code, measure_type, contract_type, source, measure_unit, value, energy_basic_fee, energy_margin, transfer_basic_fee, transfer_fee, transfer_tax_fee, tax_percentage, night, spot_price, resolution_duration) 
                                        VALUES ($1, $2, $3, $4, 'wattivahti', $5, $6, $7, $8, $9, $10, $11, $12, $13, COALESCE((SELECT (price / 10.) FROM day_ahead_prices WHERE time = $1), (SELECT (price / 10.) FROM day_ahead_prices WHERE time = date_trunc('hour', $1))), $14)
                                        ON CONFLICT (time, metering_point_code, measure_type, resolution_duration) DO UPDATE
                                            SET contract_type = $4, source = 'wattivahti', measure_unit = $5, value = $6, energy_basic_fee = $7, energy_margin = $8, transfer_basic_fee = $9, transfer_fee = $10, transfer_tax_fee = $11, tax_percentage = $12, night = $13, spot_price = EXCLUDED.spot_price, resolution_duration = $14",
                    &[&time, &meteringpointcode.to_string(), &measurementtype, &contract_type, &unit.to_string(), &value, &energy_basic_fee, &energy_margin, &transfer_basic_fee, &transfer_fee, &transfer_tax_fee, &tax_percentage, &is_night, resolution])
                .await?;
            }
            ContractType::None => todo!(),
        }

        messages.push(format!("TimescaleDB | Consumption {} - {:.2}", time, value));
    }

    trans.commit().await?;

    let all_messages = messages.join("\n");
    info!("{}", all_messages);

    Ok(())
}

pub async fn refresh_consumption_views() -> Result<(), Error> {
    let client = connect_to_db().await?;

    // Execute the refresh commands
    client
        .execute(
            "CALL refresh_continuous_aggregate('energies_consumption_15min_by_15min', NULL, NULL)",
            &[],
        )
        .await?;
    client
        .execute(
            "CALL refresh_continuous_aggregate('energies_consumption_hour_by_hour', NULL, NULL)",
            &[],
        )
        .await?;
    client
        .execute(
            "CALL refresh_continuous_aggregate('energies_consumption_day_by_day', NULL, NULL)",
            &[],
        )
        .await?;
    client
        .execute(
            "CALL refresh_continuous_aggregate('energies_consumption_month_by_month', NULL, NULL)",
            &[],
        )
        .await?;
    client
        .execute(
            "CALL refresh_continuous_aggregate('energies_consumption_year_by_year', NULL, NULL)",
            &[],
        )
        .await?;

    Ok(())
}

pub async fn refresh_production_views() -> Result<(), Error> {
    let client = connect_to_db().await?;

    // Execute the refresh commands
    client
        .execute(
            "CALL refresh_continuous_aggregate('energies_production_15min_by_15min', NULL, NULL)",
            &[],
        )
        .await?;
    client
        .execute(
            "CALL refresh_continuous_aggregate('energies_production_hour_by_hour', NULL, NULL)",
            &[],
        )
        .await?;
    client
        .execute(
            "CALL refresh_continuous_aggregate('energies_production_day_by_day', NULL, NULL)",
            &[],
        )
        .await?;
    client
        .execute(
            "CALL refresh_continuous_aggregate('energies_production_month_by_month', NULL, NULL)",
            &[],
        )
        .await?;
    client
        .execute(
            "CALL refresh_continuous_aggregate('energies_production_year_by_year', NULL, NULL)",
            &[],
        )
        .await?;

    Ok(())
}

async fn connect_to_db() -> Result<tokio_postgres::Client, Error> {
    let (client, connection) = tokio_postgres::connect(
        &dotenv::var("TIMESCALEDB_CONNECTION_STRING").unwrap_or(
            "host=localhost user=myuser password=mysecretpassword dbname=electricity".to_string(),
        ),
        NoTls,
    )
    .await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });

    Ok(client)
}
