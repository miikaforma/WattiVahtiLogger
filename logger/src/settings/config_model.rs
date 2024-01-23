use chrono::{DateTime, NaiveDateTime, TimeZone, Timelike, Utc};
use serde::{Deserialize, Serialize};

use super::time::get_timezone;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ContractType {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "fixed")]
    Fixed,
    #[serde(rename = "spot")]
    Spot,
}

impl From<i16> for ContractType {
    fn from(item: i16) -> Self {
        match item {
            1 => ContractType::None,
            2 => ContractType::Fixed,
            3 => ContractType::Spot,
            _ => panic!("Invalid value for ContractType"),
        }
    }
}

impl From<ContractType> for i16 {
    fn from(contract_type: ContractType) -> Self {
        match contract_type {
            ContractType::None => 1,
            ContractType::Fixed => 2,
            ContractType::Spot => 3,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContractConfig {
    start_time: String,
    end_time: Option<String>,
    pub contract_type: ContractType,
    energy: EnergyConfig,
    transfer: TransferConfig,
    tax_percentage: Option<f32>,
}

impl ContractConfig {
    pub fn get_spot_margin(&self) -> Option<f32> {
        self.energy.margin
    }

    pub fn get_tax_percentage(&self) -> f32 {
        self.tax_percentage.unwrap_or(24.0)
    }

    pub fn get_transfer_basic_fee(&self) -> f32 {
        self.transfer.basic_fee
    }

    pub fn get_is_night(&self, time: DateTime<Utc>) -> bool {
        let energy_config = &self.energy;

        let time_start = energy_config.night_start_hour.unwrap_or(22);
        let time_end = energy_config.night_end_hour.unwrap_or(7);

        let local = time.with_timezone(&get_timezone());
        let hour = local.hour();
        return if hour < time_end || hour >= time_start {
            true
        } else {
            false
        };
    }

    pub fn get_transfer_fee(&self, time: DateTime<Utc>) -> f32 {
        let transfer_config = &self.transfer;

        let time_start = transfer_config.night_start_hour.unwrap_or(22);
        let time_end = transfer_config.night_end_hour.unwrap_or(7);

        let local = time.with_timezone(&get_timezone());
        let hour = local.hour();
        return if hour < time_end || hour >= time_start {
            transfer_config.night_fee
        } else {
            transfer_config.day_fee
        };
    }

    pub fn get_transfer_tax_fee(&self) -> f32 {
        self.transfer.tax_fee
    }

    pub fn get_energy_basic_fee(&self) -> f32 {
        self.energy.basic_fee
    }

    pub fn get_energy_margin(&self) -> f32 {
        self.energy.margin.unwrap_or(0.0)
    }

    pub fn get_energy_fee(&self, spot_price: f32, time: DateTime<Utc>) -> f32 {
        match self.contract_type {
            ContractType::None => 0.0,
            ContractType::Fixed => self.get_energy_fee_fixed(time),
            ContractType::Spot => self.get_energy_fee_spot(spot_price),
        }
    }

    pub fn get_energy_fee_fixed(&self, time: DateTime<Utc>) -> f32 {
        let energy_config = &self.energy;

        let time_start = energy_config.night_start_hour.unwrap_or(22);
        let time_end = energy_config.night_end_hour.unwrap_or(7);

        let local = time.with_timezone(&get_timezone());
        let hour = local.hour();
        return if hour < time_end || hour >= time_start {
            energy_config.night_fee.unwrap_or(0.0)
        } else {
            energy_config.day_fee.unwrap_or(0.0)
        };
    }

    pub fn get_energy_fee_spot(&self, spot_price: f32) -> f32 {
        let energy_config = &self.energy;

        let margin = energy_config.margin.unwrap_or(0.0);
        let tax_percentage = self.get_tax_percentage();
        let no_tax_for_negative = energy_config.negative_no_tax.unwrap_or(false);

        if no_tax_for_negative && spot_price < 0.0 {
            return (spot_price / 10.0) + margin;
        }

        return (spot_price / 10.0 * (tax_percentage / 100.0 + 1.0)) + margin;
    }

    pub fn validate_energy(&self) -> Result<(), &'static str> {
        match self.contract_type {
            ContractType::Fixed => {
                if self.energy.day_fee.is_none() || self.energy.night_fee.is_none() {
                    return Err("For Fixed contract type, day_fee and night_fee are required");
                }
            }
            ContractType::Spot => {
                if self.energy.margin.is_none() {
                    return Err("For Spot contract type, margin is required");
                }
            }
            _ => {}
        }

        Ok(())
    }

    pub fn is_match(&self, time: DateTime<Utc>) -> bool {
        let start_time = &self.get_start_time_utc();
        if start_time.is_none() {
            return false;
        }
        let start_time = start_time.unwrap();

        // If time is before start_time
        if time < start_time {
            return false;
        }

        let end_time = &self.get_end_time_utc();
        if end_time.is_none() {
            return true;
        }
        let end_time = end_time.unwrap();

        time <= end_time
    }

    fn get_start_time_utc(&self) -> Option<DateTime<Utc>> {
        let naive_time = NaiveDateTime::parse_from_str(&self.start_time, "%Y-%m-%dT%H:%M:%S");
        if naive_time.is_err() {
            return None;
        }
        debug!("Start Time Local {}", naive_time.unwrap());

        Some(
            Utc.from_utc_datetime(
                &get_timezone()
                    .from_local_datetime(&naive_time.unwrap())
                    .unwrap()
                    .naive_utc(),
            ),
        )
    }

    fn get_end_time_utc(&self) -> Option<DateTime<Utc>> {
        if self.end_time.is_none() {
            return None;
        }

        let naive_time =
            NaiveDateTime::parse_from_str(&self.end_time.as_ref().unwrap(), "%Y-%m-%dT%H:%M:%S");
        if naive_time.is_err() {
            panic!(
                "Failed to parse end time {}",
                self.end_time.as_ref().unwrap()
            )
        }
        debug!("End Time Local {}", naive_time.unwrap());

        Some(
            Utc.from_utc_datetime(
                &get_timezone()
                    .from_local_datetime(&naive_time.unwrap())
                    .unwrap()
                    .naive_utc(),
            ),
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EnergyConfig {
    night_start_hour: Option<u32>,
    night_end_hour: Option<u32>,
    basic_fee: f32,
    day_fee: Option<f32>,
    night_fee: Option<f32>,
    margin: Option<f32>,
    negative_no_tax: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransferConfig {
    night_start_hour: Option<u32>,
    night_end_hour: Option<u32>,
    basic_fee: f32,
    day_fee: f32,
    night_fee: f32,
    tax_fee: f32,
    negative_no_tax: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContractsConfig {
    contracts: Vec<ContractConfig>,
}

impl ContractsConfig {
    pub fn get_contract(&self, time: DateTime<Utc>) -> Option<&ContractConfig> {
        let matches: Vec<&ContractConfig> = self
            .contracts
            .iter()
            .filter(|voc| voc.is_match(time))
            .collect();

        // If just one match, return it
        if matches.len() == 1 {
            return Some(matches[0]);
        }

        warn!(
            "Expected 1 contract in get_contract with time {} but found {}.",
            time,
            matches.len()
        );

        None
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        self.validate_contract_times()?;

        for contract in &self.contracts {
            contract.validate_energy()?;
        }

        Ok(())
    }

    fn validate_contract_times(&self) -> Result<(), &'static str> {
        let mut contracts = self.contracts.clone();
        contracts.sort_by(|a, b| a.start_time.cmp(&b.start_time));

        for windows in contracts.windows(2) {
            let first = &windows[0];
            let second = &windows[1];

            let start_time = second.get_start_time_utc().unwrap();
            let end_time = first.get_end_time_utc().unwrap();

            if end_time >= start_time {
                return Err("Overlapping contracts detected");
            }

            if end_time + chrono::Duration::seconds(1) != start_time {
                return Err("Gap between contracts detected");
            }
        }

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SettingsConfig {
    pub consumption: ContractsConfig,
    pub production: ContractsConfig,
}

impl SettingsConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.consumption.validate()?;
        self.production.validate()?;

        Ok(())
    }
}
