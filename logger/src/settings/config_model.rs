use chrono::{DateTime, NaiveDateTime, Timelike, TimeZone, Utc};
use chrono_tz::Europe::Helsinki;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct SettingsConfig {
    spot_data_vat_multiplier: Vec<FloatSetting>,
    production_transfer_fees: Vec<FloatSetting>,
    consumption_transfer_basic_fee: Vec<FloatSetting>,
    consumption_basic_fee: Vec<FloatSetting>,
    consumption_tax_fee: Vec<FloatSetting>,

    consumption_transfer_fee: Vec<TransferFeeSetting>,
    consumption_energy_fee: Vec<EnergyFeeSetting>,
}

impl SettingsConfig {
    pub fn get_spot_data_vat_multiplier(&self, time: DateTime<Utc>) -> f32 {
        self.get_float(&self.spot_data_vat_multiplier, time, "SPOT_DATA_VAT_MULTIPLIER")
    }

    pub fn get_production_transfer_fee(&self, time: DateTime<Utc>) -> f32 {
        self.get_float(&self.production_transfer_fees, time, "PRODUCTION_TRANSFER_FEE")
    }

    pub fn get_consumption_transfer_basic_fee(&self, time: DateTime<Utc>) -> f32 {
        self.get_float(&self.consumption_transfer_basic_fee, time, "CONSUMPTION_TRANSFER_BASIC_FEE")
    }

    pub fn get_consumption_basic_fee(&self, time: DateTime<Utc>) -> f32 {
        self.get_float(&self.consumption_basic_fee, time, "CONSUMPTION_BASIC_FEE")
    }

    pub fn get_consumption_tax_fee(&self, time: DateTime<Utc>) -> f32 {
        self.get_float(&self.consumption_tax_fee, time, "CONSUMPTION_TAX_FEE")
    }

    pub fn get_consumption_transfer_fee(&self, time: DateTime<Utc>) -> f32 {
        let matches: Vec<&TransferFeeSetting> = self.consumption_transfer_fee
            .iter()
            .filter(|voc| voc.is_match(time))
            .collect();

        // If just one match, return it
        if matches.len() == 1 {
            return matches[0].get_value(time);
        }

        // If no yaml configuration
        let time_or_seasonal: bool = dotenv::var("CONSUMPTION_TIME_OR_SEASONAL")
            .map(|var| var.parse::<bool>())
            .unwrap_or(Ok(false))
            .unwrap();

        if time_or_seasonal {
            let time_start: u32 = dotenv::var("CONSUMPTION_TIME_START")
                .map(|var| var.parse::<u32>())
                .unwrap_or(Ok(22))
                .unwrap();
            let time_end: u32 = dotenv::var("CONSUMPTION_TIME_END")
                .map(|var| var.parse::<u32>())
                .unwrap_or(Ok(7))
                .unwrap();

            let local = time.with_timezone(&Helsinki);
            let hour = local.hour();
            if hour < time_end || hour >= time_start {
                return dotenv::var("CONSUMPTION_TRANSFER_FEE_NIGHT")
                    .map(|var| var.parse::<f32>())
                    .unwrap_or(Ok(0.0))
                    .unwrap();
            }
            else {
                return dotenv::var("CONSUMPTION_TRANSFER_FEE_DAY")
                    .map(|var| var.parse::<f32>())
                    .unwrap_or(Ok(0.0))
                    .unwrap();
            }
        }
        else {
            return dotenv::var("CONSUMPTION_TRANSFER_FEE")
                .map(|var| var.parse::<f32>())
                .unwrap_or(Ok(0.0))
                .unwrap();
        }
    }

    pub fn get_consumption_energy_fee(&self, spot_price: f32, time: DateTime<Utc>) -> f32 {
        let matches: Vec<&EnergyFeeSetting> = self.consumption_energy_fee
            .iter()
            .filter(|voc| voc.is_match(time))
            .collect();

        // If just one match, return it
        if matches.len() == 1 {
            return matches[0].get_value(spot_price);
        }

        let stock_exchange: bool = dotenv::var("CONSUMPTION_STOCK_EXCHANGE_OR_FIXED")
            .map(|var| var.parse::<bool>())
            .unwrap_or(Ok(false))
            .unwrap();

        if stock_exchange {
            let margin: f32 = dotenv::var("CONSUMPTION_STOCK_EXCHANGE_MARGIN")
                .map(|var| var.parse::<f32>())
                .unwrap_or(Ok(0.0))
                .unwrap();

            let tax_percentage: f32 = dotenv::var("CONSUMPTION_STOCK_EXCHANGE_TAX_MULTIPLIER")
                .map(|var| var.parse::<f32>())
                .unwrap_or(Ok(1.24))
                .unwrap();

            (spot_price / 10.0 * tax_percentage) + margin
        }
        else {
            let fee: f32 = dotenv::var("CONSUMPTION_ENERGY_FEE")
                .map(|var| var.parse::<f32>())
                .unwrap_or(Ok(0.0))
                .unwrap();

            fee
        }
    }

    fn get_float(&self, settings: &Vec<FloatSetting>, time: DateTime<Utc>, key: &str) -> f32 {
        let matches: Vec<&FloatSetting> = settings
            .iter()
            .filter(|voc| voc.is_match(time))
            .collect();

        // If just one match, return it
        if matches.len() == 1 {
            return matches[0].value
        }

        // Fallback to environment variable
        let val: f32 = dotenv::var(key)
            .map(|var| var.parse::<f32>())
            .unwrap_or(Ok(0.0))
            .unwrap();
        val
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FloatSetting {
    start_time: String,
    end_time: Option<String>,
    value: f32,
}

impl FloatSetting {
    pub fn get_start_time_utc(&self) -> Option<DateTime<Utc>> {
        let naive_time = NaiveDateTime::parse_from_str(&self.start_time, "%Y-%m-%dT%H:%M:%S");
        if naive_time.is_err() {
            return None;
        }
        // println!("Start Time Helsinki {}", naive_time.unwrap());

        Some(Utc.from_utc_datetime(&Helsinki.from_local_datetime(&naive_time.unwrap())
            .unwrap()
            .naive_utc()))
    }

    pub fn get_end_time_utc(&self) -> Option<DateTime<Utc>> {
        if self.end_time.is_none() {
            return None;
        }

        let naive_time = NaiveDateTime::parse_from_str(&self.end_time.as_ref().unwrap(), "%Y-%m-%dT%H:%M:%S");
        if naive_time.is_err() {
            return None;
        }
        // println!("End Time Helsinki {}", naive_time.unwrap());

        Some(Utc.from_utc_datetime(&Helsinki.from_local_datetime(&naive_time.unwrap())
            .unwrap()
            .naive_utc()))
    }

    pub fn is_match(&self, time: DateTime<Utc>) -> bool {
        let start_time = &self.get_start_time_utc();
        if start_time.is_none() { return false }
        let start_time = start_time.unwrap();

        // If time is before start_time
        if time < start_time { return false }

        let end_time = &self.get_end_time_utc();
        if end_time.is_none() { return true }
        let end_time = end_time.unwrap();

        time <= end_time
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransferFeeSetting {
    start_time: String,
    end_time: Option<String>,
    time_or_seasonal: bool,
    time_config: Option<TimeSetting>,
    seasonal_config: Option<SeasonalSetting>,
}

impl TransferFeeSetting {
    pub fn get_start_time_utc(&self) -> Option<DateTime<Utc>> {
        let naive_time = NaiveDateTime::parse_from_str(&self.start_time, "%Y-%m-%dT%H:%M:%S");
        if naive_time.is_err() {
            return None;
        }
        // println!("Start Time Helsinki {}", naive_time.unwrap());

        Some(Utc.from_utc_datetime(&Helsinki.from_local_datetime(&naive_time.unwrap())
            .unwrap()
            .naive_utc()))
    }

    pub fn get_end_time_utc(&self) -> Option<DateTime<Utc>> {
        if self.end_time.is_none() {
            return None;
        }

        let naive_time = NaiveDateTime::parse_from_str(&self.end_time.as_ref().unwrap(), "%Y-%m-%dT%H:%M:%S");
        if naive_time.is_err() {
            return None;
        }
        // println!("End Time Helsinki {}", naive_time.unwrap());

        Some(Utc.from_utc_datetime(&Helsinki.from_local_datetime(&naive_time.unwrap())
            .unwrap()
            .naive_utc()))
    }

    pub fn is_match(&self, time: DateTime<Utc>) -> bool {
        let start_time = &self.get_start_time_utc();
        if start_time.is_none() { return false }
        let start_time = start_time.unwrap();

        // If time is before start_time
        if time < start_time { return false }

        let end_time = &self.get_end_time_utc();
        if end_time.is_none() { return true }
        let end_time = end_time.unwrap();

        time <= end_time
    }

    pub fn get_value(&self, time: DateTime<Utc>) -> f32 {
        let time_or_seasonal = self.time_or_seasonal;

        if time_or_seasonal {
            if self.time_config.is_none() {
                return 0.0;
            }

            let time_config = self.time_config.as_ref().unwrap();
            let time_start = time_config.start_hour.unwrap_or(22);
            let time_end = time_config.end_hour.unwrap_or(7);

            let local = time.with_timezone(&Helsinki);
            let hour = local.hour();
            return if hour < time_end || hour >= time_start {
                time_config.night_fee
            } else {
                time_config.day_fee
            }
        }

        if self.seasonal_config.is_none() {
            return 0.0;
        }

        self.seasonal_config
            .as_ref()
            .unwrap()
            .fee
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TimeSetting {
    start_hour: Option<u32>,
    end_hour: Option<u32>,
    night_fee: f32,
    day_fee: f32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SeasonalSetting {
    fee: f32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EnergyFeeSetting {
    start_time: String,
    end_time: Option<String>,
    stock_exchange_or_fixed: bool,
    stock_exchange_config: Option<StockExchangeSetting>,
    fixed_config: Option<FixedSetting>,
}

impl EnergyFeeSetting {
    pub fn get_start_time_utc(&self) -> Option<DateTime<Utc>> {
        let naive_time = NaiveDateTime::parse_from_str(&self.start_time, "%Y-%m-%dT%H:%M:%S");
        if naive_time.is_err() {
            return None;
        }
        // println!("Start Time Helsinki {}", naive_time.unwrap());

        Some(Utc.from_utc_datetime(&Helsinki.from_local_datetime(&naive_time.unwrap())
            .unwrap()
            .naive_utc()))
    }

    pub fn get_end_time_utc(&self) -> Option<DateTime<Utc>> {
        if self.end_time.is_none() {
            return None;
        }

        let naive_time = NaiveDateTime::parse_from_str(&self.end_time.as_ref().unwrap(), "%Y-%m-%dT%H:%M:%S");
        if naive_time.is_err() {
            return None;
        }
        // println!("End Time Helsinki {}", naive_time.unwrap());

        Some(Utc.from_utc_datetime(&Helsinki.from_local_datetime(&naive_time.unwrap())
            .unwrap()
            .naive_utc()))
    }

    pub fn is_match(&self, time: DateTime<Utc>) -> bool {
        let start_time = &self.get_start_time_utc();
        if start_time.is_none() { return false }
        let start_time = start_time.unwrap();

        // If time is before start_time
        if time < start_time { return false }

        let end_time = &self.get_end_time_utc();
        if end_time.is_none() { return true }
        let end_time = end_time.unwrap();

        time <= end_time
    }

    pub fn get_value(&self, spot_price: f32) -> f32 {
        let stock_exchange = self.stock_exchange_or_fixed;

        if stock_exchange {
            if self.stock_exchange_config.is_none() {
                return 0.0;
            }

            let config = self.stock_exchange_config.as_ref().unwrap();

            if spot_price < 0.0 {
                return (spot_price / 10.0) + config.margin
            }

            return (spot_price / 10.0 * config.tax_multiplier) + config.margin
        }

        if self.fixed_config.is_none() {
            return 0.0;
        }

        self.fixed_config
            .as_ref()
            .unwrap()
            .fee
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StockExchangeSetting {
    margin: f32,
    tax_multiplier: f32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FixedSetting {
    fee: f32,
}
