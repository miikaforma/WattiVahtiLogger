use std::fs::File;
use std::io::Read;
use std::path::Path;
use thiserror::Error;
use crate::settings::config_model::SettingsConfig;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to deserialize config.")]
    Serde(#[from] serde_yaml::Error),
    #[error("Failed to open config file")]
    Io(#[from] std::io::Error),
}

pub fn load_settings(path: impl AsRef<Path>) -> Result<SettingsConfig, ConfigError> {
    println!("Loading {}", path.as_ref().to_string_lossy());
    let mut file = File::open(path)?;
    let mut s = String::new();
    file.read_to_string(&mut s)?;
    let t: SettingsConfig = serde_yaml::from_str(&s)?;

    Ok(t)
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDateTime, Utc};
    use super::*;

    #[tokio::test]
    async fn test_load_settings() {
        let settings = load_settings(format!("configs/{}.yaml", "test"))
            .expect("Failed to load settings file.");

        // println!("Settings {:#?}", settings);

        if let Err(err) = settings.validate() {
            panic!("Validation error: {}", err);
        }

        let dt = DateTime::<Utc>::from_utc(NaiveDateTime::parse_from_str("2019-12-31T22:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(), Utc);
        let contract = settings.get_contract(dt).unwrap();

        println!("Contract {:#?}", contract);

        /*let dt = DateTime::<Utc>::from_utc(NaiveDateTime::parse_from_str("2222-01-01T22:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(), Utc);
        println!("PRODUCTION_TRANSFER_FEE {:#?}", settings.get_production_transfer_fee(dt));*/

        // let dt = DateTime::<Utc>::from_utc(NaiveDateTime::parse_from_str("2019-12-31T22:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(), Utc);
        // assert_eq!(settings.get_production_transfer_fee(dt), 1.0);

        // let dt = DateTime::<Utc>::from_utc(NaiveDateTime::parse_from_str("2019-12-31T21:59:59", "%Y-%m-%dT%H:%M:%S").unwrap(), Utc);
        // assert_eq!(settings.get_production_transfer_fee(dt), 0.09);

        // let dt = DateTime::<Utc>::from_utc(NaiveDateTime::parse_from_str("2020-06-02T21:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(), Utc);
        // assert_eq!(settings.get_production_transfer_fee(dt), 1.20);

        // let dt = DateTime::<Utc>::from_utc(NaiveDateTime::parse_from_str("2021-01-01T21:59:59", "%Y-%m-%dT%H:%M:%S").unwrap(), Utc);
        // assert_eq!(settings.get_production_transfer_fee(dt), 1.20);

        // let dt = DateTime::<Utc>::from_utc(NaiveDateTime::parse_from_str("2021-01-01T22:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(), Utc);
        // assert_eq!(settings.get_production_transfer_fee(dt), 0.09);

        // let dt = DateTime::<Utc>::from_utc(NaiveDateTime::parse_from_str("2222-01-01T22:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(), Utc);
        // assert_eq!(settings.get_production_transfer_fee(dt), 99.99);
    }
}
