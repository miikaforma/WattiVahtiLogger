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
    info!("Loading {}", path.as_ref().to_string_lossy());
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

        // debug!("Settings {:#?}", settings);

        if let Err(err) = settings.validate() {
            panic!("Validation error: {}", err);
        }

        let dt = DateTime::<Utc>::from_utc(NaiveDateTime::parse_from_str("2019-12-31T22:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(), Utc);
        let contract = settings.consumption.get_contract(dt).unwrap();

        info!("Contract {:#?}", contract);
    }
}
