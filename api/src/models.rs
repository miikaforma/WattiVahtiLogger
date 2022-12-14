use chrono_tz::Europe::Helsinki;
use serde::{Deserialize, Serialize};
use chrono::TimeZone;
use chrono::{NaiveDateTime, DateTime, Utc};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct ConsumptionsResult {
    pub getconsumptionsresult: GetConsumptionsResult,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct GetConsumptionsResult {
    pub consumptiondata: ConsumptionData,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct ConsumptionData {
    pub meteringpointcode: String,
    pub sum: Sum,
    pub timeseries: TimeSeries,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Sum {
    pub quantity: f32,
    pub start: String,
    pub stop: String,
    pub unit: String,
}

impl Sum {
    pub fn get_start_utc(&self) -> Option<DateTime<Utc>> {
        let naive_time = NaiveDateTime::parse_from_str(&self.start, "%Y-%m-%dT%H:%M:%S");
        if naive_time.is_err() {
            return None;
        }

        Some(Utc.from_utc_datetime(&Helsinki.from_local_datetime(&naive_time.unwrap())
            .unwrap()
            .naive_utc()))
    }
    pub fn get_stop_utc(&self) -> Option<DateTime<Utc>> {
        let naive_time = NaiveDateTime::parse_from_str(&self.stop, "%Y-%m-%dT%H:%M:%S");
        if naive_time.is_err() {
            return None;
        }

        Some(Utc.from_utc_datetime(&Helsinki.from_local_datetime(&naive_time.unwrap())
            .unwrap()
            .naive_utc()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct TimeSeries {
    pub start: String,
    pub stop: String,
    pub resolution: String,
    pub values: Values,
}

impl TimeSeries {
    pub fn get_start_utc(&self) -> Option<DateTime<Utc>> {
        let naive_time = NaiveDateTime::parse_from_str(&self.start, "%Y-%m-%dT%H:%M:%S");
        if naive_time.is_err() {
            return None;
        }

        Some(Utc.from_utc_datetime(&Helsinki.from_local_datetime(&naive_time.unwrap())
            .unwrap()
            .naive_utc()))
    }
    pub fn get_stop_utc(&self) -> Option<DateTime<Utc>> {
        let naive_time = NaiveDateTime::parse_from_str(&self.stop, "%Y-%m-%dT%H:%M:%S");
        if naive_time.is_err() {
            return None;
        }

        Some(Utc.from_utc_datetime(&Helsinki.from_local_datetime(&naive_time.unwrap())
            .unwrap()
            .naive_utc()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Values {
    pub tsv: Vec<TSV>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct TSV {
    pub q: f32,
    pub t: String,
}

impl TSV {
    pub fn get_timestamp_utc(&self) -> Option<DateTime<Utc>> {
        let naive_time = NaiveDateTime::parse_from_str(&self.t, "%Y-%m-%dT%H:%M:%S");
        if naive_time.is_err() {
            return None;
        }
        // println!("System Time UTC {}", naive_time.unwrap());

        Some(Utc.from_utc_datetime(&Helsinki.from_local_datetime(&naive_time.unwrap())
            .unwrap()
            .naive_utc()))
    }
}
