use chrono::{DateTime, Utc};
use influxdb::InfluxDbWriteable;
use serde::{Deserialize, Serialize};

#[derive(Debug, InfluxDbWriteable, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct TimeSeriesValue {
    pub time: DateTime<Utc>,
    #[influxdb(tag)]
    pub meteringpointcode_tag: String,
    #[influxdb(tag)]
    pub measurementtype_tag: String,

    pub meteringpointcode: String,
    pub measurementtype: String,
    pub unit: String,
    pub timestamp: String,
    pub value: f32,
    pub price: f32,

    pub transfer_basic_fee: Option<f32>,
    pub transfer_fee: Option<f32>,
    pub tax_fee: Option<f32>,
    pub basic_fee: Option<f32>,
    pub energy_fee: Option<f32>,

    pub contract_type: i16,
    pub spot_margin: Option<f32>,
    pub tax_percentage: Option<f32>,
}