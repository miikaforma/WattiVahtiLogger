pub mod models;

use http::{StatusCode, header::USER_AGENT, header::AUTHORIZATION};
use dotenv::dotenv;
pub use models::*;

const API_URL: &str = r#"https://porienergia-prod-agent.frendsapp.com:9999/api/onlineapi/v1/"#;

pub async fn get_production_data(access_token: &str, metering_point_code: &str, start: &str, stop: &str) -> Result<ConsumptionsResult, anyhow::Error> {
    let res = reqwest::Client::new()
        .get(format!("{}meterdata2?meteringPointCode={}&measurementType=6&start={}&stop={}&resultStep=PT1H", API_URL, metering_point_code, start, stop))
        .header(USER_AGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/109.0")
        .header(AUTHORIZATION, format!("Bearer {}", access_token))
        .header("Host", "porienergia-prod-agent.frendsapp.com:9999")
        .header("Origin", "https://www.wattivahti.fi")
        .header("Referer", "https://www.wattivahti.fi/")
        .send()
        .await?;

    let status = res.status();

    let data_str = res
        .text()
        .await?;
    //println!("{}", data_str);

    if status != StatusCode::OK {
        return Err(anyhow::anyhow!(data_str));
    }

    let data: ConsumptionsResult = serde_json::from_str(&data_str)?;
    //println!("ConsumptionResult: {:#?}", data);

    Ok(data)
}

pub async fn get_consumption_data(access_token: &str, metering_point_code: &str, start: &str, stop: &str) -> Result<ConsumptionsResult, anyhow::Error> {
    let res = reqwest::Client::new()
        .get(format!("{}meterdata2?meteringPointCode={}&measurementType=1&start={}&stop={}&resultStep=PT1H", API_URL, metering_point_code, start, stop))
        .header(USER_AGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/109.0")
        .header(AUTHORIZATION, format!("Bearer {}", access_token))
        .header("Host", "porienergia-prod-agent.frendsapp.com:9999")
        .header("Origin", "https://www.wattivahti.fi")
        .header("Referer", "https://www.wattivahti.fi/")
        .send()
        .await?;

    let status = res.status();

    let data_str = res
        .text()
        .await?;
    //println!("{}", data_str);

    if status != StatusCode::OK {
        return Err(anyhow::anyhow!(data_str));
    }

    let data: ConsumptionsResult = serde_json::from_str(&data_str)?;
    //println!("ConsumptionsResult: {:#?}", data);

    Ok(data)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use chrono::{NaiveDateTime, DateTime, Utc};
    use chrono_tz::Europe::Helsinki;
    use chrono::Duration as ChronoDuration;
    use super::*;

    #[tokio::test]
    async fn test_production_data() {
        dotenv().ok();

        let access_token = dotenv::var("ACCESS_TOKEN").unwrap();
        let metering_point_code = dotenv::var("PRODUCTION_METERING_POINT_CODE").unwrap();
        let start = dotenv::var("START").unwrap();
        let stop = dotenv::var("STOP").unwrap();

        let data: ConsumptionsResult = get_production_data(&access_token, &metering_point_code, &start, &stop).await.unwrap();
        println!("ConsumptionResult: {:#?}", data);
    }

    #[tokio::test]
    async fn test_consumption_data() {
        dotenv().ok();

        let access_token = dotenv::var("ACCESS_TOKEN").unwrap();
        let metering_point_code = dotenv::var("CONSUMPTION_METERING_POINT_CODE").unwrap();
        let start = dotenv::var("START").unwrap();
        let stop = dotenv::var("STOP").unwrap();

        let data: ConsumptionsResult = get_consumption_data(&access_token, &metering_point_code, &start, &stop).await.unwrap();
        println!("ConsumptionResult: {:#?}", data);
    }

    #[tokio::test]
    async fn test_spot_data_data() {
        dotenv().ok();

        let access_token = dotenv::var("ACCESS_TOKEN").unwrap();
        let metering_point_code = dotenv::var("CONSUMPTION_METERING_POINT_CODE").unwrap();
        let start = "2023-02-01T00:00:00";
        let stop = "2023-03-01T00:00:00";

        let data: ConsumptionsResult = get_consumption_data(&access_token, &metering_point_code, &start, &stop).await.unwrap();
        println!("ConsumptionResult: {:#?}", data);
    }

    #[tokio::test]
    async fn test_production_data_struct() {
        dotenv().ok();

        let data_str = r#"
        {
            "getconsumptionsresult": {
                "consumptiondata": {
                    "meteringpointcode": "1337",
                    "sum": {
                        "quantity": 0.06,
                        "start": "2022-08-01T00:00:00",
                        "stop": "2022-09-01T00:00:00",
                        "unit": "kWh"
                    },
                    "timeseries": {
                        "start": "2022-08-01T00:00:00",
                        "stop": "2022-09-01T00:00:00",
                        "resolution": "PT1H",
                        "values": {
                            "tsv": [
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T00:00:00"
                                },
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T01:00:00"
                                },
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T02:00:00"
                                },
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T03:00:00"
                                },
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T04:00:00"
                                },
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T05:00:00"
                                },
                                {
                                    "quantity": 0.02,
                                    "time": "2022-08-01T06:00:00"
                                },
                                {
                                    "quantity": 0.04,
                                    "time": "2022-08-01T07:00:00"
                                }
                            ]
                        }
                    }
                }
            }
        }"#;

        let data: ConsumptionsResult = serde_json::from_str(&data_str).unwrap();
        println!("ConsumptionResult: {:#?}", data);
        println!("Start: {:#?}", data.getconsumptionsresult.consumptiondata.sum.get_start_utc());
        println!("Stop: {:#?}", data.getconsumptionsresult.consumptiondata.sum.get_stop_utc());
    }

    #[tokio::test]
    async fn test_production_data_struct_with_spotdata() {
        dotenv().ok();

        let data_str = r#"
        {
            "getconsumptionsresult": {
                "consumptiondata": {
                    "meteringpointcode": "1337",
                    "sum": {
                        "quantity": 0.06,
                        "start": "2022-08-01T00:00:00",
                        "stop": "2022-09-01T00:00:00",
                        "unit": "kWh"
                    },
                    "timeseries": {
                        "start": "2022-08-01T00:00:00",
                        "stop": "2022-09-01T00:00:00",
                        "resolution": "PT1H",
                        "values": {
                            "tsv": [
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T00:00:00"
                                },
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T01:00:00"
                                },
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T02:00:00"
                                },
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T03:00:00"
                                },
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T04:00:00"
                                },
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T05:00:00"
                                },
                                {
                                    "quantity": 0.02,
                                    "time": "2022-08-01T06:00:00"
                                },
                                {
                                    "quantity": 0.04,
                                    "time": "2022-08-01T07:00:00"
                                }
                            ]
                        }
                    }
                },
                "spotdata": {
                    "sum": {
                        "quantity": 0,
                        "start": "2022-08-01T00:00:00",
                        "stop": "2022-09-01T00:00:00",
                        "unit": "EUR/MWh"
                    },
                    "timeseries": {
                        "start": "2022-08-01T00:00:00",
                        "stop": "2022-09-01T00:00:00",
                        "resolution": "PT1H",
                        "values": {
                            "tsv": [
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T00:00:00"
                                },
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T01:00:00"
                                },
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T02:00:00"
                                },
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T03:00:00"
                                },
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T04:00:00"
                                },
                                {
                                    "quantity": 0,
                                    "time": "2022-08-01T05:00:00"
                                },
                                {
                                    "quantity": 0.02,
                                    "time": "2022-08-01T06:00:00"
                                },
                                {
                                    "quantity": 0.04,
                                    "time": "2022-08-01T07:00:00"
                                }
                            ]
                        }
                    }
                }
            }
        }"#;

        let data: ConsumptionsResult = serde_json::from_str(&data_str).unwrap();
        println!("ConsumptionResult: {:#?}", data);
        println!("Start: {:#?}", data.getconsumptionsresult.consumptiondata.sum.get_start_utc());
        println!("Stop: {:#?}", data.getconsumptionsresult.consumptiondata.sum.get_stop_utc());
        println!("Has spot data: {:#?}", data.getconsumptionsresult.spotdata.is_some());
    }

    #[tokio::test]
    async fn test_production_data_with_daylight_savings() {
        dotenv().ok();

        let data_str = r#"
        {
            "getconsumptionsresult": {
                "consumptiondata": {
                    "meteringpointcode": "1337",
                    "sum": {
                        "quantity": 0.0,
                        "start": "2023-03-26T00:00:00",
                        "stop": "2023-03-27T00:00:00",
                        "unit": "kWh"
                    },
                    "timeseries": {
                        "start": "2023-03-26T00:00:00",
                        "stop": "2023-03-27T00:00:00",
                        "resolution": "PT1H",
                        "values": {
                            "tsv": [
                                {
                                    "quantity": 0.0,
                                    "time": "2023-03-26T00:00:00",
                                    "day": 0,
                                    "night": 0,
                                    "start": "2023-03-01T00:00:00",
                                    "stop": "2023-04-01T00:00:00",
                                    "unit": "EUR/MWh"
                                },
                                {
                                    "quantity": 0.0,
                                    "time": "2023-03-26T01:00:00",
                                    "day": 0,
                                    "night": 0,
                                    "start": "2023-03-01T00:00:00",
                                    "stop": "2023-04-01T00:00:00",
                                    "unit": "EUR/MWh"
                                },
                                {
                                    "quantity": 0.0,
                                    "time": "2023-03-26T02:00:00",
                                    "day": 0,
                                    "night": 0,
                                    "start": "2023-03-01T00:00:00",
                                    "stop": "2023-04-01T00:00:00",
                                    "unit": "EUR/MWh"
                                },
                                {
                                    "quantity": 0.0,
                                    "time": "2023-03-26T03:00:00",
                                    "day": 0,
                                    "night": 0,
                                    "start": "2023-03-01T00:00:00",
                                    "stop": "2023-04-01T00:00:00",
                                    "unit": "EUR/MWh"
                                },
                                {
                                    "quantity": 0.0,
                                    "time": "2023-03-26T04:00:00",
                                    "day": 0,
                                    "night": 0,
                                    "start": "2023-03-01T00:00:00",
                                    "stop": "2023-04-01T00:00:00",
                                    "unit": "EUR/MWh"
                                },
                                {
                                    "quantity": 0.0,
                                    "time": "2023-03-26T05:00:00",
                                    "day": 0,
                                    "night": 0,
                                    "start": "2023-03-01T00:00:00",
                                    "stop": "2023-04-01T00:00:00",
                                    "unit": "EUR/MWh"
                                },
                                {
                                    "quantity": 0.0,
                                    "time": "2023-03-26T07:00:00",
                                    "day": 0,
                                    "night": 0,
                                    "start": "2023-03-01T00:00:00",
                                    "stop": "2023-04-01T00:00:00",
                                    "unit": "EUR/MWh"
                                }
                            ]
                        }
                    }
                }
            }
        }"#;

        let data: ConsumptionsResult = serde_json::from_str(&data_str).unwrap();
        println!("ConsumptionResult: {:#?}", data);
        println!("Start: {:#?}", data.getconsumptionsresult.consumptiondata.sum.get_start_utc());
        println!("Stop: {:#?}", data.getconsumptionsresult.consumptiondata.sum.get_stop_utc());
        println!("Has spot data: {:#?}", data.getconsumptionsresult.spotdata.is_some());

        for (pos, tsv) in data.getconsumptionsresult.consumptiondata.timeseries.values.tsv.iter().enumerate() {
            let time = &tsv.get_timestamp_utc_calculated(pos);
            if time.is_none() {
                println!("Time couldn't be parsed");
                return;
            }

            println!("Logging production UTC: {:?}", time);
        }
    }

    #[tokio::test]
    async fn test_daylight_savings() {
        dotenv().ok();

        let time = "2022-03-27T03:00:00";
        // let time = "2023-03-26T04:00:00";
        let naive_time = NaiveDateTime::parse_from_str(&time, "%Y-%m-%dT%H:%M:%S");
        println!("System Time UTC {}", naive_time.unwrap());

        let converted = Utc.from_utc_datetime(&Helsinki.from_local_datetime(&naive_time.unwrap())
            .unwrap()
            .naive_utc());

        println!("Converted Time Helsinki {}", converted);
    }

    #[tokio::test]
    async fn test_daylight_savings_2() {
        dotenv().ok();

        let start = "2022-03-01T00:00:00";
        let naive_time = NaiveDateTime::parse_from_str(&start, "%Y-%m-%dT%H:%M:%S");
        println!("System Time Helsinki {}", naive_time.unwrap());

        let converted : DateTime<Utc> = Utc.from_utc_datetime(&Helsinki.from_local_datetime(&naive_time.unwrap())
            .unwrap()
            .naive_utc());

        println!("Converted Time UTC {}", converted);

        let new_date : DateTime<Utc> = converted + ChronoDuration::hours(627);

        println!("Converted Time UTC + n hours {}", new_date);
    }
}
