use chrono_tz::Tz;
use chrono::{TimeZone, DateTime, Utc, Timelike};
use chrono::NaiveDateTime;

pub fn get_timezone() -> Tz {
    let timezone = dotenv::var("CHRONO_TIMEZONE").unwrap_or("Europe/Helsinki".to_string());
    timezone.parse().unwrap()
}

#[allow(dead_code)]
pub fn parse_time_to_utc(time: &str) -> DateTime<Utc> {
    let naive_time = NaiveDateTime::parse_from_str(&time, "%Y-%m-%dT%H:%M:%S");
    if naive_time.is_err() {
        panic!("Invalid time | {}", time)
    }

    Utc.from_utc_datetime(
        &get_timezone()
            .from_local_datetime(&naive_time.unwrap())
            .unwrap()
            .naive_utc(),
    )
}

pub fn get_next_fetch_milliseconds() -> i64 {
    let tz_now: DateTime<Tz> = Utc::now().with_timezone(&get_timezone());
    let mut next = tz_now + chrono::Duration::days(1);

    let fetch_hour: u32 = dotenv::var("FETCH_HOUR")
        .map(|var| var.parse::<u32>())
        .unwrap_or(Ok(6))
        .unwrap();
    let fetch_minutes: u32 = dotenv::var("FETCH_MINUTES")
        .map(|var| var.parse::<u32>())
        .unwrap_or(Ok(0))
        .unwrap();
    next = next.with_hour(fetch_hour).unwrap();
    next = next.with_minute(fetch_minutes).unwrap();
    next = next.with_second(0).unwrap();

    //next.format("%Y-%m-%dT%H:%M:%S").to_string()
    next.timestamp_millis() - tz_now.timestamp_millis()
}

pub fn get_start_stop() -> (String, String) {
    let tz_now: DateTime<Tz> = Utc::now().with_timezone(&get_timezone());

    let start = tz_now - chrono::Duration::days(1);

    (
        start.format("%Y-%m-%dT00:00:00").to_string(),
        tz_now.format("%Y-%m-%dT00:00:00").to_string(),
    )
}

pub fn get_time_after_duration(duration: u64) -> String {
    let tz_now: DateTime<Tz> = Utc::now().with_timezone(&get_timezone());
    let time = tz_now + chrono::Duration::milliseconds(duration as i64);

    time.format("%Y-%m-%dT%H:%M:%S").to_string()
}