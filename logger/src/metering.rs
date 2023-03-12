use crate::{fetch_and_log_new_consumption_entry, fetch_and_log_new_production_entry, get_access_token};
use actix_web::{post, web, HttpResponse, Responder};
use serde::{Deserialize};

#[derive(Deserialize)]
pub struct TimeParams {
    start: String,
    stop: String,
}

/// Update metering data `/metering`
#[post("/metering")]
pub async fn metering_update(params: web::Json<TimeParams>, client: web::Data<influxdb::Client>) -> impl Responder {
    let mut access_token = dotenv::var("ACCESS_TOKEN").unwrap_or("".to_string());
    let wattivahti_username = dotenv::var("WATTIVAHTI_USERNAME").unwrap_or("".to_string());
    let wattivahti_password = dotenv::var("WATTIVAHTI_PASSWORD").unwrap_or("".to_string());
    let wattivahti_token_endpoint = dotenv::var("WATTIVAHTI_TOKEN_ENDPOINT").unwrap_or("".to_string());
    let consumption_metering_point_code = dotenv::var("CONSUMPTION_METERING_POINT_CODE").unwrap();
    let production_metering_point_code = dotenv::var("PRODUCTION_METERING_POINT_CODE").unwrap();

    // If credentials provided, use those instead of the given access token (if access token was even given)
    if !wattivahti_username.is_empty() {
        let result = get_access_token(&wattivahti_token_endpoint, &wattivahti_username, &wattivahti_password).await;
        if result.is_err() {
            return HttpResponse::InternalServerError().body(format!("Error {:?}", "Logging {} - {} failed because of accessToken"))
        }
        let result = result.unwrap();
        if result.access_token.is_none() {
            return HttpResponse::InternalServerError().body(format!("Error {:?}", "Logging {} - {} failed because of accessToken"))
        }
        access_token = result.access_token.unwrap();
    }

    fetch_and_log_new_production_entry(
        &client,
        &access_token,
        &production_metering_point_code,
        &params.start,
        &params.stop,
    )
        .await;

    fetch_and_log_new_consumption_entry(
        &client,
        &access_token,
        &consumption_metering_point_code,
        &params.start,
        &params.stop,
    )
        .await;

    return HttpResponse::Ok().body("ok")
}
