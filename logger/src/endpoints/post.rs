use crate::{
    app::{fetch_consumption_for_interval, fetch_production_for_interval}, get_access_token, storage::timescaledb::timescale::{refresh_consumption_views, refresh_production_views}
};
use actix_web::{post, web, HttpResponse, Responder};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct TimeParams {
    start: String,
    stop: String,
    #[serde(default = "default_resolution")]
    resolution: String,
}

fn default_resolution() -> String {
    "PT1H".to_string()
}

/// Update metering data `/metering`
#[post("/metering")]
pub async fn metering_update(
    params: web::Json<TimeParams>,
) -> impl Responder {
    let mut access_token = dotenv::var("ACCESS_TOKEN").unwrap_or("".to_string());
    let wattivahti_username = dotenv::var("WATTIVAHTI_USERNAME").unwrap_or("".to_string());
    let wattivahti_password = dotenv::var("WATTIVAHTI_PASSWORD").unwrap_or("".to_string());
    let wattivahti_token_endpoint =
        dotenv::var("WATTIVAHTI_TOKEN_ENDPOINT").unwrap_or("".to_string());

    // If credentials provided, use those instead of the given access token (if access token was even given)
    if !wattivahti_username.is_empty() {
        let result = get_access_token(
            &wattivahti_token_endpoint,
            &wattivahti_username,
            &wattivahti_password,
        )
        .await;
        if result.is_err() {
            return HttpResponse::InternalServerError().body(format!(
                "Error {:?}",
                "Logging {} - {} failed because of accessToken"
            ));
        }
        let result = result.unwrap();
        if result.access_token.is_none() {
            return HttpResponse::InternalServerError().body(format!(
                "Error {:?}",
                "Logging {} - {} failed because of accessToken"
            ));
        }
        access_token = result.access_token.unwrap();
    }

    if let Err(err) = fetch_consumption_for_interval(
        &access_token,
        &params.start,
        &params.stop,
        &params.resolution,
    )
    .await
    {
        // Handle the error here
        error!("Error fetching consumptions: {:?}", err);
        // Return an appropriate response
        return HttpResponse::InternalServerError().body(err.to_string());
    }

    if let Err(err) = fetch_production_for_interval(
        &access_token,
        &params.start,
        &params.stop,
        &params.resolution,
    )
    .await
    {
        // Handle the error here
        error!("Error fetching productions: {:?}", err);
        // Return an appropriate response
        return HttpResponse::InternalServerError().body(err.to_string());
    }

    if let Err(err) = refresh_consumption_views().await {
        // Handle the error here
        error!("Error refreshing the consumption views: {:?}", err);
        // Return an appropriate response
        return HttpResponse::InternalServerError().body(err.to_string());
    }

    if let Err(err) = refresh_production_views().await {
        // Handle the error here
        error!("Error refreshing the production views: {:?}", err);
        // Return an appropriate response
        return HttpResponse::InternalServerError().body(err.to_string());
    }

    return HttpResponse::Ok().body("ok");
}
