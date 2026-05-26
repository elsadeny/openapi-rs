use openapi_rs::{OpenApiSdkClient, OrderSide};
use rust_decimal::Decimal;

fn d(value: i64, scale: u32) -> Decimal {
    Decimal::new(value, scale)
}

#[tokio::test]
#[ignore = "requires live broker adapter and credentials via env"]
async fn broker_smoke_login_order_balance_positions() {
    let grpc_url = std::env::var("OPENAPI_GRPC_URL")
        .expect("OPENAPI_GRPC_URL must point to broker-connected adapter server");
    let username = std::env::var("OPENAPI_USERNAME").expect("OPENAPI_USERNAME is required");
    let password = std::env::var("OPENAPI_PASSWORD").expect("OPENAPI_PASSWORD is required");
    let symbol = std::env::var("OPENAPI_SMOKE_SYMBOL").unwrap_or_else(|_| "EURUSD".to_string());

    let mut client = OpenApiSdkClient::connect(grpc_url)
        .await
        .expect("client should connect");

    let token = client
        .login(username, password)
        .await
        .expect("login should succeed");

    let execution = client
        .place_market_order(token.clone(), symbol.clone(), d(1, 2), OrderSide::Sell)
        .await
        .expect("0.01 SELL should be accepted by broker");
    assert!(execution.order_id > 0, "expected broker order id to be populated");

    let balance = client
        .fetch_balance(token.clone())
        .await
        .expect("fetch_balance should succeed after order placement");
    assert!(!balance.account_id.is_empty(), "account id should be returned");

    let positions = client
        .fetch_open_positions(token.clone())
        .await
        .expect("fetch_open_positions should succeed");
    assert!(
        positions.iter().any(|p| p.symbol == symbol),
        "expected at least one open position for {symbol}"
    );

    client.logout(token).await.expect("logout should succeed");
}
