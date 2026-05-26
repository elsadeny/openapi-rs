use openapi_rs::{OpenApiSdkClient, OrderSide};
use rust_decimal::Decimal;

fn d(value: i64, scale: u32) -> Decimal {
    Decimal::new(value, scale)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let grpc_url = std::env::var("OPENAPI_GRPC_URL")?;
    let mut client = OpenApiSdkClient::connect(grpc_url.clone()).await?;
    println!("connected to {grpc_url}");

    let token = client.login("alice", "secret").await?;

    let quote = client.quote_volume("EURUSD", d(15, 2)).await?;
    println!(
        "quote => symbol={}, lot={}, volume={}",
        quote.symbol, quote.lot_size, quote.volume
    );

    let execution = client
        .place_market_order(token.clone(), "EURUSD", d(1, 2), OrderSide::Sell)
        .await?;
    println!(
        "order => id={}, symbol={}, volume={}, margin={}",
        execution.order_id, execution.symbol, execution.volume, execution.margin_used
    );

    let positions = client.fetch_open_positions(token.clone()).await?;
    println!("open positions => {}", positions.len());

    let balance = client.fetch_balance(token.clone()).await?;
    println!(
        "balance => total={}, used_margin={}, free_margin={}",
        balance.balance, balance.used_margin, balance.free_margin
    );

    client.logout(token).await?;

    Ok(())
}
