use std::net::SocketAddr;

use openapi_rs::{OpenApiEngine, OpenApiGrpcServer, OpenApiService, SymbolSpec};
use rust_decimal::Decimal;
use tonic::transport::Server;

fn d(value: i64, scale: u32) -> Decimal {
    Decimal::new(value, scale)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = OpenApiEngine::new();
    engine.register_symbol(
        "EURUSD",
        SymbolSpec {
            contract_size: d(100_000, 0),
            min_lot: d(1, 2),
            max_lot: d(1000, 0),
            lot_step: d(1, 2),
        },
    );
    engine.register_symbol(
        "XAUUSD",
        SymbolSpec {
            contract_size: d(100, 0),
            min_lot: d(1, 1),
            max_lot: d(500, 0),
            lot_step: d(1, 1),
        },
    );

    let mut service = OpenApiService::new(engine);
    service.register_user("alice", "secret", "ACC-001", d(100_000, 0))?;

    let grpc = OpenApiGrpcServer::new(service);
    let addr: SocketAddr = "127.0.0.1:50051".parse()?;

    println!("gRPC server listening on {addr}");
    Server::builder()
        .add_service(grpc.into_tonic_service())
        .serve(addr)
        .await?;

    Ok(())
}
