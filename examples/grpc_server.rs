use std::net::SocketAddr;

use openapi_rs::BrokerGrpcAdapterServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let upstream = std::env::var("BROKER_OPENAPI_GRPC_URL")?;
    let listen_addr = std::env::var("OPENAPI_PROXY_LISTEN_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50051".to_string());

    let grpc = BrokerGrpcAdapterServer::connect(upstream.clone()).await?;
    let addr: SocketAddr = listen_addr.parse()?;

    println!("broker adapter listening on {addr}");
    println!("forwarding to broker gRPC upstream: {upstream}");
    Server::builder()
        .add_service(grpc.into_tonic_service())
        .serve(addr)
        .await?;

    Ok(())
}
