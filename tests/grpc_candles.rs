use chrono::{DateTime, Utc};
use openapi_rs::{OpenApiEngine, OpenApiGrpcServer, OpenApiSdkClient, OpenApiService, SymbolSpec, Timeframe};
use rust_decimal::Decimal;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

fn d(value: i64, scale: u32) -> Decimal {
    Decimal::new(value, scale)
}

fn sample_engine() -> OpenApiEngine {
    OpenApiEngine::with_symbols([
        (
            "EURUSD",
            SymbolSpec {
                contract_size: d(100_000, 0),
                min_lot: d(1, 2),
                max_lot: d(1000, 0),
                lot_step: d(1, 2),
            },
        ),
        (
            "XAUUSD",
            SymbolSpec {
                contract_size: d(100, 0),
                min_lot: d(1, 1),
                max_lot: d(500, 0),
                lot_step: d(1, 1),
            },
        ),
    ])
}

async fn start_server() -> (tokio::task::JoinHandle<Result<(), tonic::transport::Error>>, String) {
    let mut service = OpenApiService::new(sample_engine());
    service
        .register_user("alice", "secret", "ACC-001", d(100_000, 0))
        .expect("user must be registered");

    let server = OpenApiGrpcServer::new(service);

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let incoming = TcpListenerStream::new(listener);

    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(server.into_tonic_service())
            .serve_with_incoming(incoming)
            .await
    });

    (handle, format!("http://{addr}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_get_candles_returns_requested_count() {
    let (handle, endpoint) = start_server().await;

    let mut client = OpenApiSdkClient::connect(endpoint)
        .await
        .expect("client should connect");

    let candles = client
        .get_candles("EURUSD", Timeframe::M1, None, None, 8)
        .await
        .expect("get_candles should succeed");

    assert_eq!(candles.len(), 8);
    assert!(candles.iter().all(|c| c.symbol == "EURUSD"));

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_stream_candles_are_time_ordered() {
    let (handle, endpoint) = start_server().await;

    let mut client = OpenApiSdkClient::connect(endpoint)
        .await
        .expect("client should connect");

    let mut stream = client
        .stream_candles("EURUSD", Timeframe::M1, None, None, 12)
        .await
        .expect("stream_candles should succeed");

    let mut open_times: Vec<DateTime<Utc>> = Vec::new();
    while let Some(item) = stream.next().await {
        let msg = item.expect("stream item should be valid");
        let candle = msg.candle.expect("candle payload should exist");
        let open_time = DateTime::parse_from_rfc3339(&candle.open_time)
            .expect("open_time should be valid RFC3339")
            .with_timezone(&Utc);
        open_times.push(open_time);
    }

    assert_eq!(open_times.len(), 12);
    let is_sorted = open_times
        .windows(2)
        .all(|window| window[0] < window[1]);
    assert!(is_sorted, "streamed candles must be strictly ascending by open_time");

    handle.abort();
}
