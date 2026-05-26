# openapi-rs-sdk

Real gRPC + protobuf trading SDK in Rust with symbol-aware lot validation.

## Features

- Symbol metadata registry (`contract_size`, min/max lot, step)
- Lot-size-only volume quoting (`symbol + lot_size -> volume`)
- Auth flow (`register_user`, `login`, `logout`)
- Account flow (`fetch_balance`)
- Trading flow (`place_market_order`)
- Protobuf contract and generated tonic server/client types
- Async SDK client wrapper (`OpenApiSdkClient`)

## Project Layout

- `proto/openapi.proto`: protobuf contract
- `build.rs`: tonic/prost code generation at build time
- `src/lib.rs`: domain engine, service logic, gRPC server adapter, SDK client
- `examples/grpc_server.rs`: runnable local server
- `examples/grpc_client.rs`: runnable local client

## Requirements

- Rust stable (edition 2024)
- Cargo

## Quick Start

1. Run tests:

```bash
cargo test
```

2. Start local server:

```bash
cargo run --example grpc_server
```

3. In another terminal, run SDK client:

```bash
cargo run --example grpc_client
```

You should see quote, order execution, and balance output.

## SDK Client Usage

```rust
use openapi_rs::{OpenApiSdkClient, OrderSide};
use rust_decimal::Decimal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OpenApiSdkClient::connect("http://127.0.0.1:50051").await?;

    let token = client.login("alice", "secret").await?;

    let quote = client.quote_volume("EURUSD", Decimal::new(15, 2)).await?;
    println!("quoted volume: {}", quote.volume);

    let execution = client
        .place_market_order(token.clone(), "EURUSD", Decimal::new(15, 2), OrderSide::Buy)
        .await?;
    println!("order id: {}", execution.order_id);

    let balance = client.fetch_balance(token.clone()).await?;
    println!("free margin: {}", balance.free_margin);

    client.logout(token).await?;
    Ok(())
}
```

## API Surface

### Domain Engine

- `OpenApiEngine::register_symbol`
- `OpenApiEngine::volume_from_lot_size`
- `OpenApiEngine::quote_volume_proto`

### Service Logic

- `OpenApiService::register_user`
- `OpenApiService::login`
- `OpenApiService::logout`
- `OpenApiService::fetch_balance`
- `OpenApiService::place_market_order`

### gRPC Transport

- Server adapter: `OpenApiGrpcServer`
- Generated tonic service trait: `pb::open_api_service_server::OpenApiService`
- SDK client wrapper: `OpenApiSdkClient`

## Symbol and Lot Rules

For each symbol, configure:

- `contract_size`
- `min_lot`
- `max_lot`
- `lot_step`

Validation rules:

- `lot_size > 0`
- `min_lot <= lot_size <= max_lot`
- `(lot_size - min_lot) % lot_step == 0`

If symbol is missing, API returns not-found.

## Protobuf Notes

- Proto file: `proto/openapi.proto`
- Decimal numbers are represented as strings in protobuf messages
- Order side values:
  - `ORDER_SIDE_UNSPECIFIED`
  - `ORDER_SIDE_BUY`
  - `ORDER_SIDE_SELL`

## Error Mapping

Domain errors map to gRPC status codes in server adapter:

- `SymbolNotFound` -> `not_found`
- `InvalidCredentials`, `SessionNotFound` -> `unauthenticated`
- `UserAlreadyExists`, `AccountAlreadyExists` -> `already_exists`
- `InsufficientMargin` -> `failed_precondition`
- validation failures -> `invalid_argument`

## Generate Docs

Build Rust API docs:

```bash
cargo doc --no-deps --open
```

## Publish To crates.io

1. Ensure your crate name is available:

```bash
cargo search openapi-rs-sdk --limit 5
```

2. Package validation:

```bash
cargo package
cargo publish --dry-run
```

3. Login and publish:

```bash
cargo login <CRATES_IO_TOKEN>
cargo publish
```

4. Release updates:
  - bump version in `Cargo.toml`
  - tag in git (recommended)
  - run `cargo publish` again

## Use In Other Projects

### From crates.io (recommended)

After publishing, in another Rust project:

```bash
cargo add openapi-rs-sdk
```

or in `Cargo.toml`:

```toml
[dependencies]
openapi-rs-sdk = "0.1"
```

### Directly from GitHub (before first publish)

```toml
[dependencies]
openapi-rs-sdk = { git = "https://github.com/elsadeny/openapi-rs", tag = "v0.1.0" }
```

You can also pin a commit:

```toml
[dependencies]
openapi-rs-sdk = { git = "https://github.com/elsadeny/openapi-rs", rev = "<commit-sha>" }

Import in Rust code:

```rust
use openapi_rs::{OpenApiSdkClient, OrderSide};
```
```

## Production Notes

- Current storage is in-memory for users/sessions/accounts
- Session tokens are simple IDs (`sess-N`) for local/dev usage
- Add persistent storage and stronger auth/token strategy for production
