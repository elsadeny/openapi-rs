//! openapi-rs: protobuf/gRPC trading SDK primitives.
//!
//! This crate provides:
//! - symbol-aware lot-size validation and volume quoting
//! - auth/session/account service logic
//! - tonic server adapter (`OpenApiGrpcServer`)
//! - typed async SDK client (`OpenApiSdkClient`)
//!
//! # Example
//!
//! ```no_run
//! use openapi_rs::{OpenApiSdkClient, OrderSide};
//! use rust_decimal::Decimal;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut client = OpenApiSdkClient::connect("http://127.0.0.1:50051").await?;
//!     let token = client.login("alice", "secret").await?;
//!     let _quote = client.quote_volume("EURUSD", Decimal::new(10, 2)).await?;
//!     let _execution = client
//!         .place_market_order(token.clone(), "EURUSD", Decimal::new(10, 2), OrderSide::Buy)
//!         .await?;
//!     let _balance = client.fetch_balance(token.clone()).await?;
//!     client.logout(token).await?;
//!     Ok(())
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rust_decimal::Decimal;
use thiserror::Error;
use tokio::sync::Mutex as TokioMutex;
use tonic::{Request, Response, Status};

pub mod pb {
    tonic::include_proto!("openapi");
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolSpec {
    pub contract_size: Decimal,
    pub min_lot: Decimal,
    pub max_lot: Decimal,
    pub lot_step: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeQuote {
    pub symbol: String,
    pub lot_size: Decimal,
    pub volume: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRequest {
    pub symbol: String,
    pub lot_size: Decimal,
    pub side: OrderSide,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderExecution {
    pub order_id: u64,
    pub account_id: String,
    pub symbol: String,
    pub lot_size: Decimal,
    pub volume: Decimal,
    pub side: OrderSide,
    pub margin_used: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountBalance {
    pub account_id: String,
    pub balance: Decimal,
    pub used_margin: Decimal,
    pub free_margin: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Account {
    account_id: String,
    balance: Decimal,
    used_margin: Decimal,
}

impl Account {
    fn free_margin(&self) -> Decimal {
        self.balance - self.used_margin
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UserRecord {
    password: String,
    account_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionRecord {
    username: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OpenApiError {
    #[error("symbol '{0}' not found")]
    SymbolNotFound(String),
    #[error("lot size must be greater than zero")]
    NonPositiveLotSize,
    #[error("lot size {lot_size} is outside allowed range [{min_lot}, {max_lot}]")]
    LotOutOfRange {
        lot_size: Decimal,
        min_lot: Decimal,
        max_lot: Decimal,
    },
    #[error("lot size {lot_size} does not match lot step {lot_step} from min lot {min_lot}")]
    InvalidLotStep {
        lot_size: Decimal,
        lot_step: Decimal,
        min_lot: Decimal,
    },
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("session token not found")]
    SessionNotFound,
    #[error("user '{0}' already exists")]
    UserAlreadyExists(String),
    #[error("account '{0}' already exists")]
    AccountAlreadyExists(String),
    #[error("account '{0}' not found")]
    AccountNotFound(String),
    #[error("insufficient margin: required {required}, available {available}")]
    InsufficientMargin { required: Decimal, available: Decimal },
    #[error("invalid decimal for field '{field}': '{value}'")]
    InvalidDecimalField { field: &'static str, value: String },
    #[error("unsupported order side: {0}")]
    UnsupportedOrderSide(i32),
    #[error("grpc status: {0}")]
    GrpcStatus(String),
}

#[derive(Debug, Default)]
pub struct OpenApiEngine {
    symbols: HashMap<String, SymbolSpec>,
}

impl OpenApiEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_symbols(symbols: impl IntoIterator<Item = (impl Into<String>, SymbolSpec)>) -> Self {
        let mut engine = Self::new();
        for (symbol, spec) in symbols {
            engine.register_symbol(symbol, spec);
        }
        engine
    }

    pub fn register_symbol(&mut self, symbol: impl Into<String>, spec: SymbolSpec) {
        self.symbols.insert(normalize_symbol(symbol), spec);
    }

    pub fn volume_from_lot_size(
        &self,
        symbol: impl AsRef<str>,
        lot_size: Decimal,
    ) -> Result<VolumeQuote, OpenApiError> {
        if lot_size <= Decimal::ZERO {
            return Err(OpenApiError::NonPositiveLotSize);
        }

        let normalized_symbol = normalize_symbol(symbol.as_ref());
        let spec = self
            .symbols
            .get(&normalized_symbol)
            .ok_or_else(|| OpenApiError::SymbolNotFound(symbol.as_ref().to_string()))?;

        if lot_size < spec.min_lot || lot_size > spec.max_lot {
            return Err(OpenApiError::LotOutOfRange {
                lot_size,
                min_lot: spec.min_lot,
                max_lot: spec.max_lot,
            });
        }

        let offset = lot_size - spec.min_lot;
        let remainder = offset % spec.lot_step;
        if !remainder.is_zero() {
            return Err(OpenApiError::InvalidLotStep {
                lot_size,
                lot_step: spec.lot_step,
                min_lot: spec.min_lot,
            });
        }

        Ok(VolumeQuote {
            symbol: normalized_symbol,
            lot_size,
            volume: lot_size * spec.contract_size,
        })
    }

    pub fn quote_volume_proto(
        &self,
        request: pb::QuoteVolumeRequest,
    ) -> Result<pb::QuoteVolumeResponse, OpenApiError> {
        let lot_size = parse_decimal_field("lot_size", &request.lot_size)?;
        let quote = self.volume_from_lot_size(request.symbol, lot_size)?;

        Ok(pb::QuoteVolumeResponse {
            quote: Some(pb::VolumeQuote {
                symbol: quote.symbol,
                lot_size: quote.lot_size.to_string(),
                volume: quote.volume.to_string(),
            }),
        })
    }
}

#[derive(Debug)]
pub struct OpenApiService {
    engine: OpenApiEngine,
    users: HashMap<String, UserRecord>,
    sessions: HashMap<String, SessionRecord>,
    accounts: HashMap<String, Account>,
    next_session_id: AtomicU64,
    next_order_id: AtomicU64,
    leverage: Decimal,
}

impl OpenApiService {
    pub fn new(engine: OpenApiEngine) -> Self {
        Self {
            engine,
            users: HashMap::new(),
            sessions: HashMap::new(),
            accounts: HashMap::new(),
            next_session_id: AtomicU64::new(1),
            next_order_id: AtomicU64::new(1),
            leverage: Decimal::new(100, 0),
        }
    }

    pub fn register_user(
        &mut self,
        username: impl AsRef<str>,
        password: impl Into<String>,
        account_id: impl Into<String>,
        initial_balance: Decimal,
    ) -> Result<(), OpenApiError> {
        let normalized_username = normalize_username(username.as_ref());
        if self.users.contains_key(&normalized_username) {
            return Err(OpenApiError::UserAlreadyExists(username.as_ref().to_string()));
        }

        let account_id = account_id.into();
        if self.accounts.contains_key(&account_id) {
            return Err(OpenApiError::AccountAlreadyExists(account_id));
        }

        self.accounts.insert(
            account_id.clone(),
            Account {
                account_id: account_id.clone(),
                balance: initial_balance,
                used_margin: Decimal::ZERO,
            },
        );

        self.users.insert(
            normalized_username,
            UserRecord {
                password: password.into(),
                account_id,
            },
        );

        Ok(())
    }

    pub fn login(
        &mut self,
        username: impl AsRef<str>,
        password: impl AsRef<str>,
    ) -> Result<String, OpenApiError> {
        let normalized_username = normalize_username(username.as_ref());
        let user = self
            .users
            .get(&normalized_username)
            .ok_or(OpenApiError::InvalidCredentials)?;

        if user.password != password.as_ref() {
            return Err(OpenApiError::InvalidCredentials);
        }

        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        let token = format!("sess-{session_id}");

        self.sessions.insert(
            token.clone(),
            SessionRecord {
                username: normalized_username,
            },
        );

        Ok(token)
    }

    pub fn logout(&mut self, token: impl AsRef<str>) -> Result<(), OpenApiError> {
        if self.sessions.remove(token.as_ref()).is_none() {
            return Err(OpenApiError::SessionNotFound);
        }

        Ok(())
    }

    pub fn fetch_balance(&self, token: impl AsRef<str>) -> Result<AccountBalance, OpenApiError> {
        let account = self.account_for_token(token.as_ref())?;

        Ok(AccountBalance {
            account_id: account.account_id.clone(),
            balance: account.balance,
            used_margin: account.used_margin,
            free_margin: account.free_margin(),
        })
    }

    pub fn place_market_order(
        &mut self,
        token: impl AsRef<str>,
        request: OrderRequest,
    ) -> Result<OrderExecution, OpenApiError> {
        let volume_quote = self
            .engine
            .volume_from_lot_size(&request.symbol, request.lot_size)?;

        let account_id = self.account_id_for_token(token.as_ref())?;
        let account = self
            .accounts
            .get_mut(&account_id)
            .ok_or_else(|| OpenApiError::AccountNotFound(account_id.clone()))?;

        let margin_required = volume_quote.volume / self.leverage;
        let free_margin = account.free_margin();
        if free_margin < margin_required {
            return Err(OpenApiError::InsufficientMargin {
                required: margin_required,
                available: free_margin,
            });
        }

        account.used_margin += margin_required;

        let order_id = self.next_order_id.fetch_add(1, Ordering::Relaxed);

        Ok(OrderExecution {
            order_id,
            account_id,
            symbol: volume_quote.symbol,
            lot_size: volume_quote.lot_size,
            volume: volume_quote.volume,
            side: request.side,
            margin_used: margin_required,
        })
    }

    pub fn register_user_proto(
        &mut self,
        request: pb::RegisterUserRequest,
    ) -> Result<pb::RegisterUserResponse, OpenApiError> {
        let initial_balance = parse_decimal_field("initial_balance", &request.initial_balance)?;

        self.register_user(
            request.username,
            request.password,
            request.account_id,
            initial_balance,
        )?;

        Ok(pb::RegisterUserResponse { success: true })
    }

    pub fn login_proto(
        &mut self,
        request: pb::LoginRequest,
    ) -> Result<pb::LoginResponse, OpenApiError> {
        let session_token = self.login(request.username, request.password)?;
        Ok(pb::LoginResponse { session_token })
    }

    pub fn logout_proto(
        &mut self,
        request: pb::LogoutRequest,
    ) -> Result<pb::LogoutResponse, OpenApiError> {
        self.logout(request.session_token)?;
        Ok(pb::LogoutResponse { success: true })
    }

    pub fn fetch_balance_proto(
        &self,
        request: pb::FetchBalanceRequest,
    ) -> Result<pb::FetchBalanceResponse, OpenApiError> {
        let balance = self.fetch_balance(request.session_token)?;

        Ok(pb::FetchBalanceResponse {
            balance: Some(pb::AccountBalance {
                account_id: balance.account_id,
                balance: balance.balance.to_string(),
                used_margin: balance.used_margin.to_string(),
                free_margin: balance.free_margin.to_string(),
            }),
        })
    }

    pub fn place_market_order_proto(
        &mut self,
        request: pb::PlaceMarketOrderRequest,
    ) -> Result<pb::PlaceMarketOrderResponse, OpenApiError> {
        let lot_size = parse_decimal_field("lot_size", &request.lot_size)?;
        let side = proto_side_to_domain(request.side)?;

        let execution = self.place_market_order(
            request.session_token,
            OrderRequest {
                symbol: request.symbol,
                lot_size,
                side,
            },
        )?;

        Ok(pb::PlaceMarketOrderResponse {
            execution: Some(pb::OrderExecution {
                order_id: execution.order_id,
                account_id: execution.account_id,
                symbol: execution.symbol,
                lot_size: execution.lot_size.to_string(),
                volume: execution.volume.to_string(),
                side: domain_side_to_proto(execution.side) as i32,
                margin_used: execution.margin_used.to_string(),
            }),
        })
    }

    fn account_for_token(&self, token: &str) -> Result<&Account, OpenApiError> {
        let account_id = self.account_id_for_token(token)?;
        self.accounts
            .get(&account_id)
            .ok_or(OpenApiError::AccountNotFound(account_id))
    }

    fn account_id_for_token(&self, token: &str) -> Result<String, OpenApiError> {
        let session = self
            .sessions
            .get(token)
            .ok_or(OpenApiError::SessionNotFound)?;

        let user = self
            .users
            .get(&session.username)
            .ok_or(OpenApiError::InvalidCredentials)?;

        Ok(user.account_id.clone())
    }
}

#[derive(Clone, Debug)]
pub struct OpenApiGrpcServer {
    inner: Arc<TokioMutex<OpenApiService>>,
}

impl OpenApiGrpcServer {
    pub fn new(service: OpenApiService) -> Self {
        Self {
            inner: Arc::new(TokioMutex::new(service)),
        }
    }

    pub fn into_tonic_service(self) -> pb::open_api_service_server::OpenApiServiceServer<Self> {
        pb::open_api_service_server::OpenApiServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl pb::open_api_service_server::OpenApiService for OpenApiGrpcServer {
    async fn register_user(
        &self,
        request: Request<pb::RegisterUserRequest>,
    ) -> Result<Response<pb::RegisterUserResponse>, Status> {
        let mut service = self.inner.lock().await;
        let response = service
            .register_user_proto(request.into_inner())
            .map_err(open_api_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn login(
        &self,
        request: Request<pb::LoginRequest>,
    ) -> Result<Response<pb::LoginResponse>, Status> {
        let mut service = self.inner.lock().await;
        let response = service
            .login_proto(request.into_inner())
            .map_err(open_api_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn logout(
        &self,
        request: Request<pb::LogoutRequest>,
    ) -> Result<Response<pb::LogoutResponse>, Status> {
        let mut service = self.inner.lock().await;
        let response = service
            .logout_proto(request.into_inner())
            .map_err(open_api_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn fetch_balance(
        &self,
        request: Request<pb::FetchBalanceRequest>,
    ) -> Result<Response<pb::FetchBalanceResponse>, Status> {
        let service = self.inner.lock().await;
        let response = service
            .fetch_balance_proto(request.into_inner())
            .map_err(open_api_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn place_market_order(
        &self,
        request: Request<pb::PlaceMarketOrderRequest>,
    ) -> Result<Response<pb::PlaceMarketOrderResponse>, Status> {
        let mut service = self.inner.lock().await;
        let response = service
            .place_market_order_proto(request.into_inner())
            .map_err(open_api_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn quote_volume(
        &self,
        request: Request<pb::QuoteVolumeRequest>,
    ) -> Result<Response<pb::QuoteVolumeResponse>, Status> {
        let service = self.inner.lock().await;
        let response = service
            .engine
            .quote_volume_proto(request.into_inner())
            .map_err(open_api_error_to_status)?;
        Ok(Response::new(response))
    }
}

#[derive(Clone, Debug)]
pub struct OpenApiSdkClient {
    inner: pb::open_api_service_client::OpenApiServiceClient<tonic::transport::Channel>,
}

impl OpenApiSdkClient {
    pub async fn connect(dst: impl Into<String>) -> Result<Self, tonic::transport::Error> {
        let inner = pb::open_api_service_client::OpenApiServiceClient::connect(dst.into()).await?;
        Ok(Self { inner })
    }

    pub async fn register_user(
        &mut self,
        username: impl Into<String>,
        password: impl Into<String>,
        account_id: impl Into<String>,
        initial_balance: Decimal,
    ) -> Result<(), OpenApiError> {
        self.inner
            .register_user(pb::RegisterUserRequest {
                username: username.into(),
                password: password.into(),
                account_id: account_id.into(),
                initial_balance: initial_balance.to_string(),
            })
            .await
            .map_err(|e| OpenApiError::GrpcStatus(e.to_string()))?;
        Ok(())
    }

    pub async fn login(
        &mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<String, OpenApiError> {
        let response = self
            .inner
            .login(pb::LoginRequest {
                username: username.into(),
                password: password.into(),
            })
            .await
            .map_err(|e| OpenApiError::GrpcStatus(e.to_string()))?
            .into_inner();

        Ok(response.session_token)
    }

    pub async fn logout(&mut self, session_token: impl Into<String>) -> Result<(), OpenApiError> {
        self.inner
            .logout(pb::LogoutRequest {
                session_token: session_token.into(),
            })
            .await
            .map_err(|e| OpenApiError::GrpcStatus(e.to_string()))?;
        Ok(())
    }

    pub async fn fetch_balance(
        &mut self,
        session_token: impl Into<String>,
    ) -> Result<AccountBalance, OpenApiError> {
        let response = self
            .inner
            .fetch_balance(pb::FetchBalanceRequest {
                session_token: session_token.into(),
            })
            .await
            .map_err(|e| OpenApiError::GrpcStatus(e.to_string()))?
            .into_inner();

        let balance = response.balance.ok_or_else(|| OpenApiError::GrpcStatus("missing balance payload".to_string()))?;
        Ok(AccountBalance {
            account_id: balance.account_id,
            balance: parse_decimal_field("balance", &balance.balance)?,
            used_margin: parse_decimal_field("used_margin", &balance.used_margin)?,
            free_margin: parse_decimal_field("free_margin", &balance.free_margin)?,
        })
    }

    pub async fn quote_volume(
        &mut self,
        symbol: impl Into<String>,
        lot_size: Decimal,
    ) -> Result<VolumeQuote, OpenApiError> {
        let response = self
            .inner
            .quote_volume(pb::QuoteVolumeRequest {
                symbol: symbol.into(),
                lot_size: lot_size.to_string(),
            })
            .await
            .map_err(|e| OpenApiError::GrpcStatus(e.to_string()))?
            .into_inner();

        let quote = response.quote.ok_or_else(|| OpenApiError::GrpcStatus("missing quote payload".to_string()))?;
        Ok(VolumeQuote {
            symbol: quote.symbol,
            lot_size: parse_decimal_field("lot_size", &quote.lot_size)?,
            volume: parse_decimal_field("volume", &quote.volume)?,
        })
    }

    pub async fn place_market_order(
        &mut self,
        session_token: impl Into<String>,
        symbol: impl Into<String>,
        lot_size: Decimal,
        side: OrderSide,
    ) -> Result<OrderExecution, OpenApiError> {
        let response = self
            .inner
            .place_market_order(pb::PlaceMarketOrderRequest {
                session_token: session_token.into(),
                symbol: symbol.into(),
                lot_size: lot_size.to_string(),
                side: domain_side_to_proto(side).into(),
            })
            .await
            .map_err(|e| OpenApiError::GrpcStatus(e.to_string()))?
            .into_inner();

        let execution = response
            .execution
            .ok_or_else(|| OpenApiError::GrpcStatus("missing execution payload".to_string()))?;

        Ok(OrderExecution {
            order_id: execution.order_id,
            account_id: execution.account_id,
            symbol: execution.symbol,
            lot_size: parse_decimal_field("lot_size", &execution.lot_size)?,
            volume: parse_decimal_field("volume", &execution.volume)?,
            side: proto_side_to_domain(execution.side)?,
            margin_used: parse_decimal_field("margin_used", &execution.margin_used)?,
        })
    }
}

fn open_api_error_to_status(error: OpenApiError) -> Status {
    match error {
        OpenApiError::SymbolNotFound(_) => Status::not_found(error.to_string()),
        OpenApiError::SessionNotFound => Status::unauthenticated(error.to_string()),
        OpenApiError::InvalidCredentials => Status::unauthenticated(error.to_string()),
        OpenApiError::UserAlreadyExists(_) | OpenApiError::AccountAlreadyExists(_) => {
            Status::already_exists(error.to_string())
        }
        OpenApiError::InsufficientMargin { .. } => Status::failed_precondition(error.to_string()),
        OpenApiError::GrpcStatus(_) => Status::internal(error.to_string()),
        _ => Status::invalid_argument(error.to_string()),
    }
}

fn normalize_symbol(symbol: impl Into<String>) -> String {
    symbol.into().trim().to_ascii_uppercase()
}

fn normalize_username(username: impl Into<String>) -> String {
    username.into().trim().to_ascii_lowercase()
}

fn parse_decimal_field(field: &'static str, value: &str) -> Result<Decimal, OpenApiError> {
    value
        .parse::<Decimal>()
        .map_err(|_| OpenApiError::InvalidDecimalField {
            field,
            value: value.to_string(),
        })
}

fn proto_side_to_domain(side: i32) -> Result<OrderSide, OpenApiError> {
    match pb::OrderSide::try_from(side) {
        Ok(pb::OrderSide::Buy) => Ok(OrderSide::Buy),
        Ok(pb::OrderSide::Sell) => Ok(OrderSide::Sell),
        _ => Err(OpenApiError::UnsupportedOrderSide(side)),
    }
}

fn domain_side_to_proto(side: OrderSide) -> pb::OrderSide {
    match side {
        OrderSide::Buy => pb::OrderSide::Buy,
        OrderSide::Sell => pb::OrderSide::Sell,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
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

    fn sample_service() -> OpenApiService {
        let mut service = OpenApiService::new(sample_engine());
        service
            .register_user("alice", "secret", "ACC-001", d(100_000, 0))
            .expect("user must be registered");
        service
    }

    #[test]
    fn computes_volume_from_lot_size_for_known_symbol() {
        let engine = sample_engine();

        let quote = engine
            .volume_from_lot_size("eurusd", d(50, 2))
            .expect("expected valid quote");

        assert_eq!(quote.symbol, "EURUSD");
        assert_eq!(quote.volume, d(50_000, 0));
    }

    #[test]
    fn rejects_unknown_symbol() {
        let engine = sample_engine();

        let err = engine
            .volume_from_lot_size("UNKNOWN", d(10, 2))
            .expect_err("expected symbol error");

        assert_eq!(err, OpenApiError::SymbolNotFound("UNKNOWN".to_string()));
    }

    #[test]
    fn rejects_lot_size_out_of_range() {
        let engine = sample_engine();

        let err = engine
            .volume_from_lot_size("XAUUSD", d(5, 2))
            .expect_err("expected range error");

        assert_eq!(
            err,
            OpenApiError::LotOutOfRange {
                lot_size: d(5, 2),
                min_lot: d(1, 1),
                max_lot: d(500, 0),
            }
        );
    }

    #[test]
    fn rejects_lot_size_not_matching_step() {
        let engine = sample_engine();

        let err = engine
            .volume_from_lot_size("XAUUSD", d(15, 2))
            .expect_err("expected step error");

        assert_eq!(
            err,
            OpenApiError::InvalidLotStep {
                lot_size: d(15, 2),
                lot_step: d(1, 1),
                min_lot: d(1, 1),
            }
        );
    }

    #[test]
    fn handles_high_volume_without_precision_loss() {
        let engine = sample_engine();

        let quote = engine
            .volume_from_lot_size("EURUSD", d(1000, 0))
            .expect("expected valid quote");

        assert_eq!(quote.volume, d(100_000_000, 0));
    }

    #[test]
    fn login_and_fetch_balance_work() {
        let mut service = sample_service();

        let token = service.login("Alice", "secret").expect("login should succeed");
        let balance = service
            .fetch_balance(&token)
            .expect("balance should be available");

        assert_eq!(balance.account_id, "ACC-001");
        assert_eq!(balance.balance, d(100_000, 0));
        assert_eq!(balance.used_margin, Decimal::ZERO);
        assert_eq!(balance.free_margin, d(100_000, 0));
    }

    #[test]
    fn place_market_order_updates_margin() {
        let mut service = sample_service();
        let token = service.login("alice", "secret").expect("login should succeed");

        let execution = service
            .place_market_order(
                &token,
                OrderRequest {
                    symbol: "EURUSD".to_string(),
                    lot_size: d(10, 2),
                    side: OrderSide::Buy,
                },
            )
            .expect("order should execute");

        assert_eq!(execution.account_id, "ACC-001");
        assert_eq!(execution.volume, d(10_000, 0));
        assert_eq!(execution.margin_used, d(100, 0));

        let balance = service
            .fetch_balance(&token)
            .expect("balance should be available");
        assert_eq!(balance.used_margin, d(100, 0));
        assert_eq!(balance.free_margin, d(99_900, 0));
    }

    #[test]
    fn fails_order_when_symbol_not_found() {
        let mut service = sample_service();
        let token = service.login("alice", "secret").expect("login should succeed");

        let err = service
            .place_market_order(
                &token,
                OrderRequest {
                    symbol: "BTCFOO".to_string(),
                    lot_size: d(1, 0),
                    side: OrderSide::Sell,
                },
            )
            .expect_err("order should fail for unknown symbol");

        assert_eq!(err, OpenApiError::SymbolNotFound("BTCFOO".to_string()));
    }

    #[test]
    fn rejects_invalid_login() {
        let mut service = sample_service();

        let err = service
            .login("alice", "wrong-pass")
            .expect_err("login should fail");

        assert_eq!(err, OpenApiError::InvalidCredentials);
    }

    #[test]
    fn logout_invalidates_session() {
        let mut service = sample_service();
        let token = service.login("alice", "secret").expect("login should succeed");

        service.logout(&token).expect("logout should succeed");

        let err = service
            .fetch_balance(&token)
            .expect_err("session must be invalid after logout");
        assert_eq!(err, OpenApiError::SessionNotFound);
    }

    #[test]
    fn protobuf_login_balance_and_order_flow() {
        let mut service = sample_service();

        let login = service
            .login_proto(pb::LoginRequest {
                username: "alice".to_string(),
                password: "secret".to_string(),
            })
            .expect("login proto should succeed");

        let balance_before = service
            .fetch_balance_proto(pb::FetchBalanceRequest {
                session_token: login.session_token.clone(),
            })
            .expect("balance proto should succeed");
        assert_eq!(
            balance_before
                .balance
                .expect("balance payload should exist")
                .free_margin,
            "100000"
        );

        let order = service
            .place_market_order_proto(pb::PlaceMarketOrderRequest {
                session_token: login.session_token.clone(),
                symbol: "EURUSD".to_string(),
                lot_size: "0.20".to_string(),
                side: pb::OrderSide::Buy as i32,
            })
            .expect("order proto should succeed");

        let execution = order.execution.expect("execution payload should exist");
        assert_eq!(execution.volume, "20000.00");
        assert_eq!(execution.margin_used, "200.00");

        let balance_after = service
            .fetch_balance_proto(pb::FetchBalanceRequest {
                session_token: login.session_token,
            })
            .expect("balance proto should succeed");
        assert_eq!(
            balance_after
                .balance
                .expect("balance payload should exist")
                .free_margin,
            "99800.00"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grpc_sdk_client_roundtrip_works() {
        let mut service = sample_service();
        service
            .register_user("bob", "pw", "ACC-002", d(250_000, 0))
            .expect("bob should be registered");

        let server = OpenApiGrpcServer::new(service);

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should resolve");
        let incoming = TcpListenerStream::new(listener);

        let server_task = tokio::spawn(async move {
            Server::builder()
                .add_service(server.into_tonic_service())
                .serve_with_incoming(incoming)
                .await
        });

        let mut client = OpenApiSdkClient::connect(format!("http://{addr}"))
            .await
            .expect("client should connect");

        let token = client
            .login("bob", "pw")
            .await
            .expect("login should succeed");

        let quote = client
            .quote_volume("EURUSD", d(25, 2))
            .await
            .expect("quote should succeed");
        assert_eq!(quote.volume, d(25_000, 0));

        let execution = client
            .place_market_order(token.clone(), "EURUSD", d(25, 2), OrderSide::Buy)
            .await
            .expect("order should execute");
        assert_eq!(execution.margin_used, d(250, 0));

        let balance = client
            .fetch_balance(token.clone())
            .await
            .expect("balance should load");
        assert_eq!(balance.free_margin, d(249_750, 0));

        client.logout(token).await.expect("logout should succeed");

        server_task.abort();
    }
}
