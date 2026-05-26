//! openapi-rs: protobuf/gRPC trading SDK primitives.
//!
//! This crate provides:
//! - symbol-aware lot-size validation and volume quoting
//! - auth/session/account service logic
//! - candle history + candle streaming APIs
//! - tonic server adapter (`OpenApiGrpcServer`)
//! - typed async SDK client (`OpenApiSdkClient`)

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use futures_core::Stream;
use rust_decimal::Decimal;
use thiserror::Error;
use tokio::sync::Mutex as TokioMutex;
#[cfg(feature = "live")]
use tokio_stream::StreamExt as _;
use tonic::{Request, Response, Status};

#[cfg(feature = "demo")]
mod spotware;
#[cfg(feature = "demo")]
pub use spotware::{SpotwareAdapterConfig, SpotwareGrpcAdapterServer};

pub mod pb {
    tonic::include_proto!("openapi");
}

pub const DEFAULT_CANDLE_LIMIT: u32 = 100;
pub const MAX_CANDLE_LIMIT: u32 = 2_000;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timeframe {
    M1,
    M5,
    M15,
    H1,
    H4,
    D1,
}

impl Timeframe {
    fn seconds(self) -> i64 {
        match self {
            Self::M1 => 60,
            Self::M5 => 300,
            Self::M15 => 900,
            Self::H1 => 3600,
            Self::H4 => 14_400,
            Self::D1 => 86_400,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candle {
    pub symbol: String,
    pub timeframe: Timeframe,
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
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
pub struct OpenPosition {
    pub position_id: u64,
    pub account_id: String,
    pub symbol: String,
    pub lot_size: Decimal,
    pub volume: Decimal,
    pub side: OrderSide,
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

#[derive(Debug, Clone)]
struct CandleQuery {
    symbol: String,
    timeframe: Timeframe,
    from_time: DateTime<Utc>,
    to_time: DateTime<Utc>,
    limit: u32,
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
    #[error("unsupported timeframe: {0}")]
    UnsupportedTimeframe(i32),
    #[error("limit {limit} exceeds maximum allowed {max}")]
    CandleLimitTooLarge { limit: u32, max: u32 },
    #[error("invalid UTC timestamp for field '{field}': '{value}'")]
    InvalidUtcTimestamp { field: &'static str, value: String },
    #[error("invalid time range: from_time {from_time} must be before to_time {to_time}")]
    InvalidTimeRange { from_time: String, to_time: String },
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

    pub fn has_symbol(&self, symbol: impl AsRef<str>) -> bool {
        self.symbols.contains_key(&normalize_symbol(symbol.as_ref()))
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
    open_positions: HashMap<String, Vec<OpenPosition>>,
    next_session_id: AtomicU64,
    next_order_id: AtomicU64,
    next_position_id: AtomicU64,
    leverage: Decimal,
}

impl OpenApiService {
    pub fn new(engine: OpenApiEngine) -> Self {
        Self {
            engine,
            users: HashMap::new(),
            sessions: HashMap::new(),
            accounts: HashMap::new(),
            open_positions: HashMap::new(),
            next_session_id: AtomicU64::new(1),
            next_order_id: AtomicU64::new(1),
            next_position_id: AtomicU64::new(1),
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
        self.open_positions.insert(account_id.clone(), Vec::new());

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

    pub fn fetch_open_positions(
        &self,
        token: impl AsRef<str>,
    ) -> Result<Vec<OpenPosition>, OpenApiError> {
        let account_id = self.account_id_for_token(token.as_ref())?;
        let positions = self
            .open_positions
            .get(&account_id)
            .ok_or_else(|| OpenApiError::AccountNotFound(account_id.clone()))?;

        Ok(positions.clone())
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
        let position_id = self.next_position_id.fetch_add(1, Ordering::Relaxed);

        self.open_positions
            .entry(account_id.clone())
            .or_default()
            .push(OpenPosition {
                position_id,
                account_id: account_id.clone(),
                symbol: volume_quote.symbol.clone(),
                lot_size: volume_quote.lot_size,
                volume: volume_quote.volume,
                side: request.side.clone(),
            });

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

    pub fn fetch_open_positions_proto(
        &self,
        request: pb::FetchOpenPositionsRequest,
    ) -> Result<pb::FetchOpenPositionsResponse, OpenApiError> {
        let positions = self.fetch_open_positions(request.session_token)?;

        Ok(pb::FetchOpenPositionsResponse {
            positions: positions
                .into_iter()
                .map(|position| pb::OpenPosition {
                    position_id: position.position_id,
                    account_id: position.account_id,
                    symbol: position.symbol,
                    lot_size: position.lot_size.to_string(),
                    volume: position.volume.to_string(),
                    side: domain_side_to_proto(position.side) as i32,
                })
                .collect(),
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

    pub fn get_candles_proto(
        &self,
        request: pb::GetCandlesRequest,
    ) -> Result<pb::GetCandlesResponse, OpenApiError> {
        let candles = self.build_candles(CandleQuery {
            symbol: request.symbol,
            timeframe: proto_timeframe_to_domain(request.timeframe)?,
            from_time: Utc::now(),
            to_time: Utc::now(),
            limit: request.limit,
        }, request.from_time, request.to_time)?;

        Ok(pb::GetCandlesResponse { candles })
    }

    pub fn stream_candles_proto(
        &self,
        request: pb::StreamCandlesRequest,
    ) -> Result<Vec<pb::Candle>, OpenApiError> {
        self.build_candles(CandleQuery {
            symbol: request.symbol,
            timeframe: proto_timeframe_to_domain(request.timeframe)?,
            from_time: Utc::now(),
            to_time: Utc::now(),
            limit: request.limit,
        }, request.from_time, request.to_time)
    }

    pub fn get_server_time_proto(
        &self,
        _request: pb::GetServerTimeRequest,
    ) -> Result<pb::GetServerTimeResponse, OpenApiError> {
        Ok(pb::GetServerTimeResponse {
            server_time: format_utc(Utc::now()),
        })
    }

    fn build_candles(
        &self,
        mut query: CandleQuery,
        from_time_str: String,
        to_time_str: String,
    ) -> Result<Vec<pb::Candle>, OpenApiError> {
        query.symbol = normalize_symbol(query.symbol);
        if !self.engine.has_symbol(&query.symbol) {
            return Err(OpenApiError::SymbolNotFound(query.symbol));
        }

        let limit = if query.limit == 0 {
            DEFAULT_CANDLE_LIMIT
        } else {
            query.limit
        };
        if limit > MAX_CANDLE_LIMIT {
            return Err(OpenApiError::CandleLimitTooLarge {
                limit,
                max: MAX_CANDLE_LIMIT,
            });
        }

        let (from_time, to_time) = resolve_time_window(
            &from_time_str,
            &to_time_str,
            query.timeframe,
            limit,
        )?;

        query.from_time = from_time;
        query.to_time = to_time;
        query.limit = limit;

        let mut candles = Vec::with_capacity(limit as usize);
        let step_seconds = query.timeframe.seconds();
        let seed = query
            .symbol
            .bytes()
            .fold(0_u64, |acc, b| acc.wrapping_add(b as u64)) as i64;

        for i in 0..limit {
            let open_time = query.from_time + Duration::seconds(step_seconds * i as i64);
            let close_time = open_time + Duration::seconds(step_seconds);
            if close_time > query.to_time {
                break;
            }

            let open = Decimal::new(10_000 + (seed % 2_000) + i as i64 * 2, 2);
            let high = open + Decimal::new(15, 2);
            let low = open - Decimal::new(12, 2);
            let close = open + Decimal::new(3, 2);
            let volume = Decimal::new(1_000 + i as i64 * 10, 2);

            candles.push(pb::Candle {
                symbol: query.symbol.clone(),
                timeframe: domain_timeframe_to_proto(query.timeframe) as i32,
                open_time: format_utc(open_time),
                close_time: format_utc(close_time),
                open: open.to_string(),
                high: high.to_string(),
                low: low.to_string(),
                close: close.to_string(),
                volume: volume.to_string(),
            });
        }

        Ok(candles)
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

#[cfg(feature = "live")]
#[derive(Clone, Debug)]
pub struct BrokerGrpcAdapterServer {
    upstream: Arc<
        TokioMutex<pb::open_api_service_client::OpenApiServiceClient<tonic::transport::Channel>>,
    >,
}

#[cfg(feature = "live")]
impl BrokerGrpcAdapterServer {
    pub async fn connect(upstream_url: impl Into<String>) -> Result<Self, tonic::transport::Error> {
        let client = pb::open_api_service_client::OpenApiServiceClient::connect(upstream_url.into())
            .await?;
        Ok(Self {
            upstream: Arc::new(TokioMutex::new(client)),
        })
    }

    pub fn into_tonic_service(self) -> pb::open_api_service_server::OpenApiServiceServer<Self> {
        pb::open_api_service_server::OpenApiServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl pb::open_api_service_server::OpenApiService for OpenApiGrpcServer {
    type StreamCandlesStream = Pin<Box<dyn Stream<Item = Result<pb::StreamCandlesResponse, Status>> + Send + 'static>>;

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

    async fn fetch_open_positions(
        &self,
        request: Request<pb::FetchOpenPositionsRequest>,
    ) -> Result<Response<pb::FetchOpenPositionsResponse>, Status> {
        let service = self.inner.lock().await;
        let response = service
            .fetch_open_positions_proto(request.into_inner())
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

    async fn get_candles(
        &self,
        request: Request<pb::GetCandlesRequest>,
    ) -> Result<Response<pb::GetCandlesResponse>, Status> {
        let service = self.inner.lock().await;
        let response = service
            .get_candles_proto(request.into_inner())
            .map_err(open_api_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn stream_candles(
        &self,
        request: Request<pb::StreamCandlesRequest>,
    ) -> Result<Response<Self::StreamCandlesStream>, Status> {
        let service = self.inner.lock().await;
        let candles = service
            .stream_candles_proto(request.into_inner())
            .map_err(open_api_error_to_status)?;

        let stream = tokio_stream::iter(candles.into_iter().map(|candle| {
            Ok(pb::StreamCandlesResponse {
                candle: Some(candle),
            })
        }));

        Ok(Response::new(Box::pin(stream) as Self::StreamCandlesStream))
    }

    async fn get_server_time(
        &self,
        request: Request<pb::GetServerTimeRequest>,
    ) -> Result<Response<pb::GetServerTimeResponse>, Status> {
        let service = self.inner.lock().await;
        let response = service
            .get_server_time_proto(request.into_inner())
            .map_err(open_api_error_to_status)?;
        Ok(Response::new(response))
    }
}

#[cfg(feature = "live")]
#[tonic::async_trait]
impl pb::open_api_service_server::OpenApiService for BrokerGrpcAdapterServer {
    type StreamCandlesStream =
        Pin<Box<dyn Stream<Item = Result<pb::StreamCandlesResponse, Status>> + Send + 'static>>;

    async fn register_user(
        &self,
        request: Request<pb::RegisterUserRequest>,
    ) -> Result<Response<pb::RegisterUserResponse>, Status> {
        let mut client = self.upstream.lock().await;
        client.register_user(request.into_inner()).await
    }

    async fn login(
        &self,
        request: Request<pb::LoginRequest>,
    ) -> Result<Response<pb::LoginResponse>, Status> {
        let mut client = self.upstream.lock().await;
        client.login(request.into_inner()).await
    }

    async fn logout(
        &self,
        request: Request<pb::LogoutRequest>,
    ) -> Result<Response<pb::LogoutResponse>, Status> {
        let mut client = self.upstream.lock().await;
        client.logout(request.into_inner()).await
    }

    async fn fetch_balance(
        &self,
        request: Request<pb::FetchBalanceRequest>,
    ) -> Result<Response<pb::FetchBalanceResponse>, Status> {
        let mut client = self.upstream.lock().await;
        client.fetch_balance(request.into_inner()).await
    }

    async fn fetch_open_positions(
        &self,
        request: Request<pb::FetchOpenPositionsRequest>,
    ) -> Result<Response<pb::FetchOpenPositionsResponse>, Status> {
        let mut client = self.upstream.lock().await;
        client.fetch_open_positions(request.into_inner()).await
    }

    async fn place_market_order(
        &self,
        request: Request<pb::PlaceMarketOrderRequest>,
    ) -> Result<Response<pb::PlaceMarketOrderResponse>, Status> {
        let mut client = self.upstream.lock().await;
        client.place_market_order(request.into_inner()).await
    }

    async fn quote_volume(
        &self,
        request: Request<pb::QuoteVolumeRequest>,
    ) -> Result<Response<pb::QuoteVolumeResponse>, Status> {
        let mut client = self.upstream.lock().await;
        client.quote_volume(request.into_inner()).await
    }

    async fn get_candles(
        &self,
        request: Request<pb::GetCandlesRequest>,
    ) -> Result<Response<pb::GetCandlesResponse>, Status> {
        let mut client = self.upstream.lock().await;
        client.get_candles(request.into_inner()).await
    }

    async fn stream_candles(
        &self,
        request: Request<pb::StreamCandlesRequest>,
    ) -> Result<Response<Self::StreamCandlesStream>, Status> {
        let mut client = self.upstream.lock().await;
        let upstream_stream = client.stream_candles(request.into_inner()).await?.into_inner();
        let mapped = upstream_stream.map(|item| item.map_err(|status| status));
        Ok(Response::new(Box::pin(mapped) as Self::StreamCandlesStream))
    }

    async fn get_server_time(
        &self,
        request: Request<pb::GetServerTimeRequest>,
    ) -> Result<Response<pb::GetServerTimeResponse>, Status> {
        let mut client = self.upstream.lock().await;
        client.get_server_time(request.into_inner()).await
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

        let balance = response
            .balance
            .ok_or_else(|| OpenApiError::GrpcStatus("missing balance payload".to_string()))?;

        Ok(AccountBalance {
            account_id: balance.account_id,
            balance: parse_decimal_field("balance", &balance.balance)?,
            used_margin: parse_decimal_field("used_margin", &balance.used_margin)?,
            free_margin: parse_decimal_field("free_margin", &balance.free_margin)?,
        })
    }

    pub async fn fetch_open_positions(
        &mut self,
        session_token: impl Into<String>,
    ) -> Result<Vec<OpenPosition>, OpenApiError> {
        let response = self
            .inner
            .fetch_open_positions(pb::FetchOpenPositionsRequest {
                session_token: session_token.into(),
            })
            .await
            .map_err(|e| OpenApiError::GrpcStatus(e.to_string()))?
            .into_inner();

        response
            .positions
            .into_iter()
            .map(|position| {
                Ok(OpenPosition {
                    position_id: position.position_id,
                    account_id: position.account_id,
                    symbol: position.symbol,
                    lot_size: parse_decimal_field("lot_size", &position.lot_size)?,
                    volume: parse_decimal_field("volume", &position.volume)?,
                    side: proto_side_to_domain(position.side)?,
                })
            })
            .collect()
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

        let quote = response
            .quote
            .ok_or_else(|| OpenApiError::GrpcStatus("missing quote payload".to_string()))?;

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

    pub async fn get_candles(
        &mut self,
        symbol: impl Into<String>,
        timeframe: Timeframe,
        from_time: Option<DateTime<Utc>>,
        to_time: Option<DateTime<Utc>>,
        limit: u32,
    ) -> Result<Vec<Candle>, OpenApiError> {
        let response = self
            .inner
            .get_candles(pb::GetCandlesRequest {
                symbol: symbol.into(),
                timeframe: domain_timeframe_to_proto(timeframe) as i32,
                from_time: from_time.map(format_utc).unwrap_or_default(),
                to_time: to_time.map(format_utc).unwrap_or_default(),
                limit,
            })
            .await
            .map_err(|e| OpenApiError::GrpcStatus(e.to_string()))?
            .into_inner();

        response
            .candles
            .into_iter()
            .map(candle_from_proto)
            .collect()
    }

    pub async fn stream_candles(
        &mut self,
        symbol: impl Into<String>,
        timeframe: Timeframe,
        from_time: Option<DateTime<Utc>>,
        to_time: Option<DateTime<Utc>>,
        limit: u32,
    ) -> Result<tonic::Streaming<pb::StreamCandlesResponse>, OpenApiError> {
        let stream = self
            .inner
            .stream_candles(pb::StreamCandlesRequest {
                symbol: symbol.into(),
                timeframe: domain_timeframe_to_proto(timeframe) as i32,
                from_time: from_time.map(format_utc).unwrap_or_default(),
                to_time: to_time.map(format_utc).unwrap_or_default(),
                limit,
            })
            .await
            .map_err(|e| OpenApiError::GrpcStatus(e.to_string()))?
            .into_inner();

        Ok(stream)
    }

    pub async fn get_server_time(&mut self) -> Result<DateTime<Utc>, OpenApiError> {
        let response = self
            .inner
            .get_server_time(pb::GetServerTimeRequest {})
            .await
            .map_err(|e| OpenApiError::GrpcStatus(e.to_string()))?
            .into_inner();

        parse_utc_timestamp_field("server_time", &response.server_time)
    }
}

fn resolve_time_window(
    from_time: &str,
    to_time: &str,
    timeframe: Timeframe,
    limit: u32,
) -> Result<(DateTime<Utc>, DateTime<Utc>), OpenApiError> {
    let from_empty = from_time.trim().is_empty();
    let to_empty = to_time.trim().is_empty();

    let (from, to) = match (from_empty, to_empty) {
        (true, true) => {
            let to = Utc::now();
            let from = to - Duration::seconds(timeframe.seconds() * limit as i64);
            (from, to)
        }
        (false, false) => (
            parse_utc_timestamp_field("from_time", from_time)?,
            parse_utc_timestamp_field("to_time", to_time)?,
        ),
        _ => {
            return Err(OpenApiError::InvalidUtcTimestamp {
                field: if from_empty { "from_time" } else { "to_time" },
                value: if from_empty {
                    from_time.to_string()
                } else {
                    to_time.to_string()
                },
            });
        }
    };

    if from >= to {
        return Err(OpenApiError::InvalidTimeRange {
            from_time: format_utc(from),
            to_time: format_utc(to),
        });
    }

    Ok((from, to))
}

fn parse_utc_timestamp_field(field: &'static str, value: &str) -> Result<DateTime<Utc>, OpenApiError> {
    let parsed = DateTime::parse_from_rfc3339(value).map_err(|_| OpenApiError::InvalidUtcTimestamp {
        field,
        value: value.to_string(),
    })?;

    if parsed.offset().local_minus_utc() != 0 {
        return Err(OpenApiError::InvalidUtcTimestamp {
            field,
            value: value.to_string(),
        });
    }

    Ok(parsed.with_timezone(&Utc))
}

fn format_utc(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn candle_from_proto(candle: pb::Candle) -> Result<Candle, OpenApiError> {
    Ok(Candle {
        symbol: candle.symbol,
        timeframe: proto_timeframe_to_domain(candle.timeframe)?,
        open_time: parse_utc_timestamp_field("open_time", &candle.open_time)?,
        close_time: parse_utc_timestamp_field("close_time", &candle.close_time)?,
        open: parse_decimal_field("open", &candle.open)?,
        high: parse_decimal_field("high", &candle.high)?,
        low: parse_decimal_field("low", &candle.low)?,
        close: parse_decimal_field("close", &candle.close)?,
        volume: parse_decimal_field("volume", &candle.volume)?,
    })
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

fn proto_timeframe_to_domain(timeframe: i32) -> Result<Timeframe, OpenApiError> {
    match pb::Timeframe::try_from(timeframe) {
        Ok(pb::Timeframe::M1) => Ok(Timeframe::M1),
        Ok(pb::Timeframe::M5) => Ok(Timeframe::M5),
        Ok(pb::Timeframe::M15) => Ok(Timeframe::M15),
        Ok(pb::Timeframe::H1) => Ok(Timeframe::H1),
        Ok(pb::Timeframe::H4) => Ok(Timeframe::H4),
        Ok(pb::Timeframe::D1) => Ok(Timeframe::D1),
        _ => Err(OpenApiError::UnsupportedTimeframe(timeframe)),
    }
}

fn domain_timeframe_to_proto(timeframe: Timeframe) -> pb::Timeframe {
    match timeframe {
        Timeframe::M1 => pb::Timeframe::M1,
        Timeframe::M5 => pb::Timeframe::M5,
        Timeframe::M15 => pb::Timeframe::M15,
        Timeframe::H1 => pb::Timeframe::H1,
        Timeframe::H4 => pb::Timeframe::H4,
        Timeframe::D1 => pb::Timeframe::D1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn candle_limit_guard_rejects_large_limit() {
        let service = sample_service();

        let err = service
            .get_candles_proto(pb::GetCandlesRequest {
                symbol: "EURUSD".to_string(),
                timeframe: pb::Timeframe::M1 as i32,
                from_time: String::new(),
                to_time: String::new(),
                limit: MAX_CANDLE_LIMIT + 1,
            })
            .expect_err("limit should be rejected");

        assert_eq!(
            err,
            OpenApiError::CandleLimitTooLarge {
                limit: MAX_CANDLE_LIMIT + 1,
                max: MAX_CANDLE_LIMIT,
            }
        );
    }

    #[test]
    fn timeframe_validation_rejects_unspecified() {
        let service = sample_service();

        let err = service
            .get_candles_proto(pb::GetCandlesRequest {
                symbol: "EURUSD".to_string(),
                timeframe: pb::Timeframe::Unspecified as i32,
                from_time: String::new(),
                to_time: String::new(),
                limit: 10,
            })
            .expect_err("timeframe should be rejected");

        assert_eq!(err, OpenApiError::UnsupportedTimeframe(0));
    }

    #[test]
    fn timestamp_validation_requires_utc() {
        let service = sample_service();

        let err = service
            .get_candles_proto(pb::GetCandlesRequest {
                symbol: "EURUSD".to_string(),
                timeframe: pb::Timeframe::M1 as i32,
                from_time: "2026-05-26T12:00:00+03:00".to_string(),
                to_time: "2026-05-26T13:00:00Z".to_string(),
                limit: 10,
            })
            .expect_err("non-utc timestamp should fail");

        assert_eq!(
            err,
            OpenApiError::InvalidUtcTimestamp {
                field: "from_time",
                value: "2026-05-26T12:00:00+03:00".to_string(),
            }
        );
    }

    #[test]
    fn range_validation_requires_from_before_to() {
        let service = sample_service();

        let err = service
            .get_candles_proto(pb::GetCandlesRequest {
                symbol: "EURUSD".to_string(),
                timeframe: pb::Timeframe::M1 as i32,
                from_time: "2026-05-26T13:00:00Z".to_string(),
                to_time: "2026-05-26T12:00:00Z".to_string(),
                limit: 10,
            })
            .expect_err("invalid range should fail");

        assert_eq!(
            err,
            OpenApiError::InvalidTimeRange {
                from_time: "2026-05-26T13:00:00Z".to_string(),
                to_time: "2026-05-26T12:00:00Z".to_string(),
            }
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

        let positions = client
            .fetch_open_positions(token.clone())
            .await
            .expect("positions should load");
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].symbol, "EURUSD");

        let candles = client
            .get_candles("EURUSD", Timeframe::M1, None, None, 5)
            .await
            .expect("candles should load");
        assert_eq!(candles.len(), 5);

        let mut stream = client
            .stream_candles("EURUSD", Timeframe::M1, None, None, 3)
            .await
            .expect("stream should open");
        let mut received = 0;
        while let Some(item) = stream.next().await {
            let msg = item.expect("stream item should be valid");
            assert!(msg.candle.is_some());
            received += 1;
        }
        assert_eq!(received, 3);

        let _server_time = client
            .get_server_time()
            .await
            .expect("server time should load");

        let balance = client
            .fetch_balance(token.clone())
            .await
            .expect("balance should load");
        assert!(balance.free_margin < d(250_000, 0));

        client.logout(token).await.expect("logout should succeed");

        server_task.abort();
    }
}
