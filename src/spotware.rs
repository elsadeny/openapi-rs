use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use ctrader_rs::proto::common::{
    ProtoOaExecutionEvent, ProtoOaGetTrendbarsReq, ProtoOaGetTrendbarsRes, ProtoOaNewOrderReq,
    ProtoOaOrderType, ProtoOaTradeSide, ProtoOaTrendbarPeriod,
};
use ctrader_rs::{Client as CtraderClient, Config as CtraderConfig};
use futures_core::Stream;
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use tokio::sync::Mutex as TokioMutex;
use tonic::{Request, Response, Status};

use crate::pb;
use crate::{
    AccountBalance, OpenApiError, OpenPosition, OrderSide, Timeframe, VolumeQuote,
};

const OA_NEW_ORDER_REQ: u32 = 2106;
const OA_EXECUTION_EVENT: u32 = 2126;
const OA_GET_TRENDBARS_REQ: u32 = 2137;
const OA_GET_TRENDBARS_RES: u32 = 2138;

#[derive(Debug, Clone)]
pub struct SpotwareAdapterConfig {
    pub client_id: String,
    pub client_secret: String,
    pub access_token: String,
    pub trader_account_id: i64,
    pub live: bool,
    pub openapi_username: String,
    pub openapi_password: String,
}

#[derive(Debug, Clone)]
struct SpotwareSymbolMeta {
    symbol_id: i64,
    digits: i32,
    lot_size_cents: i64,
    min_volume_cents: i64,
    max_volume_cents: i64,
    step_volume_cents: i64,
}

#[derive(Debug, Clone)]
struct SpotwareSession {
    username: String,
}

pub struct SpotwareGrpcAdapterServer {
    client: Arc<CtraderClient>,
    trader_account_id: i64,
    openapi_username: String,
    openapi_password: String,
    sessions: Arc<TokioMutex<HashMap<String, SpotwareSession>>>,
    next_session_id: AtomicU64,
    symbols: Arc<TokioMutex<HashMap<String, SpotwareSymbolMeta>>>,
}

impl SpotwareGrpcAdapterServer {
    pub async fn connect(config: SpotwareAdapterConfig) -> Result<Self, OpenApiError> {
        let mut client_cfg = CtraderConfig::new(config.client_id, config.client_secret);
        if config.live {
            client_cfg = client_cfg.live();
        }

        let client = CtraderClient::start(client_cfg)
            .await
            .map_err(|e| OpenApiError::GrpcStatus(format!("spotware client start failed: {e}")))?;
        client
            .account_auth(config.trader_account_id, &config.access_token)
            .await
            .map_err(|e| OpenApiError::GrpcStatus(format!("spotware account auth failed: {e}")))?;

        let symbols = load_symbol_map(&client, config.trader_account_id).await?;

        Ok(Self {
            client: Arc::new(client),
            trader_account_id: config.trader_account_id,
            openapi_username: config.openapi_username,
            openapi_password: config.openapi_password,
            sessions: Arc::new(TokioMutex::new(HashMap::new())),
            next_session_id: AtomicU64::new(1),
            symbols: Arc::new(TokioMutex::new(symbols)),
        })
    }

    pub fn into_tonic_service(self) -> pb::open_api_service_server::OpenApiServiceServer<Self> {
        pb::open_api_service_server::OpenApiServiceServer::new(self)
    }

    async fn require_session(&self, token: &str) -> Result<(), OpenApiError> {
        let sessions = self.sessions.lock().await;
        if sessions.contains_key(token) {
            Ok(())
        } else {
            Err(OpenApiError::SessionNotFound)
        }
    }

    async fn resolve_symbol(&self, symbol: &str) -> Result<SpotwareSymbolMeta, OpenApiError> {
        let symbols = self.symbols.lock().await;
        let key = normalize_symbol(symbol);
        symbols
            .get(&key)
            .cloned()
            .ok_or_else(|| OpenApiError::SymbolNotFound(symbol.to_string()))
    }

    fn lot_constraints(meta: &SpotwareSymbolMeta) -> Result<(Decimal, Decimal, Decimal), OpenApiError> {
        let lot_size = Decimal::from_i64(meta.lot_size_cents)
            .ok_or_else(|| OpenApiError::GrpcStatus("invalid lot_size_cents".to_string()))?;
        if lot_size <= Decimal::ZERO {
            return Err(OpenApiError::GrpcStatus("lot_size_cents must be > 0".to_string()));
        }

        let min = Decimal::from_i64(meta.min_volume_cents)
            .ok_or_else(|| OpenApiError::GrpcStatus("invalid min_volume_cents".to_string()))?
            / lot_size;
        let max = Decimal::from_i64(meta.max_volume_cents)
            .ok_or_else(|| OpenApiError::GrpcStatus("invalid max_volume_cents".to_string()))?
            / lot_size;
        let step = Decimal::from_i64(meta.step_volume_cents)
            .ok_or_else(|| OpenApiError::GrpcStatus("invalid step_volume_cents".to_string()))?
            / lot_size;

        Ok((min, max, step))
    }

    fn quote_for_lot(symbol: String, lot_size: Decimal, meta: &SpotwareSymbolMeta) -> Result<VolumeQuote, OpenApiError> {
        if lot_size <= Decimal::ZERO {
            return Err(OpenApiError::NonPositiveLotSize);
        }

        let (min_lot, max_lot, lot_step) = Self::lot_constraints(meta)?;
        if lot_size < min_lot || lot_size > max_lot {
            return Err(OpenApiError::LotOutOfRange {
                lot_size,
                min_lot,
                max_lot,
            });
        }

        let remainder = (lot_size - min_lot) % lot_step;
        if !remainder.is_zero() {
            return Err(OpenApiError::InvalidLotStep {
                lot_size,
                lot_step,
                min_lot,
            });
        }

        let contract_size = Decimal::from_i64(meta.lot_size_cents)
            .ok_or_else(|| OpenApiError::GrpcStatus("invalid lot_size_cents".to_string()))?
            / Decimal::new(100, 0);
        Ok(VolumeQuote {
            symbol,
            lot_size,
            volume: lot_size * contract_size,
        })
    }

    fn lot_to_volume_cents(lot_size: Decimal, meta: &SpotwareSymbolMeta) -> Result<i64, OpenApiError> {
        let lot_size_cents = Decimal::from_i64(meta.lot_size_cents)
            .ok_or_else(|| OpenApiError::GrpcStatus("invalid lot_size_cents".to_string()))?;
        let volume = lot_size * lot_size_cents;
        volume
            .to_i64()
            .ok_or_else(|| OpenApiError::GrpcStatus("computed volume out of range".to_string()))
    }

    async fn fetch_balance_domain(&self) -> Result<AccountBalance, OpenApiError> {
        let trader_res = self
            .client
            .get_trader(self.trader_account_id)
            .await
            .map_err(|e| OpenApiError::GrpcStatus(format!("spotware get_trader failed: {e}")))?;

        let trader = trader_res.trader;

        let money_digits = trader.money_digits.unwrap_or(2) as u32;
        let balance = Decimal::new(trader.balance, money_digits);

        let reconcile = self
            .client
            .reconcile(self.trader_account_id, false)
            .await
            .map_err(|e| OpenApiError::GrpcStatus(format!("spotware reconcile failed: {e}")))?;

        let mut used_margin = Decimal::ZERO;
        for position in reconcile.position {
            if let Some(um) = position.used_margin {
                let md = position.money_digits.unwrap_or(money_digits) as u32;
                used_margin += Decimal::new(um as i64, md);
            }
        }

        Ok(AccountBalance {
            account_id: self.trader_account_id.to_string(),
            balance,
            used_margin,
            free_margin: balance - used_margin,
        })
    }

    async fn fetch_open_positions_domain(&self) -> Result<Vec<OpenPosition>, OpenApiError> {
        let reconcile = self
            .client
            .reconcile(self.trader_account_id, false)
            .await
            .map_err(|e| OpenApiError::GrpcStatus(format!("spotware reconcile failed: {e}")))?;

        let symbols = self.symbols.lock().await;

        let mut reverse = HashMap::new();
        for (name, meta) in symbols.iter() {
            reverse.insert(meta.symbol_id, name.clone());
        }

        let mut out = Vec::new();
        for pos in reconcile.position {
            let td = pos.trade_data;

            let symbol = reverse
                .get(&td.symbol_id)
                .cloned()
                .unwrap_or_else(|| td.symbol_id.to_string());

            let side = match ProtoOaTradeSide::try_from(td.trade_side) {
                Ok(ProtoOaTradeSide::Buy) => OrderSide::Buy,
                Ok(ProtoOaTradeSide::Sell) => OrderSide::Sell,
                _ => return Err(OpenApiError::UnsupportedOrderSide(td.trade_side)),
            };

            let volume_units = Decimal::new(td.volume, 2);
            let lot_size = if let Some(meta) = symbols.get(&symbol) {
                let denom = Decimal::from_i64(meta.lot_size_cents)
                    .ok_or_else(|| OpenApiError::GrpcStatus("invalid lot_size_cents".to_string()))?;
                Decimal::new(td.volume, 0) / denom
            } else {
                Decimal::ZERO
            };

            out.push(OpenPosition {
                position_id: pos.position_id as u64,
                account_id: self.trader_account_id.to_string(),
                symbol,
                lot_size,
                volume: volume_units,
                side,
            });
        }

        Ok(out)
    }

    async fn get_candles_domain(
        &self,
        symbol: String,
        timeframe: Timeframe,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<crate::Candle>, OpenApiError> {
        let meta = self.resolve_symbol(&symbol).await?;

        let req = ProtoOaGetTrendbarsReq {
            payload_type: Some(OA_GET_TRENDBARS_REQ as i32),
            ctid_trader_account_id: self.trader_account_id,
            from_timestamp: Some(from.timestamp_millis()),
            to_timestamp: Some(to.timestamp_millis()),
            period: spotware_period_from_timeframe(timeframe) as i32,
            symbol_id: meta.symbol_id,
            count: Some(limit),
        };

        let res: ProtoOaGetTrendbarsRes = self
            .client
            .command(OA_GET_TRENDBARS_REQ, req, OA_GET_TRENDBARS_RES)
            .await
            .map_err(|e| OpenApiError::GrpcStatus(format!("spotware get trendbars failed: {e}")))?;

        let mut candles = Vec::new();
        let step = timeframe_seconds(timeframe);
        let scale = meta.digits as u32;
        for bar in res.trendbar {
            let low_raw = bar.low.unwrap_or_default();
            let open_raw = low_raw + bar.delta_open.unwrap_or_default() as i64;
            let close_raw = low_raw + bar.delta_close.unwrap_or_default() as i64;
            let high_raw = low_raw + bar.delta_high.unwrap_or_default() as i64;

            let open_time = DateTime::<Utc>::from_timestamp((bar.utc_timestamp_in_minutes.unwrap_or_default() as i64) * 60, 0)
                .ok_or_else(|| OpenApiError::GrpcStatus("invalid trendbar timestamp".to_string()))?;
            let close_time = open_time + Duration::seconds(step);

            candles.push(crate::Candle {
                symbol: symbol.clone(),
                timeframe,
                open_time,
                close_time,
                open: Decimal::new(open_raw, scale),
                high: Decimal::new(high_raw, scale),
                low: Decimal::new(low_raw, scale),
                close: Decimal::new(close_raw, scale),
                volume: Decimal::new(bar.volume, 0),
            });
        }

        Ok(candles)
    }
}

#[tonic::async_trait]
impl pb::open_api_service_server::OpenApiService for SpotwareGrpcAdapterServer {
    type StreamCandlesStream =
        Pin<Box<dyn Stream<Item = Result<pb::StreamCandlesResponse, Status>> + Send + 'static>>;

    async fn register_user(
        &self,
        _request: Request<pb::RegisterUserRequest>,
    ) -> Result<Response<pb::RegisterUserResponse>, Status> {
        Ok(Response::new(pb::RegisterUserResponse { success: true }))
    }

    async fn login(
        &self,
        request: Request<pb::LoginRequest>,
    ) -> Result<Response<pb::LoginResponse>, Status> {
        let req = request.into_inner();
        if normalize_username(&req.username) != normalize_username(&self.openapi_username)
            || req.password != self.openapi_password
        {
            return Err(Status::unauthenticated("invalid credentials"));
        }

        let id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        let token = format!("spotware-sess-{id}");
        self.sessions.lock().await.insert(
            token.clone(),
            SpotwareSession {
                username: req.username,
            },
        );

        Ok(Response::new(pb::LoginResponse {
            session_token: token,
        }))
    }

    async fn logout(
        &self,
        request: Request<pb::LogoutRequest>,
    ) -> Result<Response<pb::LogoutResponse>, Status> {
        let token = request.into_inner().session_token;
        let removed = self.sessions.lock().await.remove(&token).is_some();
        if !removed {
            return Err(Status::unauthenticated("session token not found"));
        }

        Ok(Response::new(pb::LogoutResponse { success: true }))
    }

    async fn fetch_balance(
        &self,
        request: Request<pb::FetchBalanceRequest>,
    ) -> Result<Response<pb::FetchBalanceResponse>, Status> {
        let token = request.into_inner().session_token;
        self.require_session(&token)
            .await
            .map_err(open_api_error_to_status)?;

        let balance = self
            .fetch_balance_domain()
            .await
            .map_err(open_api_error_to_status)?;

        Ok(Response::new(pb::FetchBalanceResponse {
            balance: Some(pb::AccountBalance {
                account_id: balance.account_id,
                balance: balance.balance.to_string(),
                used_margin: balance.used_margin.to_string(),
                free_margin: balance.free_margin.to_string(),
            }),
        }))
    }

    async fn fetch_open_positions(
        &self,
        request: Request<pb::FetchOpenPositionsRequest>,
    ) -> Result<Response<pb::FetchOpenPositionsResponse>, Status> {
        let token = request.into_inner().session_token;
        self.require_session(&token)
            .await
            .map_err(open_api_error_to_status)?;

        let positions = self
            .fetch_open_positions_domain()
            .await
            .map_err(open_api_error_to_status)?;

        Ok(Response::new(pb::FetchOpenPositionsResponse {
            positions: positions
                .into_iter()
                .map(|p| pb::OpenPosition {
                    position_id: p.position_id,
                    account_id: p.account_id,
                    symbol: p.symbol,
                    lot_size: p.lot_size.to_string(),
                    volume: p.volume.to_string(),
                    side: match p.side {
                        OrderSide::Buy => pb::OrderSide::Buy as i32,
                        OrderSide::Sell => pb::OrderSide::Sell as i32,
                    },
                })
                .collect(),
        }))
    }

    async fn place_market_order(
        &self,
        request: Request<pb::PlaceMarketOrderRequest>,
    ) -> Result<Response<pb::PlaceMarketOrderResponse>, Status> {
        let req = request.into_inner();
        self.require_session(&req.session_token)
            .await
            .map_err(open_api_error_to_status)?;

        let lot_size = req
            .lot_size
            .parse::<Decimal>()
            .map_err(|_| Status::invalid_argument("invalid lot_size"))?;
        let symbol = normalize_symbol(req.symbol);
        let meta = self
            .resolve_symbol(&symbol)
            .await
            .map_err(open_api_error_to_status)?;

        let quote = Self::quote_for_lot(symbol.clone(), lot_size, &meta)
            .map_err(open_api_error_to_status)?;
        let volume_cents = Self::lot_to_volume_cents(lot_size, &meta)
            .map_err(open_api_error_to_status)?;

        let side = match pb::OrderSide::try_from(req.side) {
            Ok(pb::OrderSide::Buy) => ProtoOaTradeSide::Buy,
            Ok(pb::OrderSide::Sell) => ProtoOaTradeSide::Sell,
            _ => return Err(Status::invalid_argument("unsupported order side")),
        };

        let order_req = ProtoOaNewOrderReq {
            payload_type: Some(OA_NEW_ORDER_REQ as i32),
            ctid_trader_account_id: self.trader_account_id,
            symbol_id: meta.symbol_id,
            trade_side: side as i32,
            volume: volume_cents,
            order_type: ProtoOaOrderType::Market as i32,
            ..Default::default()
        };

        let event: ProtoOaExecutionEvent = self
            .client
            .command(OA_NEW_ORDER_REQ, order_req, OA_EXECUTION_EVENT)
            .await
            .map_err(|e| Status::internal(format!("spotware new order failed: {e}")))?;

        let order_id = event
            .order
            .as_ref()
            .map(|o| o.order_id as u64)
            .unwrap_or_default();

        let margin_used = event
            .position
            .as_ref()
            .and_then(|p| p.used_margin)
            .map(|um| {
                let md = event
                    .position
                    .as_ref()
                    .and_then(|p| p.money_digits)
                    .unwrap_or(2) as u32;
                Decimal::new(um as i64, md)
            })
            .unwrap_or(Decimal::ZERO);

        Ok(Response::new(pb::PlaceMarketOrderResponse {
            execution: Some(pb::OrderExecution {
                order_id,
                account_id: self.trader_account_id.to_string(),
                symbol,
                lot_size: quote.lot_size.to_string(),
                volume: quote.volume.to_string(),
                side: req.side,
                margin_used: margin_used.to_string(),
            }),
        }))
    }

    async fn quote_volume(
        &self,
        request: Request<pb::QuoteVolumeRequest>,
    ) -> Result<Response<pb::QuoteVolumeResponse>, Status> {
        let req = request.into_inner();
        let lot_size = req
            .lot_size
            .parse::<Decimal>()
            .map_err(|_| Status::invalid_argument("invalid lot_size"))?;
        let symbol = normalize_symbol(req.symbol);
        let meta = self
            .resolve_symbol(&symbol)
            .await
            .map_err(open_api_error_to_status)?;

        let quote = Self::quote_for_lot(symbol, lot_size, &meta).map_err(open_api_error_to_status)?;

        Ok(Response::new(pb::QuoteVolumeResponse {
            quote: Some(pb::VolumeQuote {
                symbol: quote.symbol,
                lot_size: quote.lot_size.to_string(),
                volume: quote.volume.to_string(),
            }),
        }))
    }

    async fn get_candles(
        &self,
        request: Request<pb::GetCandlesRequest>,
    ) -> Result<Response<pb::GetCandlesResponse>, Status> {
        let req = request.into_inner();

        let timeframe = pb_timeframe_to_domain(req.timeframe).map_err(open_api_error_to_status)?;
        let limit = if req.limit == 0 { 100 } else { req.limit };
        if limit > 2000 {
            return Err(Status::invalid_argument("limit too large"));
        }

        let (from, to) = resolve_time_range(&req.from_time, &req.to_time, timeframe, limit)
            .map_err(open_api_error_to_status)?;
        let candles = self
            .get_candles_domain(req.symbol, timeframe, from, to, limit)
            .await
            .map_err(open_api_error_to_status)?;

        let out = candles
            .into_iter()
            .map(|c| pb::Candle {
                symbol: c.symbol,
                timeframe: domain_timeframe_to_pb(c.timeframe) as i32,
                open_time: format_utc(c.open_time),
                close_time: format_utc(c.close_time),
                open: c.open.to_string(),
                high: c.high.to_string(),
                low: c.low.to_string(),
                close: c.close.to_string(),
                volume: c.volume.to_string(),
            })
            .collect();

        Ok(Response::new(pb::GetCandlesResponse { candles: out }))
    }

    async fn stream_candles(
        &self,
        request: Request<pb::StreamCandlesRequest>,
    ) -> Result<Response<Self::StreamCandlesStream>, Status> {
        let req = request.into_inner();
        let timeframe = pb_timeframe_to_domain(req.timeframe).map_err(open_api_error_to_status)?;
        let limit = if req.limit == 0 { 100 } else { req.limit };
        if limit > 2000 {
            return Err(Status::invalid_argument("limit too large"));
        }

        let (from, to) = resolve_time_range(&req.from_time, &req.to_time, timeframe, limit)
            .map_err(open_api_error_to_status)?;
        let candles = self
            .get_candles_domain(req.symbol, timeframe, from, to, limit)
            .await
            .map_err(open_api_error_to_status)?;

        let stream = tokio_stream::iter(candles.into_iter().map(|c| {
            Ok(pb::StreamCandlesResponse {
                candle: Some(pb::Candle {
                    symbol: c.symbol,
                    timeframe: domain_timeframe_to_pb(c.timeframe) as i32,
                    open_time: format_utc(c.open_time),
                    close_time: format_utc(c.close_time),
                    open: c.open.to_string(),
                    high: c.high.to_string(),
                    low: c.low.to_string(),
                    close: c.close.to_string(),
                    volume: c.volume.to_string(),
                }),
            })
        }));

        Ok(Response::new(Box::pin(stream) as Self::StreamCandlesStream))
    }

    async fn get_server_time(
        &self,
        _request: Request<pb::GetServerTimeRequest>,
    ) -> Result<Response<pb::GetServerTimeResponse>, Status> {
        Ok(Response::new(pb::GetServerTimeResponse {
            server_time: format_utc(Utc::now()),
        }))
    }
}

async fn load_symbol_map(
    client: &CtraderClient,
    account_id: i64,
) -> Result<HashMap<String, SpotwareSymbolMeta>, OpenApiError> {
    let light = client
        .symbols_list(account_id, false)
        .await
        .map_err(|e| OpenApiError::GrpcStatus(format!("spotware symbols_list failed: {e}")))?;

    let mut name_to_id = HashMap::new();
    let mut ids = Vec::new();
    for s in light.symbol {
        if let Some(name) = s.symbol_name {
            let key = normalize_symbol(name);
            name_to_id.insert(key, s.symbol_id);
            ids.push(s.symbol_id);
        }
    }

    let details_req = ctrader_rs::proto::common::ProtoOaSymbolByIdReq {
        payload_type: Some(2116),
        ctid_trader_account_id: account_id,
        symbol_id: ids,
    };

    let details: ctrader_rs::proto::common::ProtoOaSymbolByIdRes = client
        .command(2116, details_req.clone(), 2117)
        .await
        .map_err(|e| OpenApiError::GrpcStatus(format!("spotware symbol_by_id failed: {e}")))?;

    let mut by_id = HashMap::new();
    for s in details.symbol {
        by_id.insert(s.symbol_id, s);
    }

    let mut out = HashMap::new();
    for (name, symbol_id) in name_to_id {
        if let Some(s) = by_id.get(&symbol_id) {
            out.insert(
                name,
                SpotwareSymbolMeta {
                    symbol_id,
                    digits: s.digits,
                    lot_size_cents: s.lot_size.unwrap_or(10_000_000),
                    min_volume_cents: s.min_volume.unwrap_or(100_000),
                    max_volume_cents: s.max_volume.unwrap_or(10_000_000_000),
                    step_volume_cents: s.step_volume.unwrap_or(100_000),
                },
            );
        }
    }

    Ok(out)
}

fn resolve_time_range(
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
            let from = to - Duration::seconds(timeframe_seconds(timeframe) * limit as i64);
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
            })
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

fn normalize_symbol(symbol: impl Into<String>) -> String {
    symbol.into().trim().to_ascii_uppercase()
}

fn normalize_username(username: impl Into<String>) -> String {
    username.into().trim().to_ascii_lowercase()
}

fn pb_timeframe_to_domain(value: i32) -> Result<Timeframe, OpenApiError> {
    match pb::Timeframe::try_from(value) {
        Ok(pb::Timeframe::M1) => Ok(Timeframe::M1),
        Ok(pb::Timeframe::M5) => Ok(Timeframe::M5),
        Ok(pb::Timeframe::M15) => Ok(Timeframe::M15),
        Ok(pb::Timeframe::H1) => Ok(Timeframe::H1),
        Ok(pb::Timeframe::H4) => Ok(Timeframe::H4),
        Ok(pb::Timeframe::D1) => Ok(Timeframe::D1),
        _ => Err(OpenApiError::UnsupportedTimeframe(value)),
    }
}

fn domain_timeframe_to_pb(value: Timeframe) -> pb::Timeframe {
    match value {
        Timeframe::M1 => pb::Timeframe::M1,
        Timeframe::M5 => pb::Timeframe::M5,
        Timeframe::M15 => pb::Timeframe::M15,
        Timeframe::H1 => pb::Timeframe::H1,
        Timeframe::H4 => pb::Timeframe::H4,
        Timeframe::D1 => pb::Timeframe::D1,
    }
}

fn timeframe_seconds(value: Timeframe) -> i64 {
    match value {
        Timeframe::M1 => 60,
        Timeframe::M5 => 300,
        Timeframe::M15 => 900,
        Timeframe::H1 => 3600,
        Timeframe::H4 => 14_400,
        Timeframe::D1 => 86_400,
    }
}

fn spotware_period_from_timeframe(value: Timeframe) -> ProtoOaTrendbarPeriod {
    match value {
        Timeframe::M1 => ProtoOaTrendbarPeriod::M1,
        Timeframe::M5 => ProtoOaTrendbarPeriod::M5,
        Timeframe::M15 => ProtoOaTrendbarPeriod::M15,
        Timeframe::H1 => ProtoOaTrendbarPeriod::H1,
        Timeframe::H4 => ProtoOaTrendbarPeriod::H4,
        Timeframe::D1 => ProtoOaTrendbarPeriod::D1,
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
