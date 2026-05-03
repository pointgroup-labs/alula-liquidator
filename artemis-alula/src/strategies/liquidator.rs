use {
    crate::{
        collectors::block_collector::NewBlock,
        constants::{
            EVT_ADD_COLLATERAL, EVT_BORROW, EVT_DEPOSIT, EVT_LIQUIDATE, EVT_REMOVE_COLLATERAL,
            EVT_REPAY, EVT_WITHDRAW, REFRESH_INTERVAL_BLOCKS,
        },
        db::DbManager,
        executors::tx_executor::SubmitStellarTx,
        helper,
        types::{
            Action, BoxFuture, Event, MarketData, Obligation, ObligationKey, PoolData, Strategy,
        },
    },
    ed25519_dalek::SigningKey,
    std::{collections::HashMap, sync::Arc},
    stellar_rpc_client::{Client, Event as SorobanEvent},
    stellar_xdr::curr::{AccountId, PublicKey, ScAddress, Uint256},
    tracing::{debug, error, info, warn},
    url::Url,
};

pub struct Config {
    pub rpc_url: Url,
    pub db_path: String,
    pub markets: Vec<String>,
    pub xlm_address: String,
    /// Minimum profit margin in cents (e.g. 50 = $0.50).
    pub min_profit_margin_cents: i128,
    pub assets_to_hold: Vec<String>,
    pub swap_providers: Vec<String>,
    /// XLM balance to reserve for fees and trustlines (7 decimals).
    pub xlm_safety_margin: i128,
    // Slippage protection in basis points (e.g. 100 = 1%).
    // pub slippage_bps: Option<i128>, | Not sure we need this here. If slippage is high but liquidation
    // is profitable - we must go for it
}

/// Liquidation approach determined by available liquidity.
#[derive(Debug, Clone)]
enum LiquidationType {
    /// Liquidator has sufficient balance to cover the full repayment.
    Own { full_repay_amount: i128 },
    /// Liquidator needs a flash loan to bridge the liquidity gap. This requires an immediate swap
    /// after receiving the collateral back to fund the flash loan.
    FlashLoan {
        full_repay_amount: i128,
        flash_borrow_amount: i128,
    },
    /// Liquidator relies on swapped liquidity(required), own liquidity(optional) and a flash borrowed amount(optional)
    PreSwap {
        full_repay_amount: i128,
        pre_liquidation_swap_amount: i128,
        flash_amount: i128,                  // additional flash loan after pre-swap
        pre_liquidation_swa_asset: String,   // address of the liquid asset we're swapping from
    },
}


/// Complete liquidation plan with batch composition details.
#[derive(Debug)]
struct LiquidationPlan {
    liquidation_type: LiquidationType,
}

pub struct Liquidator {
    rpc: Client,
    pkey: String,
    config: Config,
    skey: SigningKey,
    db: Arc<DbManager>, // TODO: Try 'Rc' here
    last_refresh_ledger: u32,
    market_data: HashMap<String, MarketData>,
    obligations: HashMap<String, HashMap<ObligationKey, Obligation>>,
}


impl Liquidator {
    pub fn try_create(config: Config, skey: &SigningKey, db: &Arc<DbManager>) -> anyhow::Result<Self> {
        let db = Arc::clone(db);
        let rpc = Client::new(config.rpc_url.as_str())?;
        let pkey = ScAddress::Account(AccountId(PublicKey::PublicKeyTypeEd25519(Uint256(
            skey.verifying_key().to_bytes(),
        ))))
        .to_string();

        Ok(Self {
            db,
            rpc,
            pkey,
            config,
            skey: skey.clone(),
            last_refresh_ledger: 0,
            obligations: HashMap::new(),
            market_data: HashMap::new(),
        })
    }
}


impl Strategy<Event, Action> for Liquidator {
    fn process_event(&mut self, event: Event) -> BoxFuture<'_, Vec<Action>> {
        // Box::pin(async {
        //     match event {
        //         Event::SorobanEvents(e) => self.handle_soroban_event(e).await,
        //         Event::NewBlock(b) => self.handle_new_block(b).await,
        //     }
        // })

        todo!()
    }

    fn sync_state(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async {
            info!(?self.config.markets, "sync_state: loading market(s)");

            for market in &self.config.markets {
                match helper::simulate_get_market_data(&self.rpc, market, &self.pkey).await {
                    Ok(market_data) => {
                        info!(market, ?market_data);
                        self.market_data.insert(market.clone(), market_data);
                    }
                    Err(e) => error!(?e, ?market, "get_market_data failed"),
                }

                let cached_obligations = self.db.load_obligations(market).unwrap_or_else(|e| {
                    warn!(
                        ?e,
                        ?market,
                        "failed to load obligations from DB. Falling back to RPC"
                    );
                    HashMap::new()
                });

                if cached_obligations.is_empty() {
                    info!(?market, "no cached obligations, fetching from RPC");
                    let obl_map = todo!();
                    self.obligations.insert(market.clone(), obl_map);
                } else {
                    info!(?cached_obligations, "loaded obligations from DB");
                    self.obligations.insert(market.clone(), cached_obligations);
                }
            }
            info!("sync_state: done");

            Ok(())
        })
    }
}