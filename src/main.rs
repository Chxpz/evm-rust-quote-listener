use dotenv::dotenv;
use std::env;
use ethers::providers::{Provider, Ws};
use ethers::contract::Contract;
use ethers::core::abi::Abi;
use ethers::types::{Address, U256};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::error::Error;
use std::fs;
use tokio::task;
use tokio::time::{sleep, Duration};
use std::collections::HashMap;
use tracing::{info, error};
use tracing_subscriber;
use futures::future;

const MIN_ARBITRAGE_DIFFERENCE: f64 = 0.01;

#[derive(Serialize, Deserialize, Clone)]
struct Pool {
    asset: String,
    market: String,
    pool_address: String,
    abi: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct Network {
    network: String,
    websocket_env: String,
    pools: Vec<Pool>,
}

#[derive(Serialize, Deserialize)]
struct NetworksConfig {
    networks: Vec<Network>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Load environment variables from .env
    dotenv().ok();

    info!("Initializing listener");

    // Load the JSON file
    let json_data = fs::read_to_string("target_pools.json")?;
    let config: NetworksConfig = serde_json::from_str(&json_data)?;

    // Validate configuration
    validate_config(&config)?;

    // For each network in the JSON
    for network in config.networks {
        let websocket_env = network.websocket_env.clone();
        let pools = network.pools.clone();

        // Fetch the WebSocket URL from the environment
        let websocket = env::var(&websocket_env).expect(&format!(
            "Environment variable {} not found",
            websocket_env
        ));

        // Start an asynchronous task to process this network
        task::spawn(async move {
            if let Err(e) = listen_to_network(&network.network, &websocket, pools).await {
                error!("Error in network {}: {}", network.network, e);
            }
        });
    }

    // Main loop to keep the program running
    loop {
        info!("Running...");
        sleep(Duration::from_secs(60)).await;
    }
}

fn validate_config(config: &NetworksConfig) -> Result<(), Box<dyn Error + Send + Sync>> {
    if config.networks.is_empty() {
        return Err("No networks found in JSON configuration".into());
    }

    for network in &config.networks {
        if network.websocket_env.is_empty() {
            return Err(format!("WebSocket env missing for network {}", network.network).into());
        }
    }

    Ok(())
}

async fn listen_to_network(network_name: &str, websocket: &str, pools: Vec<Pool>) -> Result<(), Box<dyn Error + Send + Sync>> {
    info!("Connecting to websocket: {}", websocket);

    let provider = Arc::new(connect_with_retry(websocket, 3).await?);
    let mut prices: HashMap<String, Vec<(String, f64)>> = HashMap::new();

    let mut handles = Vec::new();

    for pool in pools {
        let provider = provider.clone();
        let network_name = network_name.to_string();
        let pool = pool.clone();

        // Spawn a task for each pool
        let handle = tokio::spawn(async move {
            match fetch_token_price(&pool.pool_address, &pool.abi, &provider).await {
                Ok(price) => {
                    let price_f64 = price.as_u64() as f64 / 1e18;
                    info!(
                        "Network: {}, Asset: {}, Market: {}, Price: {:.6}",
                        network_name, pool.asset, pool.market, price_f64
                    );
                    Ok((pool.asset, pool.market, price_f64))
                }
                Err(e) => {
                    error!(
                        "Error fetching price for pool {}: {}",
                        pool.pool_address, e
                    );
                    Err(e)
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all tasks to finish
    let results = futures::future::join_all(handles).await;

    // Process results
    for result in results {
        if let Ok(Ok((asset, market, price))) = result {
            prices
                .entry(asset)
                .or_insert_with(Vec::new)
                .push((market, price));
        }
    }

    // Check for arbitrage opportunities
    check_arbitrage_opportunities(network_name, prices);

    Ok(())
}

fn check_arbitrage_opportunities(network_name: &str, prices: HashMap<String, Vec<(String, f64)>>) {
    for (asset, markets) in prices {
        for i in 0..markets.len() {
            for j in (i + 1)..markets.len() {
                let (market1, price1) = &markets[i];
                let (market2, price2) = &markets[j];

                let diff = (price1 - price2).abs() / price1.max(*price2);
                if diff > MIN_ARBITRAGE_DIFFERENCE {
                    info!(
                        "TRADE OPPORTUNITY: Network: {}, Asset: {}, Market1: {}, Price1: {:.6}, Market2: {}, Price2: {:.6}, Difference: {:.2}%",
                        network_name, asset, market1, price1, market2, price2, diff * 100.0
                    );
                }
            }
        }
    }
}

pub async fn fetch_token_price(
    pool_address: &str,
    abi: &str,
    provider: &Arc<Provider<Ws>>,
) -> Result<U256, Box<dyn Error + Send + Sync>> {
    let address: Address = pool_address.parse()?;
    let abi: Abi = serde_json::from_str(abi)?;
    let contract = Contract::new(address, abi.clone(), provider.clone());

    let price: U256 = contract
        .method::<(), U256>("getQuote", ())?
        .call()
        .await?;

    Ok(price)
}

pub async fn connect_with_retry(url: &str, retries: usize) -> Result<Provider<Ws>, Box<dyn Error + Send + Sync>> {
    let mut attempts = 0;
    let mut delay = 1;

    while attempts < retries {
        match Provider::<Ws>::connect(url).await {
            Ok(provider) => return Ok(provider),
            Err(e) => {
                error!("Failed to connect: {}. Retrying in {} seconds...", e, delay);
                tokio::time::sleep(Duration::from_secs(delay)).await;
                delay *= 2; // Exponential backoff
                attempts += 1;
            }
        }
    }

    Err(format!("Failed to connect after {} attempts", retries).into())
}
