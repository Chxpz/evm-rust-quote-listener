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
async fn main() -> Result<(), Box<dyn Error>> {
    // Load environment variables from .env
    dotenv().ok();

    println!("Initializing listener");

    // Load the JSON file
    let json_data = fs::read_to_string("target_pools.json")?;
    let config: NetworksConfig = serde_json::from_str(&json_data)?;

    // For each network in the JSON
    for network in config.networks {
        // Fetch the WebSocket URL from the environment
        let websocket = env::var(&network.websocket_env).expect(&format!(
            "Environment variable {} not found",
            network.websocket_env
        ));
        let pools = network.pools.clone();

        // Start an asynchronous task to process this network
        task::spawn(async move {
            listen_to_network(&network.network, &websocket, &pools).await;
        });
    }

    // Main loop to keep the program running
    loop {
        println!("Running...");
        sleep(Duration::from_secs(60)).await;
    }
}

async fn listen_to_network(network_name: &str, websocket: &str, pools: &[Pool]) {
    println!("Connecting to websocket: {}", websocket);

    let mut prices: HashMap<String, Vec<(String, f64)>> = HashMap::new();

    for pool in pools {
        match fetch_token_price(&pool.pool_address, &pool.abi, websocket).await {
            Ok(price) => {
                let price_f64 = price.as_u64() as f64 / 1e18; // Convert U256 to float

                // Log the price
                println!(
                    "Network: {}, Asset: {}, Market: {}, Price: {:.6}",
                    network_name, pool.asset, pool.market, price_f64
                );

                // Store the price for comparison
                prices
                    .entry(pool.asset.clone())
                    .or_insert_with(Vec::new)
                    .push((pool.market.clone(), price_f64));
            }
            Err(e) => {
                eprintln!(
                    "Error fetching price for pool {}: {}",
                    pool.pool_address, e
                );
            }
        }
    }

    // Check for arbitrage opportunities
    for (asset, markets) in prices {
        for i in 0..markets.len() {
            for j in (i + 1)..markets.len() {
                let (market1, price1) = &markets[i];
                let (market2, price2) = &markets[j];

                let diff = (price1 - price2).abs() / price1.max(*price2);
                if diff > 0.01 {
                    println!(
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
    rpc_url: &str,
) -> Result<U256, Box<dyn Error>> {
    // Connect to the WebSocket node
    let provider = Provider::<Ws>::connect(rpc_url).await?;
    let provider = Arc::new(provider);

    // Parse the contract address
    let address: Address = pool_address.parse()?;

    // Parse the contract ABI
    let abi: Abi = serde_json::from_str(abi)?;

    // Create an instance of the contract
    let contract = Contract::new(address, abi, provider);

    // Call the contract function to get the price (example: "getQuote")
    let price: U256 = contract
        .method::<(), U256>("getQuote", ())? // Function name and arguments
        .call()
        .await?;

    Ok(price)
}
