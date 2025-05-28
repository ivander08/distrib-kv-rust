use clap::{Parser, Subcommand};
use std::time::Duration; // For timeouts, similar to your test client
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// You'll need to make your ClientRequest and ClientReply types accessible.
// If core_types.rs is part of your library (e.g., declared in src/lib.rs), you'd use:
// use distrib_kv_rust::core_types::{ClientRequest, ClientReply};
// For now, let's assume you'll make them available. We might need to adjust this.
// Placeholder for now - replace with your actual path to these types:
mod core_types { // Temporary placeholder if types are not in lib
    // You would typically get these from your library: use your_crate_name::core_types::*;
    // For this example to compile standalone temporarily, we might need to define them briefly
    // or ensure your project structure allows `use distrib_kv_rust::core_types::*;`
    // This will be an important step to resolve.
    // For now, let's assume the real types will be imported.
    #[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
    pub enum ClientRequest {
        Get { key: String },
        Set { key: String, value: String },
        Delete { key: String },
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
    pub enum ClientReply {
        Value {
            key: String,
            value: Option<String>,
        },
        Success {
            command_applied_at_log_index: u64,
        },
        Error {
            msg: String,
        },
        LeaderRedirect {
            leader_id: u64,
            leader_addr: Option<String>,
        },
    }
}
use crate::core_types::{ClientRequest, ClientReply}; // Adjust if using a library path

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Address of the Raft server to connect to (e.g., 127.0.0.1:9081)
    #[clap(short, long, global = true, default_value = "127.0.0.1:9081")]
    address: String,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Set a key-value pair
    Set {
        #[clap(value_parser)]
        key: String,
        #[clap(value_parser)]
        value: String,
    },
    /// Get the value for a key
    Get {
        #[clap(value_parser)]
        key: String,
    },
    /// Delete a key
    Delete {
        #[clap(value_parser)]
        key: String,
    },
}

// We'll reuse and adapt the logic from your `send_test_client_command` here
// For now, let's define its signature. It will handle connection, sending,
// receiving, and leader redirection.
async fn send_command_to_server(
    initial_target_addr: String,
    request: ClientRequest,
) -> Result<ClientReply, String> {
    let mut current_target_addr = initial_target_addr;
    let max_redirects = 3; // Max 3 redirects

    for attempt in 0..=max_redirects {
        if attempt > 0 {
            println!("[CLI] Redirecting (attempt {}), trying {}...", attempt + 1, current_target_addr);
            tokio::time::sleep(Duration::from_millis(50)).await; // Small delay before retry
        }

        println!("[CLI] Connecting to {}...", current_target_addr);
        match TcpStream::connect(&current_target_addr).await {
            Ok(mut stream) => {
                println!("[CLI] Connected. Sending request: {:?}", request);
                let serialized_req = match bincode::serialize(&request) {
                    Ok(sr) => sr,
                    Err(e) => return Err(format!("Failed to serialize request: {}", e)),
                };
                let len_bytes_req = (serialized_req.len() as u32).to_be_bytes();

                if stream.write_all(&len_bytes_req).await.is_err() ||
                   stream.write_all(&serialized_req).await.is_err() {
                    eprintln!("[CLI] Error: Failed to send request to {}.", current_target_addr);
                    if attempt == max_redirects {
                        return Err(format!("Failed to send request after {} attempts.", max_redirects + 1));
                    }
                    // If send fails, we might want to retry *if* we have a new address from a redirect.
                    // For now, if send fails on an address, we probably shouldn't retry that same address immediately.
                    // The loop continues if a redirect provides a new address.
                    continue; // Try next redirect if any
                }

                let mut len_bytes_reply = [0u8; 4];
                match stream.read_exact(&mut len_bytes_reply).await {
                    Ok(_) => {
                        let reply_len = u32::from_be_bytes(len_bytes_reply) as usize;
                        if reply_len == 0 || reply_len > 1_048_576 { // Max 1MB reply
                            return Err(format!("Invalid reply length {} received.", reply_len));
                        }
                        let mut reply_buffer = vec![0u8; reply_len];
                        if stream.read_exact(&mut reply_buffer).await.is_err() {
                            return Err("Failed to read reply body.".to_string());
                        }

                        match bincode::deserialize::<ClientReply>(&reply_buffer) {
                            Ok(reply) => {
                                match reply {
                                    ClientReply::LeaderRedirect { leader_id: _, leader_addr: Some(new_addr) } => {
                                        if attempt < max_redirects {
                                            println!("[CLI] Received redirect to: {}", new_addr);
                                            current_target_addr = new_addr;
                                            let _ = stream.shutdown().await;
                                            continue; // Retry with new address
                                        } else {
                                            return Err(format!("Max redirects ({}) reached. Last known redirect: {}.", max_redirects, new_addr));
                                        }
                                    }
                                    ClientReply::LeaderRedirect { leader_id, leader_addr: None } => {
                                        return Err(format!("Redirected, but no new leader address provided (Known Leader ID: {}).", leader_id));
                                    }
                                    _ => return Ok(reply), // Success or other terminal error
                                }
                            }
                            Err(e) => return Err(format!("Failed to deserialize reply: {}", e)),
                        }
                    }
                    Err(e) => return Err(format!("Failed to read reply length: {}", e)),
                }
            }
            Err(e) => {
                eprintln!("[CLI] Error: Failed to connect to {}: {}", current_target_addr, e);
                if attempt == max_redirects {
                    return Err(format!("Failed to connect after {} attempts. Last target: {}.", max_redirects + 1, current_target_addr));
                }
                // If connection fails, and we haven't been redirected yet in this attempt,
                // wait a bit before trying the same address again if it's the initial address,
                // or if it's a redirected address that immediately fails.
                // The loop structure will retry up to max_redirects times.
                // If the address doesn't change (no successful redirect), it will keep trying the same one.
            }
        }
    }
    Err(format!("Command failed after {} attempts. Last target: {}", max_redirects + 1, current_target_addr))
}


#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let request = match cli.command {
        Commands::Set { key, value } => ClientRequest::Set { key, value },
        Commands::Get { key } => ClientRequest::Get { key },
        Commands::Delete { key } => ClientRequest::Delete { key },
    };

    println!("[CLI] Sending command: {:?} to initial address: {}", request, cli.address);

    match send_command_to_server(cli.address, request).await {
        Ok(reply) => {
            println!("[CLI] Server Reply:");
            // Nicer printing for different reply types
            match reply {
                ClientReply::Value { key, value } => {
                    println!("  Key: {}", key);
                    match value {
                        Some(v) => println!("  Value: {}", v),
                        None => println!("  Value: (not found)"),
                    }
                }
                ClientReply::Success { command_applied_at_log_index } => {
                    println!("  Success! (Command applied at log index: {})", command_applied_at_log_index);
                }
                ClientReply::Error { msg } => {
                    println!("  Error: {}", msg);
                }
                ClientReply::LeaderRedirect { leader_id, leader_addr } => {
                    // This case should ideally be handled by the loop in send_command_to_server
                    // If we see it here, it means redirection failed or maxed out.
                    println!("  Failed to redirect. Last known leader ID: {}, Addr: {:?}", leader_id, leader_addr);
                }
            }
        }
        Err(e) => {
            eprintln!("[CLI] Error: {}", e);
        }
    }
}