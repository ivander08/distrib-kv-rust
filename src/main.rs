// src/main.rs
mod core_types;

use crate::core_types::{
    AppendEntriesArgs, AppendEntriesReply, Command, LogEntry, NodeState, RequestVoteArgs,
    RequestVoteReply, RpcMessage, Server,
};
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
struct AppendEntriesContext {
    prev_log_index_sent: u64,
    entries_len_sent: usize,
}

#[derive(Clone)]
struct ServerConfig {
    id: u64,
    listen_addr: String,
    peer_ids: Vec<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("--- distributed kv store mmulti node setup stuff ---");

    let server_configs = vec![
        ServerConfig {
            id: 1,
            listen_addr: "127.0.0.1:8081".to_string(),
            peer_ids: vec![2, 3],
        },
        ServerConfig {
            id: 2,
            listen_addr: "127.0.0.1:8082".to_string(),
            peer_ids: vec![1, 3],
        },
        ServerConfig {
            id: 3,
            listen_addr: "127.0.0.1:8083".to_string(),
            peer_ids: vec![1, 2],
        },
    ];

    let total_servers = server_configs.len();

    let mut server_addresses = HashMap::new();
    for config in &server_configs {
        server_addresses.insert(config.id, config.listen_addr.clone());
    }
    let server_addresses_arc = Arc::new(server_addresses);

    let mut server_handles = Vec::new();

    for config in server_configs {
        let server_logic = Arc::new(Mutex::new(Server::new(config.id, config.peer_ids.clone())));
        let listen_addr = config.listen_addr.clone();
        let server_id = config.id;
        let server_addresses_clone = Arc::clone(&server_addresses_arc);

        println!(
            "[Main] Spawning server task for S{} on {}",
            server_id, listen_addr
        );

        let handle = tokio::spawn(async move {
            run_server(
                server_id,
                server_logic,
                listen_addr,
                server_addresses_clone,
                total_servers,
            )
            .await;
        });
        server_handles.push(handle);
    }

    for handle in server_handles {
        let _ = handle.await;
    }

    Ok(())
}

async fn run_server(
    id: u64,
    server_logic_arc: Arc<Mutex<Server>>,
    listen_addr: String,
    all_server_addrs: Arc<HashMap<u64, String>>,
    total_servers_in_cluster: usize,
) {
    println!("[S{}] Attempting to listen on {}...", id, listen_addr);
    let listener = match TcpListener::bind(&listen_addr).await {
        Ok(l) => {
            println!("[S{}] Successfully listening on {}", id, listen_addr);
            l
        }
        Err(e) => {
            eprintln!(
                "[S{}] Failed to bind to {}: {:?}. Shutting down this server task.",
                id, listen_addr, e
            );
            return;
        }
    };

    let server_logic_for_listener = Arc::clone(&server_logic_arc);
    let total_servers_for_listener = total_servers_in_cluster;

    let listen_task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    println!("[S{}] New connection from: {}", id, addr);
                    let server_logic_for_handler = Arc::clone(&server_logic_for_listener);
                    tokio::spawn(async move {
                        handle_connection(
                            socket,
                            id,
                            server_logic_for_handler,
                            total_servers_for_listener,
                        )
                        .await;
                    });
                }
                Err(e) => {
                    eprintln!("[S{}] Failed to accept connection: {:?}", id, e);
                }
            }
        }
    });

    let mut tick_interval = tokio::time::interval(Duration::from_millis(100));

    loop {
        tokio::select! {
            _ = tick_interval.tick() => {
                let mut server_guard = server_logic_arc.lock().await;
                let messages_to_send_tuples = server_guard.tick();
                drop(server_guard);

                if !messages_to_send_tuples.is_empty() {
                    println!("[S{}] Tick generated {} messages to send.", id, messages_to_send_tuples.len());
                }

                for (target_peer_id, rpc_message) in messages_to_send_tuples {
                    let target_addr_option = all_server_addrs.get(&target_peer_id);

                    if let Some(target_addr_str) = target_addr_option {
                        let message_to_send = rpc_message.clone();
                        let target_addr_owned = target_addr_str.clone();
                        let current_server_id = id;

                        let ae_context_for_reply = if let RpcMessage::AppendEntries(args) = &message_to_send {
                            Some(AppendEntriesContext {
                                prev_log_index_sent: args.prev_log_index,
                                entries_len_sent: args.entries.len(),
                            })
                        } else {
                            None
                        };

                        let server_logic_for_rpc_task = Arc::clone(&server_logic_arc);

                        tokio::spawn(async move {
                            println!("[S{} -> S{}] Attempting to send: {:?}", current_server_id, target_peer_id, message_to_send);
                            match TcpStream::connect(&target_addr_owned).await {
                                Ok(mut stream) => {
                                    match bincode::serialize(&message_to_send) {
                                        Ok(serialized_msg) => {
                                            let msg_len = serialized_msg.len() as u32;
                                            if stream.write_all(&msg_len.to_be_bytes()).await.is_err() {
                                                eprintln!("[S{} -> S{}] Send length failed to {}", current_server_id, target_peer_id, target_addr_owned);
                                                return;
                                            }
                                            if stream.write_all(&serialized_msg).await.is_err() {
                                                eprintln!("[S{} -> S{}] Send message failed to {}", current_server_id, target_peer_id, target_addr_owned);
                                                return;
                                            }
                                            println!("[S{} -> S{}] Message sent. Awaiting reply...", current_server_id, target_peer_id);

                                            let mut len_bytes = [0u8; 4];
                                            match stream.read_exact(&mut len_bytes).await {
                                                Ok(_) => {
                                                    let reply_len = u32::from_be_bytes(len_bytes) as usize;
                                                    if reply_len > 0 && reply_len < 1_048_576 {
                                                        let mut reply_buffer = vec![0u8; reply_len];
                                                        if stream.read_exact(&mut reply_buffer).await.is_ok() {
                                                            match bincode::deserialize::<RpcMessage>(&reply_buffer) {
                                                                Ok(reply_rpc) => {
                                                                    println!("[S{} <- S{}] Received reply: {:?}", current_server_id, target_peer_id, reply_rpc);
                                                                    let mut server_guard = server_logic_for_rpc_task.lock().await;
                                                                    match reply_rpc {
                                                                        RpcMessage::RequestVoteReply(rvr_args) => {
                                                                            if let Some(new_leader_msgs) = server_guard.handle_request_vote_reply(rvr_args, target_peer_id, total_servers_in_cluster) {
                                                                                println!("[S{}] Became LEADER, should send {} heartbeats.", server_guard.id, new_leader_msgs.len());
                                                                            }
                                                                        }
                                                                        RpcMessage::AppendEntriesReply(aer_args) => {
                                                                            if let Some(context) = ae_context_for_reply {
                                                                                server_guard.handle_append_entries_reply(target_peer_id, aer_args, context.prev_log_index_sent, context.entries_len_sent, total_servers_in_cluster);
                                                                            } else {
                                                                                eprintln!("[S{}] Error: Missing context for AppendEntriesReply from S{}", server_guard.id, target_peer_id);
                                                                            }
                                                                        }
                                                                        _ => {}
                                                                    }
                                                                }
                                                                Err(e) => eprintln!("[S{} <- S{}] Failed to deserialize reply: {:?}", current_server_id, target_peer_id, e),
                                                            }
                                                        } else { eprintln!("[S{} <- S{}] Failed to read reply body", current_server_id, target_peer_id); }
                                                    } else { eprintln!("[S{} <- S{}] Invalid reply length: {}", current_server_id, target_peer_id, reply_len); }
                                                }
                                                Err(e) => {
                                                    if e.kind() != std::io::ErrorKind::UnexpectedEof { 
                                                        eprintln!("[S{} <- S{}] Failed to read reply length: {:?}", current_server_id, target_peer_id, e);
                                                    } else {
                                                        println!("[S{} <- S{}] Connection closed by peer after sending reply (or no reply sent).", current_server_id, target_peer_id);
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => eprintln!("[S{} -> S{}] Failed to serialize message: {:?}", current_server_id, target_peer_id, e),
                                    }
                                    let _ = stream.shutdown().await;
                                }
                                Err(e) => {
                                    eprintln!("[S{} -> S{}] Failed to connect to {}: {:?}", current_server_id, target_peer_id, target_addr_owned, e);
                                }
                            }
                        });
                    } else {
                        eprintln!("[S{}] Error: Could not find address for peer_id {}", id, target_peer_id);
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(u64::MAX)) => {

            }
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    server_id_context: u64, // for logging
    server_logic_arc: Arc<Mutex<Server>>,
    total_servers: usize,
) {
    let peer_addr = stream
        .peer_addr()
        .unwrap_or_else(|_| "unknown peer".parse().unwrap());
    println!(
        "[s{}] handling new stream from: {:?}",
        server_id_context, peer_addr
    );

    loop {
        // read multiple messages from same connection
        let mut len_bytes = [0u8; 4]; // for u32 length prefix
        match stream.read_exact(&mut len_bytes).await {
            // read message length
            Ok(_) => {
                let msg_len = u32::from_be_bytes(len_bytes) as usize; // network byte order

                if msg_len == 0 {
                    println!(
                        "[s{}] received msg_len 0 from {:?}",
                        server_id_context, peer_addr
                    );
                    continue;
                }
                if msg_len > 1_048_576 {
                    /* 1MB limit */
                    eprintln!(
                        "[s{}] msg len {} too large from {:?}",
                        server_id_context, msg_len, peer_addr
                    );
                    return;
                }

                let mut msg_buffer = vec![0u8; msg_len];
                match stream.read_exact(&mut msg_buffer).await {
                    // read message body
                    Ok(_) => {
                        match bincode::deserialize::<RpcMessage>(&msg_buffer) {
                            // try to convert bytes to RpcMessage
                            Ok(rpc_message) => {
                                println!(
                                    "[s{}] deserialized rpc: {:?}",
                                    server_id_context, rpc_message
                                );

                                let mut reply_rpc_message: Option<RpcMessage> = None;
                                // no need for ae_context_for_reply in this simplified handler for now

                                let mut server_guard = server_logic_arc.lock().await; // lock server state for modification

                                match rpc_message {
                                    // process based on message type
                                    RpcMessage::RequestVote(args) => {
                                        let reply_data = server_guard.handle_request_vote(args);
                                        reply_rpc_message =
                                            Some(RpcMessage::RequestVoteReply(reply_data));
                                    }
                                    RpcMessage::AppendEntries(args) => {
                                        let reply_data = server_guard.handle_append_entries(args);
                                        reply_rpc_message =
                                            Some(RpcMessage::AppendEntriesReply(reply_data));
                                    }
                                    // server receives requests, not replies, directly from stream in this model
                                    RpcMessage::RequestVoteReply(_)
                                    | RpcMessage::AppendEntriesReply(_) => {
                                        eprintln!(
                                            "[s{}] received a reply type message directly on listener, unexpected.",
                                            server_id_context
                                        );
                                    }
                                }
                                drop(server_guard); // release lock

                                // send reply if one was generated
                                if let Some(reply_to_send) = reply_rpc_message {
                                    println!(
                                        "[{}] sending reply: {:?}",
                                        server_id_context, reply_to_send
                                    );
                                    match bincode::serialize(&reply_to_send) {
                                        Ok(serialized_reply) => {
                                            let reply_len = serialized_reply.len() as u32;
                                            if stream
                                                .write_all(&reply_len.to_be_bytes())
                                                .await
                                                .is_err()
                                            {
                                                return;
                                            } // send len
                                            if stream.write_all(&serialized_reply).await.is_err() {
                                                return;
                                            } // send body
                                            println!(
                                                "[s{}] reply sent successfully to {:?}",
                                                server_id_context, peer_addr
                                            );
                                        }
                                        Err(e) => eprintln!(
                                            "[s{}] failed to serialize reply: {:?}",
                                            server_id_context, e
                                        ),
                                    }
                                }
                            }
                            Err(e) => eprintln!(
                                "[s{}] failed to deserialize message from {:?}: {:?}",
                                server_id_context, peer_addr, e
                            ),
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "[s{}] failed to read message body from {:?}: {:?}",
                            server_id_context, peer_addr, e
                        );
                        return;
                    }
                }
            }
            Err(e) => {
                // error reading length (often connection closed)
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    println!(
                        "[s{}] connection closed by peer {:?}",
                        server_id_context, peer_addr
                    );
                } else {
                    eprintln!(
                        "[s{}] failed to read length prefix from {:?}: {:?}",
                        server_id_context, peer_addr, e
                    );
                }
                return;
            }
        }
    }
}

// test client: sends one message and tries to read one reply
async fn test_send_message(
    target_addr: &str,
    _server_logic_arc: Arc<Mutex<Server>>,
    _total_servers: usize,
) {
    println!("[testsender] attempting to connect to {}", target_addr);
    match TcpStream::connect(target_addr).await {
        Ok(mut stream) => {
            println!("[testsender] connected to {}", target_addr);

            let vote_request_args = RequestVoteArgs {
                // example message
                term: 1,
                candidate_id: 100,
                last_log_index: 0,
                last_log_term: 0,
            };
            let rpc_message_to_send = RpcMessage::RequestVote(vote_request_args);

            println!("[testsender] sending message: {:?}", rpc_message_to_send);
            match bincode::serialize(&rpc_message_to_send) {
                Ok(serialized_msg) => {
                    let msg_len = serialized_msg.len() as u32;
                    // send length prefix
                    if stream.write_all(&msg_len.to_be_bytes()).await.is_err() {
                        eprintln!("[testsender] send length failed");
                        return;
                    }
                    // send message
                    if stream.write_all(&serialized_msg).await.is_err() {
                        eprintln!("[testsender] send message failed");
                        return;
                    }
                    println!("[testsender] message sent. awaiting reply...");

                    // read reply length
                    let mut len_bytes = [0u8; 4];
                    match stream.read_exact(&mut len_bytes).await {
                        Ok(_) => {
                            let reply_len = u32::from_be_bytes(len_bytes) as usize;
                            println!("[testsender] received reply length: {}", reply_len);
                            if reply_len > 0 && reply_len < 1_048_576 {
                                // basic sanity check
                                let mut reply_buffer = vec![0u8; reply_len];
                                // read reply body
                                match stream.read_exact(&mut reply_buffer).await {
                                    Ok(_) => {
                                        // deserialize reply
                                        match bincode::deserialize::<RpcMessage>(&reply_buffer) {
                                            Ok(reply_rpc) => {
                                                println!(
                                                    "[testsender] received reply: {:?}",
                                                    reply_rpc
                                                );
                                                if let RpcMessage::RequestVoteReply(rvr) = reply_rpc
                                                {
                                                    println!(
                                                        "[testsender] vote granted: {}",
                                                        rvr.vote_granted
                                                    );
                                                }
                                            }
                                            Err(e) => eprintln!(
                                                "[testsender] failed to deserialize reply: {:?}",
                                                e
                                            ),
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("[testsender] failed to read reply body: {:?}", e)
                                    }
                                }
                            } else if reply_len == 0 { /* ... */
                            } else { /* ... */
                            }
                        }
                        Err(e) => { /* ... */ }
                    }
                }
                Err(e) => eprintln!("[testsender] failed to serialize message: {:?}", e),
            }
            if stream.shutdown().await.is_err() { /* ignore error during shutdown for test */ }
        }
        Err(e) => eprintln!("[testsender] failed to connect to {}: {:?}", target_addr, e),
    }
}

// helper to print server details briefly
fn print_server_details_brief(server: &Server) {
    println!(
        "s{}: t={}, s={:?}, log_len={}, cidx={}, aidx={}, voted={:?}, votes_rcv={}, kv_len={}", // shortened field names
        server.id,
        server.current_term,
        server.state,
        server.log.len(),
        server.commit_index,
        server.last_applied,
        server.voted_for,
        server.votes_received.len(),
        // server.next_index.len(), // can be verbose if many peers
        // server.match_index.len(),
        server.kv_store.len()
    );
}
