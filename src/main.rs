// src/main.rs
mod core_types;

use crate::core_types::{
    AppendEntriesArgs, AppendEntriesReply, ClientReply, ClientRequest, Command, LogEntry,
    NodeState, RequestVoteArgs, RequestVoteReply, RpcMessage, Server,
};
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Clone)]
struct AppendEntriesContext {
    prev_log_index_sent: u64,
    entries_len_sent: usize,
}

#[derive(Clone)]
struct ServerConfig {
    id: u64,
    listen_addr: String,
    client_listen_addr: String,
    peer_ids: Vec<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("--- distributed kv store mmulti node setup stuff ---");

    let server_configs = vec![
        ServerConfig {
            id: 1,
            listen_addr: "127.0.0.1:8081".to_string(),
            client_listen_addr: "127.0.0.1:9081".to_string(),
            peer_ids: vec![2, 3],
        },
        ServerConfig {
            id: 2,
            listen_addr: "127.0.0.1:8082".to_string(),
            client_listen_addr: "127.0.0.1:9082".to_string(),
            peer_ids: vec![1, 3],
        },
        ServerConfig {
            id: 3,
            listen_addr: "127.0.0.1:8083".to_string(),
            client_listen_addr: "127.0.0.1:9083".to_string(),
            peer_ids: vec![1, 2],
        },
    ];

    let total_servers = server_configs.len();

    let mut server_addresses = HashMap::new();
    let mut client_addresses = HashMap::new();

    for config in &server_configs {
        server_addresses.insert(config.id, config.listen_addr.clone());
        client_addresses.insert(config.id, config.client_listen_addr.clone());
    }

    let server_addresses_arc = Arc::new(server_addresses);
    let client_addresses_arc = Arc::new(client_addresses);

    let mut server_handles = Vec::new();

    for config in server_configs {
        let server_logic = Arc::new(Mutex::new(Server::new(config.id, config.peer_ids.clone())));
        let listen_addr = config.listen_addr.clone();
        let client_listen_addr_for_task = config.client_listen_addr.clone();
        let server_id = config.id;
        let server_addresses_clone = Arc::clone(&server_addresses_arc);
        let client_addresses_clone = Arc::clone(&client_addresses_arc);

        println!(
            "[Main] Spawning server task for S{} (Raft on {}, Client on {})",
            server_id, listen_addr, client_listen_addr_for_task
        );

        let handle = tokio::spawn(async move {
            run_server(
                server_id,
                server_logic,
                listen_addr,
                client_listen_addr_for_task, // new parameter
                server_addresses_clone,
                client_addresses_clone, // new parameter
                total_servers,
            )
            .await;
        });
        server_handles.push(handle);
    }

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        println!("[TestClient] Attempting to send a SET command...");
        send_test_client_command(
            "127.0.0.1:9081",
            ClientRequest::Set {
                key: "my_cluster_key".to_string(),
                value: "my_cluster_value".to_string(),
            },
        )
        .await;
    });

    for handle in server_handles {
        let _ = handle.await;
    }

    Ok(())
}

async fn run_server(
    id: u64,
    server_logic_arc: Arc<Mutex<Server>>,
    raft_listen_addr: String,   // renamed for clarity
    client_listen_addr: String, // new: address for this server's client listener if it's leader
    all_raft_addrs: Arc<HashMap<u64, String>>,
    _all_client_addrs: Arc<HashMap<u64, String>>,
    total_servers_in_cluster: usize,
) {
    println!(
        "[S{}] Attempting Raft listener on {}...",
        id, raft_listen_addr
    );
    let raft_listener = match TcpListener::bind(&raft_listen_addr).await {
        Ok(l) => {
            println!("[S{}] Raft listener active on {}", id, raft_listen_addr);
            l
        }
        Err(e) => {
            eprintln!(
                "[S{}] Failed Raft bind to {}: {:?}. Shutting down.",
                id, raft_listen_addr, e
            );
            return;
        }
    };

    let server_logic_for_raft_listener = Arc::clone(&server_logic_arc);
    let total_servers_for_raft = total_servers_in_cluster;
    tokio::spawn(async move {
        // task for raft peer connections
        loop {
            match raft_listener.accept().await {
                Ok((socket, addr)) => {
                    println!("[S{}] Raft connection from: {}", id, addr);
                    let server_logic_for_handler = Arc::clone(&server_logic_for_raft_listener);
                    tokio::spawn(async move {
                        handle_raft_connection(
                            socket,
                            id,
                            server_logic_for_handler,
                            total_servers_for_raft,
                        )
                        .await;
                    });
                }
                Err(e) => eprintln!("[S{}] Failed Raft accept: {:?}", id, e),
            }
        }
    });

    let server_logic_for_client_loop = Arc::clone(&server_logic_arc);
    let client_listen_addr_for_task = client_listen_addr.clone();

    tokio::spawn(async move {
        let mut client_listener_active: Option<TcpListener> = None;
        loop {
            let is_leader;
            {
                // scope for mutex guard
                let server_guard = server_logic_for_client_loop.lock().await;
                is_leader = server_guard.state == NodeState::Leader;
            }

            if is_leader && client_listener_active.is_none() {
                println!(
                    "[S{}] Became Leader, attempting client listener on {}...",
                    id, client_listen_addr_for_task
                );
                match TcpListener::bind(&client_listen_addr_for_task).await {
                    Ok(listener) => {
                        println!(
                            "[S{}] Client listener active on {}",
                            id, client_listen_addr_for_task
                        );
                        client_listener_active = Some(listener);
                    }
                    Err(e) => {
                        eprintln!(
                            "[S{}] Failed to bind client listener on {}: {:?}",
                            id, client_listen_addr_for_task, e
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            } else if !is_leader && client_listener_active.is_some() {
                println!(
                    "[S{}] No longer Leader, stopping client listener on {}.",
                    id, client_listen_addr_for_task
                );
                client_listener_active = None;
            }

            if let Some(listener) = &client_listener_active {
                tokio::select! {
                    biased; // check leader status more eagerly
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    }
                    result = listener.accept() => {
                        match result {
                            Ok((socket, addr)) => {
                                println!("[S{}] Client connection from: {}", id, addr);
                                let server_logic_for_client_handler = Arc::clone(&server_logic_for_client_loop);
                                tokio::spawn(async move {
                                    handle_client_connection(socket, id, server_logic_for_client_handler).await;
                                });
                            }
                            Err(e) => eprintln!("[S{}] Failed client accept: {:?}", id, e),
                        }
                    }
                }
            } else {
                // not leader or listener not active
                tokio::time::sleep(Duration::from_millis(200)).await; // check leadership periodically
            }
        }
    });

    let mut tick_interval = tokio::time::interval(Duration::from_millis(100));
    loop {
        loop {
            tick_interval.tick().await;

            let messages_to_send_tuples;
            let current_server_id_for_log;
            {
                let mut server_guard = server_logic_arc.lock().await;
                messages_to_send_tuples = server_guard.tick();
                current_server_id_for_log = server_guard.id;
            }

            if !messages_to_send_tuples.is_empty() {
                println!(
                    "[S{}] Tick generated {} messages to send.",
                    current_server_id_for_log,
                    messages_to_send_tuples.len()
                );
            }

            for (target_peer_id, rpc_message) in messages_to_send_tuples {
                if let Some(target_addr_str) = all_raft_addrs.get(&target_peer_id) {
                    let message_to_send = rpc_message.clone();
                    let target_addr_owned = target_addr_str.clone();
                    let server_logic_for_rpc_task = Arc::clone(&server_logic_arc);

                    let ae_context_for_reply =
                        if let RpcMessage::AppendEntries(ref args_rpc) = message_to_send {
                            // Changed args to args_rpc
                            Some(AppendEntriesContext {
                                prev_log_index_sent: args_rpc.prev_log_index,
                                entries_len_sent: args_rpc.entries.len(),
                            })
                        } else {
                            None
                        };

                    tokio::spawn(async move {
                        let sender_id_for_this_rpc = server_logic_for_rpc_task.lock().await.id;
                        println!(
                            "[S{} -> S{}] Attempting to send: {:?}",
                            sender_id_for_this_rpc, target_peer_id, message_to_send
                        );

                        match TcpStream::connect(&target_addr_owned).await {
                            Ok(mut stream) => {
                                match bincode::serialize(&message_to_send) {
                                    Ok(serialized_msg) => {
                                        let msg_len = serialized_msg.len() as u32;
                                        if stream.write_all(&msg_len.to_be_bytes()).await.is_err() {
                                            return;
                                        }
                                        if stream.write_all(&serialized_msg).await.is_err() {
                                            return;
                                        }

                                        let mut len_bytes = [0u8; 4];
                                        match stream.read_exact(&mut len_bytes).await {
                                            Ok(_) => {
                                                let reply_len =
                                                    u32::from_be_bytes(len_bytes) as usize;
                                                if reply_len > 0 && reply_len < 1_048_576 {
                                                    let mut reply_buffer = vec![0u8; reply_len];
                                                    if stream
                                                        .read_exact(&mut reply_buffer)
                                                        .await
                                                        .is_ok()
                                                    {
                                                        match bincode::deserialize::<RpcMessage>(
                                                            &reply_buffer,
                                                        ) {
                                                            Ok(reply_rpc) => {
                                                                println!(
                                                                    "[S{} <- S{}] Received reply: {:?}",
                                                                    sender_id_for_this_rpc,
                                                                    target_peer_id,
                                                                    reply_rpc
                                                                );
                                                                let mut server_guard =
                                                                    server_logic_for_rpc_task
                                                                        .lock()
                                                                        .await;
                                                                match reply_rpc {
                                                                RpcMessage::RequestVoteReply(rvr_args) => {
                                                                    if let Some(_new_leader_msgs) = server_guard.handle_request_vote_reply(rvr_args, target_peer_id, total_servers_in_cluster) {
                                                                        // Queuing new_leader_msgs needs a proper channel or shared queue
                                                                        println!("[S{}] Became LEADER after vote reply, initial heartbeats generated (TODO: dispatch them).", server_guard.id);
                                                                    }
                                                                }
                                                                RpcMessage::AppendEntriesReply(aer_args) => {
                                                                    if let Some(context) = ae_context_for_reply {
                                                                        server_guard.handle_append_entries_reply(target_peer_id, aer_args, context.prev_log_index_sent, context.entries_len_sent, total_servers_in_cluster);
                                                                    } else { /* error */ }
                                                                }
                                                                _ => {}
                                                            }
                                                            }
                                                            Err(e) => eprintln!(
                                                                "[S{} <- S{}] Deserialize reply error: {:?}",
                                                                sender_id_for_this_rpc,
                                                                target_peer_id,
                                                                e
                                                            ),
                                                        }
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                if e.kind() != std::io::ErrorKind::UnexpectedEof { /* ... */
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => eprintln!(
                                        "[S{} -> S{}] Serialize error: {:?}",
                                        sender_id_for_this_rpc, target_peer_id, e
                                    ),
                                }
                                let _ = stream.shutdown().await;
                            }
                            Err(e) => eprintln!(
                                "[S{} -> S{}] Connect error to {}: {:?}",
                                sender_id_for_this_rpc, target_peer_id, target_addr_owned, e
                            ),
                        }
                    });
                } else {
                    eprintln!(
                        "[S{}] Error: Could not find address for peer_id {}",
                        id, target_peer_id
                    );
                }
            }
        }
    }
}

// Renamed original handle_connection to handle_raft_connection
async fn handle_raft_connection(
    mut stream: TcpStream,
    server_id_context: u64,
    server_logic_arc: Arc<Mutex<Server>>,
    total_servers: usize, // This was already passed, good
) {
    let peer_addr = stream
        .peer_addr()
        .unwrap_or_else(|_| "unknown peer".parse().unwrap());
    // println!("[S{}] Raft connection handler for: {:?}", server_id_context, peer_addr); // Redundant with accept log

    loop {
        let mut len_bytes = [0u8; 4];
        match stream.read_exact(&mut len_bytes).await {
            Ok(_) => {
                /* ... same as your existing handle_connection for reading/deserializing RPCs ... */
                let msg_len = u32::from_be_bytes(len_bytes) as usize;
                if msg_len == 0 {
                    continue;
                }
                if msg_len > 1_048_576 {
                    return;
                }

                let mut msg_buffer = vec![0u8; msg_len];
                match stream.read_exact(&mut msg_buffer).await {
                    Ok(_) => {
                        match bincode::deserialize::<RpcMessage>(&msg_buffer) {
                            Ok(rpc_message) => {
                                // println!("[S{}] Raft Deserialized: {:?}", server_id_context, rpc_message);
                                let mut reply_rpc_message: Option<RpcMessage> = None;
                                let mut ae_context_for_handler: Option<AppendEntriesContext> = None;

                                if let RpcMessage::AppendEntries(ref args_ref) = rpc_message {
                                    ae_context_for_handler = Some(AppendEntriesContext {
                                        prev_log_index_sent: args_ref.prev_log_index, // This context is for what the *sender* sent,
                                        entries_len_sent: args_ref.entries.len(), // not directly used by handle_append_entries itself here.
                                    }); // It's more for the sender when it gets a reply.
                                } // This seems like a slight mix-up of where context is needed.
                                // handle_append_entries_reply needs it.

                                let mut server_guard = server_logic_arc.lock().await;

                                match rpc_message {
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
                                    _ => {
                                        eprintln!(
                                            "[S{}] Raft listener received a reply type message, unexpected.",
                                            server_id_context
                                        );
                                    }
                                }
                                drop(server_guard);

                                if let Some(reply_to_send) = reply_rpc_message {
                                    // println!("[S{}] Raft sending reply: {:?}", server_id_context, reply_to_send);
                                    match bincode::serialize(&reply_to_send) {
                                        Ok(serialized_reply) => {
                                            let reply_len = serialized_reply.len() as u32;
                                            if stream
                                                .write_all(&reply_len.to_be_bytes())
                                                .await
                                                .is_err()
                                            {
                                                return;
                                            }
                                            if stream.write_all(&serialized_reply).await.is_err() {
                                                return;
                                            }
                                        }
                                        Err(e) => eprintln!(
                                            "[S{}] Raft failed to serialize reply: {:?}",
                                            server_id_context, e
                                        ),
                                    }
                                }
                            }
                            Err(e) => eprintln!(
                                "[S{}] Raft failed to deserialize from {:?}: {:?}",
                                server_id_context, peer_addr, e
                            ),
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "[S{}] Raft failed to read body from {:?}: {:?}",
                            server_id_context, peer_addr, e
                        );
                        return;
                    }
                }
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    println!(
                        "[S{}] Raft connection closed by peer {:?}",
                        server_id_context, peer_addr
                    );
                } else {
                    eprintln!(
                        "[S{}] Raft failed to read len_prefix from {:?}: {:?}",
                        server_id_context, peer_addr, e
                    );
                }
                return;
            }
        }
    }
}

// --- New function to handle client connections ---
async fn handle_client_connection(
    mut stream: TcpStream,
    server_id_context: u64, // Leader's ID
    server_logic_arc: Arc<Mutex<Server>>,
) {
    let peer_addr = stream
        .peer_addr()
        .unwrap_or_else(|_| "unknown client".parse().unwrap());
    println!(
        "[S{}] Client connection handler for: {:?}",
        server_id_context, peer_addr
    );

    loop {
        let mut len_bytes = [0u8; 4];
        match stream.read_exact(&mut len_bytes).await {
            Ok(_) => {
                let msg_len = u32::from_be_bytes(len_bytes) as usize;
                if msg_len == 0 || msg_len > 1_048_576 {
                    /* handle error or empty */
                    return;
                }

                let mut msg_buffer = vec![0u8; msg_len];
                if stream.read_exact(&mut msg_buffer).await.is_err() {
                    return;
                }

                match bincode::deserialize::<ClientRequest>(&msg_buffer) {
                    Ok(client_request) => {
                        println!(
                            "[S{}] Client Deserialized: {:?}",
                            server_id_context, client_request
                        );

                        let mut server_guard = server_logic_arc.lock().await;
                        let mut client_reply_payload: Option<ClientReply> = None;

                        if server_guard.state != NodeState::Leader {
                            // Not leader, find out who is if known (simplified)
                            // For now, just tell client we are not the leader.
                            // A real implementation might look up the known leader from its state.
                            client_reply_payload = Some(ClientReply::Error {
                                msg: "Not the leader".to_string(),
                            });
                            println!("[S{}] Not leader, telling client.", server_id_context);
                        } else {
                            // Is Leader, process request
                            match client_request {
                                ClientRequest::Set { key, value } => {
                                    println!(
                                        "[S{}] Leader received SET: k='{}', v='{}'",
                                        server_id_context, key, value
                                    );
                                    let command = Command::Set { key, value };
                                    let log_entry = LogEntry {
                                        term: server_guard.current_term,
                                        command,
                                    };
                                    server_guard.log.push(log_entry);
                                    let new_log_index = server_guard.log.len() as u64;
                                    println!(
                                        "[S{}] Leader appended to own log at index {}. Log len: {}",
                                        server_guard.id,
                                        new_log_index,
                                        server_guard.log.len()
                                    );
                                    // Replication will happen via tick. Client waits.
                                    // TODO: Implement waiting for commit before replying to client.
                                    // For now, reply optimistically or after local append.
                                    // Let's reply "Command Proposed" - actual commit confirmation is harder.
                                    client_reply_payload = Some(ClientReply::Success {
                                        command_applied_at_log_index: new_log_index,
                                    });
                                }
                                ClientRequest::Get { key } => {
                                    println!(
                                        "[S{}] Leader received GET: k='{}'",
                                        server_id_context, key
                                    );
                                    let value = server_guard.kv_store.get(&key).cloned(); // Clone to own the String
                                    client_reply_payload = Some(ClientReply::Value { key, value });
                                }
                                ClientRequest::Delete { key } => {
                                    println!(
                                        "[S{}] Leader received DELETE: k='{}'",
                                        server_id_context, key
                                    );
                                    let command = Command::Delete { key };
                                    let log_entry = LogEntry {
                                        term: server_guard.current_term,
                                        command,
                                    };
                                    server_guard.log.push(log_entry);
                                    let new_log_index = server_guard.log.len() as u64;
                                    // TODO: Wait for commit
                                    client_reply_payload = Some(ClientReply::Success {
                                        command_applied_at_log_index: new_log_index,
                                    });
                                }
                            }
                        }
                        drop(server_guard); // Release lock

                        if let Some(reply) = client_reply_payload {
                            match bincode::serialize(&reply) {
                                Ok(serialized_reply) => {
                                    let len = serialized_reply.len() as u32;
                                    if stream.write_all(&len.to_be_bytes()).await.is_err() {
                                        return;
                                    }
                                    if stream.write_all(&serialized_reply).await.is_err() {
                                        return;
                                    }
                                    println!(
                                        "[S{}] Client reply sent: {:?}",
                                        server_id_context, reply
                                    );
                                }
                                Err(e) => eprintln!(
                                    "[S{}] Failed to serialize client reply: {:?}",
                                    server_id_context, e
                                ),
                            }
                        }
                    }
                    Err(e) => eprintln!(
                        "[S{}] Client failed to deserialize: {:?}",
                        server_id_context, e
                    ),
                }
            }
            Err(e) => {
                /* ... handle error reading length ... */
                return;
            }
        }
    }
}

// Test function to send a client command
async fn send_test_client_command(target_client_addr: &str, request: ClientRequest) {
    println!(
        "[TestClient] Attempting to connect to client port {}",
        target_client_addr
    );
    match TcpStream::connect(target_client_addr).await {
        Ok(mut stream) => {
            println!(
                "[TestClient] Connected to client port {}",
                target_client_addr
            );
            println!("[TestClient] Sending client request: {:?}", request);
            match bincode::serialize(&request) {
                Ok(serialized_req) => {
                    let len = serialized_req.len() as u32;
                    if stream.write_all(&len.to_be_bytes()).await.is_err() {
                        return;
                    }
                    if stream.write_all(&serialized_req).await.is_err() {
                        return;
                    }

                    // Await and process reply
                    let mut len_bytes = [0u8; 4];
                    if stream.read_exact(&mut len_bytes).await.is_ok() {
                        let reply_len = u32::from_be_bytes(len_bytes) as usize;
                        if reply_len > 0 && reply_len < 1_048_576 {
                            let mut reply_buffer = vec![0u8; reply_len];
                            if stream.read_exact(&mut reply_buffer).await.is_ok() {
                                match bincode::deserialize::<ClientReply>(&reply_buffer) {
                                    Ok(reply) => {
                                        println!("[TestClient] Received reply: {:?}", reply)
                                    }
                                    Err(e) => eprintln!(
                                        "[TestClient] Deserialize client reply error: {:?}",
                                        e
                                    ),
                                }
                            }
                        }
                    } else {
                        eprintln!("[TestClient] Failed to read client reply length");
                    }
                }
                Err(e) => eprintln!("[TestClient] Serialize client request error: {:?}", e),
            }
            let _ = stream.shutdown().await;
        }
        Err(e) => eprintln!(
            "[TestClient] Failed to connect to client port {}: {:?}",
            target_client_addr, e
        ),
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
