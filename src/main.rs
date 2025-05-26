// src/main.rs
mod core_types;

use crate::core_types::{
    Command, LogEntry, NodeState, RpcMessage, Server, // These might become unused if main only uses Server and RpcMessage directly
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::error::Error;
use std::sync::Arc; // Keep std::sync::Arc
use tokio::sync::Mutex; // Changed: Use tokio's async-aware Mutex
use std::time::Duration;
use std::collections::VecDeque;


#[derive(Debug, Clone)] 
struct AppendEntriesContext {
    prev_log_index_sent: u64,
    entries_len_sent: usize,
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("--- simplified raft kv store server starting (with tokio) ---");

    let server_id_to_run: u64 = 1;
    let listen_addr = "127.0.0.1:8081".to_string(); // s1 listens here
    
    let all_cluster_server_ids: Vec<u64> = vec![1, 2, 3]; 
    let total_servers = all_cluster_server_ids.len();

    let peer_ids_for_this_server: Vec<u64> = all_cluster_server_ids
        .iter()
        .cloned()
        .filter(|&p_id| p_id != server_id_to_run)
        .collect();

    // server logic shared safely across async tasks
    let server_logic = Arc::new(Mutex::new(Server::new(server_id_to_run, peer_ids_for_this_server)));

    println!("[server {}] attempting to listen on {}...", server_id_to_run, listen_addr);
    let listener = TcpListener::bind(&listen_addr).await?;
    println!("[server {}] successfully listening on {}", server_id_to_run, listen_addr);

    // spawn a test client task
    let server_logic_for_test = Arc::clone(&server_logic);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await; // wait for server
        test_send_message(&listen_addr, server_logic_for_test, total_servers).await;
    });

    // main server loop: accept connections
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                println!("[server {}] new connection from: {}", server_id_to_run, addr);
                let server_logic_for_handler = Arc::clone(&server_logic);
                tokio::spawn(async move { // handle each connection concurrently
                    handle_connection(socket, server_id_to_run, server_logic_for_handler, total_servers).await;
                });
            }
            Err(e) => {
                eprintln!("[server {}] failed to accept connection: {:?}", server_id_to_run, e);
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
    let peer_addr = stream.peer_addr().unwrap_or_else(|_| "unknown peer".parse().unwrap());
    println!("[s{}] handling new stream from: {:?}", server_id_context, peer_addr);
    
    loop { // read multiple messages from same connection
        let mut len_bytes = [0u8; 4]; // for u32 length prefix
        match stream.read_exact(&mut len_bytes).await { // read message length
            Ok(_) => {
                let msg_len = u32::from_be_bytes(len_bytes) as usize; // network byte order
                
                if msg_len == 0 { println!("[s{}] received msg_len 0 from {:?}", server_id_context, peer_addr); continue; }
                if msg_len > 1_048_576 { /* 1MB limit */ eprintln!("[s{}] msg len {} too large from {:?}", server_id_context, msg_len, peer_addr); return; }

                let mut msg_buffer = vec![0u8; msg_len];
                match stream.read_exact(&mut msg_buffer).await { // read message body
                    Ok(_) => {
                        match bincode::deserialize::<RpcMessage>(&msg_buffer) { // try to convert bytes to RpcMessage
                            Ok(rpc_message) => {
                                println!("[s{}] deserialized rpc: {:?}", server_id_context, rpc_message);
                                
                                let mut reply_rpc_message: Option<RpcMessage> = None;
                                // no need for ae_context_for_reply in this simplified handler for now
                                
                                let mut server_guard = server_logic_arc.lock().await; // lock server state for modification

                                match rpc_message { // process based on message type
                                    RpcMessage::RequestVote(args) => {
                                        let reply_data = server_guard.handle_request_vote(args);
                                        reply_rpc_message = Some(RpcMessage::RequestVoteReply(reply_data));
                                    }
                                    RpcMessage::AppendEntries(args) => {
                                        let reply_data = server_guard.handle_append_entries(args);
                                        reply_rpc_message = Some(RpcMessage::AppendEntriesReply(reply_data));
                                    }
                                    // server receives requests, not replies, directly from stream in this model
                                    RpcMessage::RequestVoteReply(_) | RpcMessage::AppendEntriesReply(_) => {
                                        eprintln!("[s{}] received a reply type message directly on listener, unexpected.", server_id_context);
                                    }
                                }
                                drop(server_guard); // release lock

                                // send reply if one was generated
                                if let Some(reply_to_send) = reply_rpc_message {
                                    println!("[{}] sending reply: {:?}", server_id_context, reply_to_send);
                                    match bincode::serialize(&reply_to_send) {
                                        Ok(serialized_reply) => {
                                            let reply_len = serialized_reply.len() as u32;
                                            if stream.write_all(&reply_len.to_be_bytes()).await.is_err() { return; } // send len
                                            if stream.write_all(&serialized_reply).await.is_err() { return; }      // send body
                                            println!("[s{}] reply sent successfully to {:?}", server_id_context, peer_addr);
                                        }
                                        Err(e) => eprintln!("[s{}] failed to serialize reply: {:?}", server_id_context, e),
                                    }
                                }
                            }
                            Err(e) => eprintln!("[s{}] failed to deserialize message from {:?}: {:?}", server_id_context, peer_addr, e),
                        }
                    }
                    Err(e) => { eprintln!("[s{}] failed to read message body from {:?}: {:?}", server_id_context, peer_addr, e); return; }
                }
            }
            Err(e) => { // error reading length (often connection closed)
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    println!("[s{}] connection closed by peer {:?}", server_id_context, peer_addr);
                } else { eprintln!("[s{}] failed to read length prefix from {:?}: {:?}", server_id_context, peer_addr, e); }
                return; 
            }
        }
    }
}

// test client: sends one message and tries to read one reply
async fn test_send_message(target_addr: &str, _server_logic_arc: Arc<Mutex<Server>>, _total_servers: usize) {
    println!("[testsender] attempting to connect to {}", target_addr);
    match TcpStream::connect(target_addr).await {
        Ok(mut stream) => {
            println!("[testsender] connected to {}", target_addr);

            let vote_request_args = RequestVoteArgs { // example message
                term: 1, candidate_id: 100, last_log_index: 0, last_log_term: 0,
            };
            let rpc_message_to_send = RpcMessage::RequestVote(vote_request_args);
            
            println!("[testsender] sending message: {:?}", rpc_message_to_send);
            match bincode::serialize(&rpc_message_to_send) {
                Ok(serialized_msg) => {
                    let msg_len = serialized_msg.len() as u32;
                    // send length prefix
                    if stream.write_all(&msg_len.to_be_bytes()).await.is_err() { eprintln!("[testsender] send length failed"); return; }
                    // send message
                    if stream.write_all(&serialized_msg).await.is_err() { eprintln!("[testsender] send message failed"); return; }
                    println!("[testsender] message sent. awaiting reply...");

                    // read reply length
                    let mut len_bytes = [0u8; 4];
                    match stream.read_exact(&mut len_bytes).await {
                        Ok(_) => {
                            let reply_len = u32::from_be_bytes(len_bytes) as usize;
                            println!("[testsender] received reply length: {}", reply_len);
                            if reply_len > 0 && reply_len < 1_048_576 { // basic sanity check
                                let mut reply_buffer = vec![0u8; reply_len];
                                // read reply body
                                match stream.read_exact(&mut reply_buffer).await {
                                    Ok(_) => {
                                        // deserialize reply
                                        match bincode::deserialize::<RpcMessage>(&reply_buffer) {
                                            Ok(reply_rpc) => {
                                                println!("[testsender] received reply: {:?}", reply_rpc);
                                                if let RpcMessage::RequestVoteReply(rvr) = reply_rpc {
                                                    println!("[testsender] vote granted: {}", rvr.vote_granted);
                                                }
                                            }
                                            Err(e) => eprintln!("[testsender] failed to deserialize reply: {:?}", e),
                                        }
                                    }
                                    Err(e) => eprintln!("[testsender] failed to read reply body: {:?}", e),
                                }
                            } else if reply_len == 0 { /* ... */ } else { /* ... */ }
                        }
                        Err(e) => { /* ... */ },
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