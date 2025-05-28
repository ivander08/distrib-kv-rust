mod core_types;

use crate::core_types::{
    ClientReply, ClientRequest, Command, LogEntry, NodeState, RpcMessage, Server,
};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, oneshot};
use std::fs::create_dir_all;

const DETAILED_LOGS: bool = true;

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
    data_dir: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("SYS INIT: --- distributed kv store multi node setup ---");

    let base_data_dir = "raft_data_p1";
    if let Err(e) = create_dir_all(base_data_dir) {
        eprintln!(
            "FATAL: Could not create base data directory {}: {}", 
            base_data_dir, 
            e
        );
        return Err(e.into());
    }

    let server_configs = vec![
        ServerConfig {
            id: 1,
            listen_addr: "127.0.0.1:8081".to_string(),
            client_listen_addr: "127.0.0.1:9081".to_string(),
            peer_ids: vec![2, 3],
            data_dir: format!("{}/server1", base_data_dir),
        },
        ServerConfig {
            id: 2,
            listen_addr: "127.0.0.1:8082".to_string(),
            client_listen_addr: "127.0.0.1:9082".to_string(),
            peer_ids: vec![1, 3],
            data_dir: format!("{}/server2", base_data_dir),
        },
        ServerConfig {
            id: 3,
            listen_addr: "127.0.0.1:8083".to_string(),
            client_listen_addr: "127.0.0.1:9083".to_string(),
            peer_ids: vec![1, 2],
            data_dir: format!("{}/server3", base_data_dir),
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
        if let Err(e) = create_dir_all(&config.data_dir) {
            eprintln!(
                "FATAL: Could not create data directory {} for S{}: {}", 
                &config.data_dir, 
                config.id, 
                e
            );
            return Err(e.into());
        }
        
        let metadata_path = format!("{}/metadata.dat", config.data_dir);
        let log_path = format!("{}/log.wal", config.data_dir);

        let server_logic = Arc::new(Mutex::new(Server::new(
            config.id,
            config.peer_ids.clone(),
            metadata_path,
            log_path,
        )));
        
        let listen_addr = config.listen_addr.clone();
        let client_listen_addr_for_task = config.client_listen_addr.clone();
        let server_id = config.id;
        let server_addresses_clone = Arc::clone(&server_addresses_arc);
        let client_addresses_clone = Arc::clone(&client_addresses_arc);

        println!(
            "SYS SERVER_SPAWN: Spawning S{} (Raft: {}, Client: {}, Data: {})",
            server_id, 
            listen_addr, 
            client_listen_addr_for_task, 
            config.data_dir
        );

        let handle = tokio::spawn(async move {
            run_server(
                server_id,
                server_logic,
                listen_addr,
                client_listen_addr_for_task,
                server_addresses_clone,
                client_addresses_clone,
                total_servers,
            ).await;
        });
        server_handles.push(handle);
    }

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        println!("[TestClient] CMD_SEND_ATTEMPT: Attempting to send a SET command...");
        send_test_client_command(
            "127.0.0.1:9081", 
            ClientRequest::Set {
                key: "my_cluster_key_redirect_test".to_string(),
                value: "value_after_redirect".to_string(),
            },
        ).await;

        tokio::time::sleep(Duration::from_secs(2)).await;
        println!("[TestClient] CMD_SEND_ATTEMPT: Attempting to send a GET command...");
        send_test_client_command(
            "127.0.0.1:9082",
            ClientRequest::Get {
                key: "my_cluster_key_redirect_test".to_string(),
            },
        ).await;
    });

    for handle in server_handles {
        let _ = handle.await;
    }
    Ok(())
}

async fn run_server(
    id: u64,
    server_logic_arc: Arc<Mutex<Server>>,
    raft_listen_addr: String,
    client_listen_addr: String,
    all_raft_addrs: Arc<HashMap<u64, String>>,
    all_client_addrs: Arc<HashMap<u64, String>>,
    total_servers_in_cluster: usize,
) {
    println!(
        "S{} RAFT_LISTENER_INIT: Attempting Raft listener on {}...",
        id, 
        raft_listen_addr
    );
    
    let raft_listener = match TcpListener::bind(&raft_listen_addr).await {
        Ok(l) => {
            println!(
                "S{} RAFT_LISTENER_OK: Raft listener active on {}", 
                id, 
                raft_listen_addr
            );
            l
        }
        Err(e) => {
            eprintln!(
                "S{} RAFT_BIND_ERR: Failed Raft bind to {}: {:?}. Shutting down.",
                id, 
                raft_listen_addr, 
                e
            );
            return;
        }
    };

    let server_logic_for_raft_listener = Arc::clone(&server_logic_arc);
    let total_servers_for_raft = total_servers_in_cluster;
    let all_client_addrs_for_loop = Arc::clone(&all_client_addrs);
    
    tokio::spawn(async move {
        loop {
            match raft_listener.accept().await {
                Ok((socket, addr)) => {
                    println!(
                        "S{} RAFT_CONN_NEW: Raft connection from: {}", 
                        id, 
                        addr
                    );
                    let server_logic_for_handler = Arc::clone(&server_logic_for_raft_listener);
                    tokio::spawn(async move {
                        handle_raft_connection(
                            socket,
                            id,
                            server_logic_for_handler,
                            total_servers_for_raft,
                        ).await;
                    });
                }
                Err(e) => {
                    eprintln!(
                        "S{} RAFT_ACCEPT_ERR: Failed Raft accept: {:?}", 
                        id, 
                        e
                    );
                }
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
                let server_guard = server_logic_for_client_loop.lock().await;
                is_leader = server_guard.state == NodeState::Leader;
            }

            if is_leader && client_listener_active.is_none() {
                println!(
                    "S{} CLIENT_LISTENER_INIT: Became Leader, attempting client listener on {}...",
                    id, 
                    client_listen_addr_for_task
                );
                
                match TcpListener::bind(&client_listen_addr_for_task).await {
                    Ok(listener) => {
                        println!(
                            "S{} CLIENT_LISTENER_OK: Client listener active on {}",
                            id, 
                            client_listen_addr_for_task
                        );
                        client_listener_active = Some(listener);
                    }
                    Err(e) => {
                        eprintln!(
                            "S{} CLIENT_BIND_ERR: Failed to bind client listener on {}: {:?}",
                            id, 
                            client_listen_addr_for_task, 
                            e
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            } else if !is_leader && client_listener_active.is_some() {
                println!(
                    "S{} CLIENT_LISTENER_DOWN: No longer Leader, stopping client listener on {}.",
                    id, 
                    client_listen_addr_for_task
                );
                client_listener_active = None;
            }

            if let Some(listener) = &client_listener_active {
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                    result = listener.accept() => {
                        match result {
                            Ok((socket, addr)) => {
                                println!(
                                    "S{} CLIENT_CONN_NEW: Client connection from: {}", 
                                    id, 
                                    addr
                                );

                                let server_logic_for_client_handler = Arc::clone(&server_logic_for_client_loop);
                                let all_client_addrs_for_handler = Arc::clone(&all_client_addrs_for_loop);

                                tokio::spawn(async move {
                                    handle_client_connection(
                                        socket, 
                                        id, 
                                        server_logic_for_client_handler,
                                        all_client_addrs_for_handler,
                                    ).await;
                                });
                            }
                            Err(e) => {
                                eprintln!(
                                    "S{} CLIENT_ACCEPT_ERR: Failed client accept: {:?}", 
                                    id, 
                                    e
                                );
                            }
                        }
                    }
                }
            } else {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    });

    let mut tick_interval = tokio::time::interval(Duration::from_millis(100));
    loop {
        tick_interval.tick().await;

        let messages_to_send_tuples;
        let current_server_id_for_log;
        {
            let mut server_guard = server_logic_arc.lock().await;
            messages_to_send_tuples = server_guard.tick();
            current_server_id_for_log = server_guard.id;
        }

        if DETAILED_LOGS && !messages_to_send_tuples.is_empty() {
            println!(
                "S{} TICK_MESSAGES: Tick generated {} messages to send.",
                current_server_id_for_log,
                messages_to_send_tuples.len()
            );
        }

        for (target_peer_id, rpc_message) in messages_to_send_tuples {
            if let Some(target_addr_str) = all_raft_addrs.get(&target_peer_id) {
                let message_to_send = rpc_message.clone();
                let target_addr_owned = target_addr_str.clone();
                let server_logic_for_rpc_task = Arc::clone(&server_logic_arc);
                
                let ae_context_for_reply = if let RpcMessage::AppendEntries(ref args_rpc) = message_to_send {
                    Some(AppendEntriesContext {
                        prev_log_index_sent: args_rpc.prev_log_index,
                        entries_len_sent: args_rpc.entries.len(),
                    })
                } else { 
                    None 
                };

                tokio::spawn(async move {
                    let sender_id_for_this_rpc = server_logic_for_rpc_task.lock().await.id;
                    if DETAILED_LOGS {
                        println!(
                            "S{} RPC_SEND -> S{}: Attempting to send: {:?}",
                            sender_id_for_this_rpc, 
                            target_peer_id, 
                            message_to_send
                        );
                    }
                    
                    match TcpStream::connect(&target_addr_owned).await {
                        Ok(mut stream) => {
                            match bincode::serialize(&message_to_send) {
                                Ok(serialized_msg) => {
                                    let msg_len = serialized_msg.len() as u32;
                                    if stream.write_all(&msg_len.to_be_bytes()).await.is_err() ||
                                       stream.write_all(&serialized_msg).await.is_err() 
                                    {
                                        eprintln!(
                                            "S{} RPC_SEND_IO_ERR -> S{}: Write failed for {:?}",
                                            sender_id_for_this_rpc, 
                                            target_peer_id, 
                                            message_to_send
                                        );
                                        return;
                                    }
                                    
                                    let mut len_bytes = [0u8; 4];
                                    match stream.read_exact(&mut len_bytes).await {
                                        Ok(_) => {
                                            let reply_len = u32::from_be_bytes(len_bytes) as usize;
                                            if reply_len > 0 && reply_len < 1_048_576 {
                                                let mut reply_buffer = vec![0u8; reply_len];
                                                if stream.read_exact(&mut reply_buffer).await.is_ok() {
                                                    match bincode::deserialize::<RpcMessage>(&reply_buffer) {
                                                        Ok(reply_rpc) => {
                                                            if DETAILED_LOGS {
                                                                println!(
                                                                    "S{} RPC_RECV_REPLY <- S{}: Received reply: {:?}",
                                                                    sender_id_for_this_rpc, 
                                                                    target_peer_id, 
                                                                    reply_rpc
                                                                );
                                                            }
                                                            
                                                            let mut server_guard = 
                                                                server_logic_for_rpc_task.lock().await;
                                                            match reply_rpc {
                                                                RpcMessage::RequestVoteReply(rvr_args) => {
                                                                    server_guard.handle_request_vote_reply(
                                                                        rvr_args, 
                                                                        target_peer_id, 
                                                                        total_servers_in_cluster
                                                                    );
                                                                }
                                                                RpcMessage::AppendEntriesReply(aer_args) => {
                                                                    if let Some(context) = ae_context_for_reply {
                                                                        server_guard.handle_append_entries_reply(
                                                                            target_peer_id,
                                                                            aer_args, 
                                                                            context.prev_log_index_sent,
                                                                            context.entries_len_sent,
                                                                            total_servers_in_cluster
                                                                        );
                                                                    } else {
                                                                        eprintln!(
                                                                            "S{} RPC_REPLY_ERR <- S{}: \
                                                                            Missing AE context for AppendEntriesReply.",
                                                                            sender_id_for_this_rpc, 
                                                                            target_peer_id
                                                                        );
                                                                    }
                                                                }
                                                                _ => {
                                                                    eprintln!(
                                                                        "S{} RPC_UNEXPECTED_REPLY <- S{}: \
                                                                        Received unexpected RPC type in reply: {:?}",
                                                                        sender_id_for_this_rpc, 
                                                                        target_peer_id, 
                                                                        reply_rpc
                                                                    );
                                                                }
                                                            }
                                                        }
                                                        Err(e) => {
                                                            eprintln!(
                                                                "S{} RPC_REPLY_DESER_ERR <- S{}: \
                                                                Deserialize reply error: {:?}",
                                                                sender_id_for_this_rpc, 
                                                                target_peer_id, 
                                                                e
                                                            );
                                                        }
                                                    }
                                                } else {
                                                    eprintln!(
                                                        "S{} RPC_REPLY_READ_BODY_ERR <- S{}: \
                                                        Failed to read reply body.",
                                                        sender_id_for_this_rpc, 
                                                        target_peer_id
                                                    );
                                                }
                                            } else if reply_len != 0 {
                                                eprintln!(
                                                    "S{} RPC_REPLY_LEN_ERR <- S{}: \
                                                    Invalid reply length {} received.",
                                                    sender_id_for_this_rpc, 
                                                    target_peer_id, 
                                                    reply_len
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            if e.kind() != std::io::ErrorKind::UnexpectedEof {
                                                eprintln!(
                                                    "S{} RPC_REPLY_READ_LEN_ERR <- S{}: \
                                                    Failed to read reply length: {:?}",
                                                    sender_id_for_this_rpc, 
                                                    target_peer_id, 
                                                    e
                                                );
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!(
                                        "S{} RPC_SEND_SER_ERR -> S{}: \
                                        Serialize error for {:?}: {:?}",
                                        sender_id_for_this_rpc, 
                                        target_peer_id, 
                                        message_to_send, 
                                        e
                                    );
                                }
                            }
                            let _ = stream.shutdown().await;
                        }
                        Err(e) => {
                            eprintln!(
                                "S{} RPC_CONNECT_ERR -> S{}: \
                                Connect error to {}: {:?}",
                                sender_id_for_this_rpc, 
                                target_peer_id, 
                                target_addr_owned, 
                                e
                            );
                        }
                    }
                });
            } else {
                eprintln!(
                    "S{} PEER_ADDR_ERR: \
                    Could not find address for peer_id {}", 
                    current_server_id_for_log, 
                    target_peer_id
                );
            }
        }
    }
}

async fn handle_raft_connection(
    mut stream: TcpStream,
    server_id_context: u64,
    server_logic_arc: Arc<Mutex<Server>>,
    _total_servers: usize,
) {
    let peer_addr_str = stream.peer_addr().map_or_else(
        |_| "unknown peer".to_string(),
        |pa| pa.to_string()
    );

    loop {
        let mut len_bytes = [0u8; 4];
        match stream.read_exact(&mut len_bytes).await {
            Ok(_) => {
                let msg_len = u32::from_be_bytes(len_bytes) as usize;
                if msg_len == 0 {
                    if DETAILED_LOGS {
                        println!(
                            "S{} RAFT_RECV_EMPTY_MSG from {}: Skipping.",
                            server_id_context,
                            peer_addr_str
                        );
                    }
                    continue;
                }

                if msg_len > 1_048_576 {
                    eprintln!(
                        "S{} RAFT_MSG_LEN_ERR from {}: \
                         Message length {} too large. Closing.",
                        server_id_context,
                        peer_addr_str,
                        msg_len
                    );
                    return;
                }

                let mut msg_buffer = vec![0u8; msg_len];
                match stream.read_exact(&mut msg_buffer).await {
                    Ok(_) => {
                        match bincode::deserialize::<RpcMessage>(&msg_buffer) {
                            Ok(rpc_message) => {
                                if DETAILED_LOGS {
                                    println!(
                                        "S{} RAFT_RECV_MSG from {}: Received {:?})",
                                        server_id_context,
                                        peer_addr_str,
                                        rpc_message
                                    );
                                }

                                let reply_rpc_message: Option<RpcMessage> = {
                                    let mut server_guard = server_logic_arc.lock().await;
                                    match rpc_message {
                                        RpcMessage::RequestVote(args) => {
                                            Some(RpcMessage::RequestVoteReply(server_guard.handle_request_vote(args)))
                                        }
                                        RpcMessage::AppendEntries(args) => {
                                            Some(RpcMessage::AppendEntriesReply(server_guard.handle_append_entries(args)))
                                        }
                                        RpcMessage::InstallSnapshot(args) => {
                                            Some(RpcMessage::InstallSnapshotReply(
                                                server_guard.handle_install_snapshot(args)
                                            ))
                                        }
                                        _ => {
                                            eprintln!("S{} RAFT_UNEXPECTED_MSG_TYPE from {}: {:?}",
                                                server_id_context, peer_addr_str, rpc_message);
                                            None
                                        }
                                    }
                                };

                                if let Some(reply_to_send) = reply_rpc_message {
                                    if DETAILED_LOGS {
                                        println!(
                                            "S{} RAFT_SEND_REPLY to {}: Sending: {:?}",
                                            server_id_context,
                                            peer_addr_str,
                                            reply_to_send
                                        );
                                    }

                                    match bincode::serialize(&reply_to_send) {
                                        Ok(serialized_reply) => {
                                            let reply_len = serialized_reply.len() as u32;
                                            if stream.write_all(&reply_len.to_be_bytes()).await.is_err() ||
                                               stream.write_all(&serialized_reply).await.is_err()
                                            {
                                                eprintln!(
                                                    "S{} RAFT_REPLY_SEND_IO_ERR to {}: \
                                                     Write failed for {:?}",
                                                    server_id_context,
                                                    peer_addr_str,
                                                    reply_to_send
                                                );
                                                return;
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "S{} RAFT_REPLY_SER_ERR to {}: \
                                                 Failed to serialize reply {:?}: {:?}",
                                                server_id_context,
                                                peer_addr_str,
                                                reply_to_send,
                                                e
                                            );
                                        }
                                    }
                                } else {
                                    if DETAILED_LOGS {
                                        println!(
                                            "S{} RAFT_NO_REPLY_GENERATED for msg from {}",
                                            server_id_context, peer_addr_str
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "S{} RAFT_REQ_DESER_ERR from {}: \
                                     Failed to deserialize RPC: {:?}",
                                    server_id_context,
                                    peer_addr_str,
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "S{} RAFT_READ_BODY_ERR from {}: \
                             Failed to read body: {:?}",
                            server_id_context,
                            peer_addr_str,
                            e
                        );
                        return;
                    }
                }
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    println!(
                        "S{} RAFT_CONN_CLOSED by {}: Connection closed.",
                        server_id_context,
                        peer_addr_str
                    );
                } else {
                    eprintln!(
                        "S{} RAFT_READ_LEN_ERR from {}: \
                         Failed to read length prefix: {:?}",
                        server_id_context,
                        peer_addr_str,
                        e
                    );
                }
                return;
            }
        }
    }
}

async fn handle_client_connection(
    mut stream: TcpStream,
    server_id_context: u64,
    server_logic_arc: Arc<Mutex<Server>>,
    all_client_addrs: Arc<HashMap<u64, String>>,
) {
    let peer_addr_str = stream.peer_addr().map_or_else(
        |_| "unknown client".to_string(),
        |pa| pa.to_string()
    );

    println!(
        "S{} CLIENT_HANDLER_INIT for {}: New connection.",
        server_id_context,
        peer_addr_str
    );

    loop {
        let mut len_bytes = [0u8; 4];
        match stream.read_exact(&mut len_bytes).await {
            Ok(_) => {
                let msg_len = u32::from_be_bytes(len_bytes) as usize;
                if msg_len == 0 || msg_len > 1_048_576 {
                    eprintln!(
                        "S{} CLIENT_INVALID_MSG_LEN from {}: \
                         Invalid length {}, closing.",
                        server_id_context,
                        peer_addr_str,
                        msg_len
                    );
                    return;
                }

                let mut msg_buffer = vec![0u8; msg_len];
                if stream.read_exact(&mut msg_buffer).await.is_err() {
                    eprintln!(
                        "S{} CLIENT_READ_BODY_ERR from {}: Failed to read body.",
                        server_id_context,
                        peer_addr_str
                    );
                    return;
                }

                match bincode::deserialize::<ClientRequest>(&msg_buffer) {
                    Ok(client_request) => {
                        if DETAILED_LOGS {
                            println!(
                                "S{} CLIENT_REQ_RECV from {}: Deserialized: {:?}",
                                server_id_context,
                                peer_addr_str,
                                &client_request
                            );
                        }

                        let mut eventual_client_reply: Option<ClientReply> = None;
                        let mut opt_log_idx_for_cleanup: Option<u64> = None;

                        let (is_leader_for_this_request, known_leader_id_opt, current_term_for_log) = {
                            let server_guard = server_logic_arc.lock().await;
                            (
                                server_guard.state == NodeState::Leader,
                                server_guard.current_leader_id,
                                server_guard.current_term
                            )
                        };

                        if !is_leader_for_this_request {
                            match &client_request {
                                ClientRequest::Set { .. } | ClientRequest::Delete { .. } | ClientRequest::Get { .. } => {
                                    if let Some(leader_id) = known_leader_id_opt {
                                        if leader_id == server_id_context {
                                            println!(
                                                "S{} T{} CLIENT_REQ_REDIRECT_WARN from {}: Am not leader, but known_leader_id is self ({}). Sending leader unknown.",
                                                server_id_context, current_term_for_log, peer_addr_str, leader_id
                                            );
                                            eventual_client_reply = Some(ClientReply::Error {
                                                msg: "Server is not leader, and leader status is inconsistent. Please retry.".to_string(),
                                            });
                                        } else {
                                            let leader_client_addr_opt = all_client_addrs.get(&leader_id).cloned();
                                            println!(
                                                "S{} T{} CLIENT_REQ_REDIRECT from {}: Not leader. Known leader S{}. Addr: {:?}. Forwarding info for {:?}.",
                                                server_id_context, current_term_for_log, peer_addr_str, leader_id, leader_client_addr_opt, client_request
                                            );
                                            eventual_client_reply = Some(ClientReply::LeaderRedirect {
                                                leader_id,
                                                leader_addr: leader_client_addr_opt,
                                            });
                                        }
                                    } else {
                                        println!(
                                            "S{} T{} CLIENT_REQ_REDIRECT_FAIL from {}: Not leader and leader unknown. Cannot redirect for {:?}.",
                                            server_id_context, current_term_for_log, peer_addr_str, client_request
                                        );
                                        eventual_client_reply = Some(ClientReply::Error {
                                            msg: "Leader currently unknown. Please retry.".to_string(),
                                        });
                                    }
                                }
                            }
                        } else {
                            match client_request {
                                ClientRequest::Set { key, value } => {
                                    let (tx, rx) = oneshot::channel::<ClientReply>();
                                    {
                                        let mut server_guard = server_logic_arc.lock().await;
                                        if server_guard.state != NodeState::Leader {
                                            let _ = tx.send(ClientReply::Error { msg: "Not the leader (state changed)".to_string() });
                                        } else {
                                            println!(
                                                "S{} CLIENT_CMD_SET_RECV from {}: Leader received SET for key '{}'",
                                                server_id_context, peer_addr_str, key
                                            );
                                            let command = Command::Set { key: key.clone(), value };
                                            let log_entry = LogEntry { term: server_guard.current_term, command };
                                            server_guard.log.push(log_entry);
                                            let new_log_index_in_mem = server_guard.log.len() - 1;
                                            server_guard.persist_log_entry(new_log_index_in_mem);
                                            let new_log_index_raft = server_guard.log.len() as u64;
                                            opt_log_idx_for_cleanup = Some(new_log_index_raft);
                                            server_guard.pending_client_acks.insert(new_log_index_raft, tx);
                                            println!(
                                                "S{} CLIENT_CMD_SET_APPENDED_PERSISTED for {}: Appended and Persisted SET for key '{}' to log idx {}. Awaiting commit.",
                                                server_id_context, peer_addr_str, key, new_log_index_raft
                                            );
                                        }
                                    }

                                    if opt_log_idx_for_cleanup.is_some() {
                                        match tokio::time::timeout(Duration::from_secs(10), rx).await {
                                            Ok(Ok(committed_reply)) => {
                                                eventual_client_reply = Some(committed_reply);
                                                println!("S{} CLIENT_COMMIT_OK for log {}: SET committed for {}.", server_id_context, opt_log_idx_for_cleanup.unwrap_or(0), peer_addr_str);
                                            }
                                            Ok(Err(_)) => {
                                                eventual_client_reply = Some(ClientReply::Error { msg: "Command processing aborted (channel closed)".to_string() });
                                                eprintln!("S{} CLIENT_COMMIT_CHAN_CLOSED for log {}: Oneshot closed before ack for SET from {}.", server_id_context, opt_log_idx_for_cleanup.unwrap_or(0), peer_addr_str);
                                            }
                                            Err(_) => {
                                                eventual_client_reply = Some(ClientReply::Error { msg: "Command timed out".to_string() });
                                                if let Some(log_idx_to_clean) = opt_log_idx_for_cleanup {
                                                    let mut server_guard = server_logic_arc.lock().await;
                                                    server_guard.pending_client_acks.remove(&log_idx_to_clean);
                                                    eprintln!("S{} CLIENT_COMMIT_TIMEOUT for log {}: Timeout for SET from {}, cleaned pending ack.", server_id_context, log_idx_to_clean, peer_addr_str);
                                                }
                                            }
                                        }
                                    } else if eventual_client_reply.is_none() { // If tx.send was called due to not being leader
                                         match tokio::time::timeout(Duration::from_secs(1), rx).await {
                                            Ok(Ok(reply_from_non_leader_path)) => eventual_client_reply = Some(reply_from_non_leader_path),
                                            _ => eprintln!("S{} CLIENT_SET_NL_REPLY_ERR: Error getting non-leader reply for SET from {}", server_id_context, peer_addr_str),
                                        }
                                    }
                                }
                                ClientRequest::Delete { key } => {
                                    let (tx, rx) = oneshot::channel::<ClientReply>();
                                    {
                                        let mut server_guard = server_logic_arc.lock().await;
                                        if server_guard.state != NodeState::Leader {
                                            let _ = tx.send(ClientReply::Error { msg: "Not the leader (state changed)".to_string() });
                                        } else {
                                            println!(
                                                "S{} CLIENT_CMD_DELETE_RECV from {}: Leader received DELETE for key '{}'",
                                                server_id_context, peer_addr_str, key
                                            );
                                            let command = Command::Delete { key: key.clone() };
                                            let log_entry = LogEntry { term: server_guard.current_term, command };
                                            server_guard.log.push(log_entry);
                                            let new_log_index_in_mem = server_guard.log.len() - 1;
                                            server_guard.persist_log_entry(new_log_index_in_mem);
                                            let new_log_index_raft = server_guard.log.len() as u64;
                                            opt_log_idx_for_cleanup = Some(new_log_index_raft);
                                            server_guard.pending_client_acks.insert(new_log_index_raft, tx);
                                            println!(
                                                "S{} CLIENT_CMD_DELETE_APPENDED_PERSISTED for {}: Appended and Persisted DELETE for key '{}' to log idx {}. Awaiting commit.",
                                                server_id_context, peer_addr_str, key, new_log_index_raft
                                            );
                                        }
                                    }

                                    if opt_log_idx_for_cleanup.is_some() {
                                        match tokio::time::timeout(Duration::from_secs(10), rx).await {
                                            Ok(Ok(committed_reply)) => {
                                                eventual_client_reply = Some(committed_reply);
                                                println!("S{} CLIENT_COMMIT_OK for log {}: DELETE committed for {}.", server_id_context, opt_log_idx_for_cleanup.unwrap_or(0), peer_addr_str);
                                            }
                                            Ok(Err(_)) => {
                                                eventual_client_reply = Some(ClientReply::Error { msg: "Command processing aborted (channel closed)".to_string() });
                                                eprintln!("S{} CLIENT_COMMIT_CHAN_CLOSED for log {}: Oneshot closed before ack for DELETE from {}.", server_id_context, opt_log_idx_for_cleanup.unwrap_or(0), peer_addr_str);
                                            }
                                            Err(_) => {
                                                eventual_client_reply = Some(ClientReply::Error { msg: "Command timed out".to_string() });
                                                if let Some(log_idx_to_clean) = opt_log_idx_for_cleanup {
                                                    let mut server_guard = server_logic_arc.lock().await;
                                                    server_guard.pending_client_acks.remove(&log_idx_to_clean);
                                                    eprintln!("S{} CLIENT_COMMIT_TIMEOUT for log {}: Timeout for DELETE from {}, cleaned pending ack.", server_id_context, log_idx_to_clean, peer_addr_str);
                                                }
                                            }
                                        }
                                    } else if eventual_client_reply.is_none() {
                                        match tokio::time::timeout(Duration::from_secs(1), rx).await {
                                            Ok(Ok(reply_from_non_leader_path)) => eventual_client_reply = Some(reply_from_non_leader_path),
                                            _ => eprintln!("S{} CLIENT_DEL_NL_REPLY_ERR: Error getting non-leader reply for DEL from {}", server_id_context, peer_addr_str),
                                        }
                                    }
                                }
                                ClientRequest::Get { key } => {
                                    let (tx_reply_channel, rx_reply_channel) = oneshot::channel::<ClientReply>();
                                    let mut registered_by_leader = false;
                                    {
                                        let mut server_guard = server_logic_arc.lock().await;
                                        if server_guard.state == NodeState::Leader {
                                            server_guard.register_linearizable_read(key.clone(), tx_reply_channel);
                                            registered_by_leader = true;
                                        } else {
                                            let _ = tx_reply_channel.send(ClientReply::Error { msg: "Not the leader (state changed)".to_string() });
                                        }
                                    }

                                    if registered_by_leader {
                                         println!(
                                            "S{} CLIENT_CMD_GET_AWAIT from {}: Leader registered GET for key '{}', awaiting linearizable reply.",
                                            server_id_context, peer_addr_str, key
                                        );
                                    }

                                    match tokio::time::timeout(Duration::from_secs(10), rx_reply_channel).await {
                                        Ok(Ok(committed_reply)) => {
                                            eventual_client_reply = Some(committed_reply);
                                            if registered_by_leader {
                                                println!(
                                                    "S{} CLIENT_GET_REPLY_OK from {}: Received linearizable read result for key '{}'.",
                                                    server_id_context, peer_addr_str, key
                                                );
                                            }
                                        }
                                        Ok(Err(_)) => {
                                            eventual_client_reply = Some(ClientReply::Error { msg: "Read operation aborted on server".to_string() });
                                            eprintln!(
                                                "S{} CLIENT_GET_REPLY_ABORTED from {}: Reply channel closed for key '{}'.",
                                                server_id_context, peer_addr_str, key
                                            );
                                        }
                                        Err(_timeout_err) => {
                                            eventual_client_reply = Some(ClientReply::Error { msg: "Read operation timed out".to_string() });
                                            eprintln!(
                                                "S{} CLIENT_GET_TIMEOUT from {}: Timed out waiting for linearizable read reply for key '{}'.",
                                                server_id_context, peer_addr_str, key
                                            );
                                        }
                                    }
                                }
                            }
                        }

                        if let Some(reply) = eventual_client_reply {
                            match bincode::serialize(&reply) {
                                Ok(serialized_reply) => {
                                    let len_bytes_reply = (serialized_reply.len() as u32).to_be_bytes();
                                    if stream.write_all(&len_bytes_reply).await.is_err() ||
                                       stream.write_all(&serialized_reply).await.is_err()
                                    {
                                        eprintln!(
                                            "S{} CLIENT_REPLY_SEND_IO_ERR to {}: Send reply failed.",
                                            server_id_context, peer_addr_str
                                        );
                                        return;
                                    }
                                    if DETAILED_LOGS {
                                        println!(
                                            "S{} CLIENT_REPLY_SENT to {}: Reply: {:?}",
                                            server_id_context, peer_addr_str, reply
                                        );
                                    }
                                }
                                Err(e) => {
                                    eprintln!(
                                        "S{} CLIENT_REPLY_SER_ERR: Failed to serialize client reply {:?}: {:?}",
                                        server_id_context, reply, e
                                    );
                                }
                            }
                        } else {
                            eprintln!(
                                "S{} CLIENT_NO_REPLY_INTERNAL_ERR for {}: No reply was prepared.",
                                server_id_context, peer_addr_str
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "S{} CLIENT_REQ_DESER_ERR from {}: Failed to deserialize request: {:?}",
                            server_id_context, peer_addr_str, e
                        );
                    }
                }
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    println!(
                        "S{} CLIENT_CONN_CLOSED by {}: Connection closed.",
                        server_id_context, peer_addr_str
                    );
                } else {
                    eprintln!(
                        "S{} CLIENT_READ_LEN_ERR from {}: Failed to read length prefix: {:?}",
                        server_id_context, peer_addr_str, e
                    );
                }
                return;
            }
        }
    }
}

async fn send_test_client_command(initial_target_client_addr: &str, request: ClientRequest) {
    let mut current_target_addr = initial_target_client_addr.to_string();
    let max_redirects = 3; 

    for attempt in 0..=max_redirects {
        if attempt > 0 { 
            println!(
                "[TestClient] REDIRECT_ATTEMPT #{}: Retrying command to new target {}",
                attempt, current_target_addr
            );
        }

        println!(
            "[TestClient] CONNECT_ATTEMPT: Attempting connection to {} for request: {:?}",
            current_target_addr, request
        );

        match TcpStream::connect(&current_target_addr).await {
            Ok(mut stream) => {
                println!("[TestClient] CONNECTED: To {}", current_target_addr);
                println!("[TestClient] SEND_CMD: Sending: {:?}", request);

                let serialized_req = match bincode::serialize(&request) {
                    Ok(sr) => sr,
                    Err(e) => {
                        eprintln!("[TestClient] SER_REQ_ERR: Serialize request error: {:?}", e);
                        return;
                    }
                };

                let len_bytes_req = (serialized_req.len() as u32).to_be_bytes();
                if stream.write_all(&len_bytes_req).await.is_err() ||
                   stream.write_all(&serialized_req).await.is_err() {
                    eprintln!("[TestClient] SEND_IO_ERR to {}: Write failed.", current_target_addr);
                    if attempt == max_redirects {
                        eprintln!("[TestClient] SEND_IO_ERR: Max attempts reached, failing command.");
                        return;
                    }
                    continue; 
                }

                let mut len_bytes_reply = [0u8; 4];
                match stream.read_exact(&mut len_bytes_reply).await {
                    Ok(_) => {
                        let reply_len = u32::from_be_bytes(len_bytes_reply) as usize;
                        if reply_len > 0 && reply_len < 1_048_576 { // Max 1MB reply
                            let mut reply_buffer = vec![0u8; reply_len];
                            if stream.read_exact(&mut reply_buffer).await.is_ok() {
                                match bincode::deserialize::<ClientReply>(&reply_buffer) {
                                    Ok(reply) => {
                                        println!("[TestClient] RECV_REPLY_OK: Received: {:?}", reply);

                                        match reply {
                                            ClientReply::LeaderRedirect { leader_id: _, leader_addr: Some(new_addr) } => {
                                                if attempt < max_redirects {
                                                    println!("[TestClient] REDIRECT_RECV: Redirecting to {}", new_addr);
                                                    current_target_addr = new_addr;
                                                    let _ = stream.shutdown().await;
                                                    continue; 
                                                } else {
                                                    eprintln!("[TestClient] REDIRECT_FAIL: Max redirects ({}) reached. Last known redirect address: {}", max_redirects, new_addr);
                                                    return; 
                                                }
                                            }
                                            ClientReply::LeaderRedirect { leader_id, leader_addr: None } => {
                                                eprintln!("[TestClient] REDIRECT_FAIL: Server indicated redirect but provided no new address (Leader ID: {}). Giving up.", leader_id);
                                                return; 
                                            }
                                            _ => {
                                                return;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("[TestClient] DESER_REPLY_ERR: Deserialize reply error from {}: {:?}", current_target_addr, e);
                                        return; 
                                    }
                                }
                            } else {
                                eprintln!("[TestClient] READ_REPLY_BODY_ERR from {}: Failed to read reply body.", current_target_addr);
                                return;
                            }
                        } else if reply_len == 0 {
                            println!("[TestClient] EMPTY_REPLY_RECV from {}: Server sent an empty reply.", current_target_addr);
                            return; 
                        }else {
                             eprintln!("[TestClient] INVALID_REPLY_LEN: Invalid reply length {} from {}", reply_len, current_target_addr);
                            return; 
                        }
                    }
                    Err(e) => {
                        eprintln!("[TestClient] READ_REPLY_LEN_ERR from {}: Failed to read reply length: {:?}", current_target_addr, e);

                        return;
                    }
                }
            }
            Err(e) => {
                eprintln!("[TestClient] CONNECT_FAIL to {}: {:?}", current_target_addr, e);
                if attempt == max_redirects {
                    eprintln!("[TestClient] CONNECT_FAIL: Max attempts reached after connection failures.");
                    return; 
                }
            }
        }
        println!("[TestClient] Attempt {} for request to {} concluded.", attempt + 1, current_target_addr);
    } 
    eprintln!("[TestClient] FAILED_CMD: Command {:?} failed after {} attempts. Last target: {}", request, max_redirects +1, current_target_addr);
}