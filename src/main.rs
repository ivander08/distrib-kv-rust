// src/main.rs
mod core_types;

use crate::core_types::{
    Command, LogEntry, NodeState, RpcMessage, Server, AppendEntriesArgs, AppendEntriesReply,
    RequestVoteArgs, RequestVoteReply, // Added to resolve some unused import warnings potentially
};
use std::thread;
use std::time::Duration;

fn main() {
    println!("--- Simplified Raft KV Store Simulation ---");

    let all_server_ids: Vec<u64> = vec![1, 2, 3];
    let total_servers = all_server_ids.len();

    let mut servers: Vec<Server> = all_server_ids
        .iter()
        .map(|&id| {
            let peer_ids_for_this_server: Vec<u64> = all_server_ids
                .iter()
                .cloned()
                .filter(|&p_id| p_id != id)
                .collect();
            Server::new(id, peer_ids_for_this_server)
        })
        .collect();

    if let Some(leader_candidate) = servers.iter_mut().find(|s| s.id == 1) {
        leader_candidate.current_term = 1;
        leader_candidate.state = NodeState::Leader;
        leader_candidate.votes_received.insert(1);
        for &peer_id in &leader_candidate.peer_ids {
            if peer_id != leader_candidate.id { // Ensure peer_id is used correctly
                leader_candidate.next_index.insert(peer_id, (leader_candidate.log.len() + 1) as u64);
                leader_candidate.match_index.insert(peer_id, 0);
            }
        }
        println!("[Sim] Server 1 becomes Leader in Term 1 (simulated for test).");
        
        let client_command1 = Command::Set { key: "hello".to_string(), value: "world".to_string() };
        leader_candidate.log.push(LogEntry { term: 1, command: client_command1 });
        println!("[SimClient -> S1] Sent SET hello=world. S1 log: {:?}", leader_candidate.log);
        
        let client_command2 = Command::Set { key: "foo".to_string(), value: "bar".to_string() };
        leader_candidate.log.push(LogEntry { term: 1, command: client_command2 });
        println!("[SimClient -> S1] Sent SET foo=bar. S1 log: {:?}", leader_candidate.log);
    }

    for tick_num in 0..100 {
        println!("\n--- TICK {} ---", tick_num);
        
        let mut current_requests: Vec<(u64, u64, RpcMessage)> = Vec::new();
        let mut current_replies: Vec<(u64, u64, RpcMessage)> = Vec::new();
        let mut next_round_requests: Vec<(u64, u64, RpcMessage)> = Vec::new();

        // 1. Servers generate initial requests from their tick logic
        for server in servers.iter_mut() {
            let outgoing_rpcs = server.tick(); 
            for (target_peer_id, rpc_message) in outgoing_rpcs {
                current_requests.push((server.id, target_peer_id, rpc_message.clone()));
            }
        }
        
        // Loop to process requests and replies until stable (or max iterations per tick)
        // This is a simplified way to handle new messages generated within the same tick
        let mut processing_iterations = 0;
        while !current_requests.is_empty() || !current_replies.is_empty() {
            if processing_iterations > 5 { // Safety break for complex cascades in one tick
                println!("[SimWarning] Exceeded max processing iterations for tick {}", tick_num);
                break;
            }
            processing_iterations += 1;

            // 2. "Deliver" current requests and collect replies
            let requests_to_process = std::mem::take(&mut current_requests); // Take ownership
            if !requests_to_process.is_empty() {
                println!("--- Tick {}.{}: Delivering {} requests ---", tick_num, processing_iterations, requests_to_process.len());
            }
            for (sender_id, receiver_id, rpc_message) in requests_to_process { // Iterates over the taken vec
                if let Some(receiver_server) = servers.iter_mut().find(|s| s.id == receiver_id) {
                    println!("[SimNetwork] S{} -> S{}: {:?}", sender_id, receiver_id, &rpc_message);
                    match rpc_message {
                        RpcMessage::RequestVote(args) => {
                            let reply = receiver_server.handle_request_vote(args);
                            current_replies.push((sender_id, receiver_server.id, RpcMessage::RequestVoteReply(reply)));
                        }
                        RpcMessage::AppendEntries(args) => {
                            let reply = receiver_server.handle_append_entries(args);
                            current_replies.push((sender_id, receiver_server.id, RpcMessage::AppendEntriesReply(reply)));
                        }
                        _ => {} 
                    }
                }
            }
            
            // 3. "Deliver" current replies back to the original senders
            let replies_to_process = std::mem::take(&mut current_replies); // Take ownership
            if !replies_to_process.is_empty() {
                println!("--- Tick {}.{}: Delivering {} replies ---", tick_num, processing_iterations, replies_to_process.len());
            }
            for (original_requester_id, replier_id, rpc_reply_message) in replies_to_process {
                 if let Some(original_requester_server) = servers.iter_mut().find(|s| s.id == original_requester_id) {
                    println!("[SimNetworkReply] S{} -> S{}: {:?}", replier_id, original_requester_id, &rpc_reply_message);
                    match rpc_reply_message {
                        RpcMessage::RequestVoteReply(reply_args) => {
                            if let Some(msgs_if_leader) = original_requester_server.handle_request_vote_reply(reply_args, replier_id, total_servers) {
                                for (target_peer_id, rpc_m) in msgs_if_leader {
                                    // New requests generated by becoming leader
                                    next_round_requests.push((original_requester_server.id, target_peer_id, rpc_m));
                                    println!("[S{}] Became LEADER, queueing initial heartbeat to S{}", original_requester_server.id, target_peer_id);
                                }
                            }
                        }
                        RpcMessage::AppendEntriesReply(reply_args) => {
                             original_requester_server.handle_append_entries_reply(replier_id, reply_args, total_servers);
                        }
                        _ => {} 
                    }
                }
            }
            // Add newly generated requests to be processed in the next iteration of this inner while loop
            current_requests.extend(std::mem::take(&mut next_round_requests));
        } // End of while processing_iterations

        println!("--- Server States after Tick {} processing ---", tick_num);
        for server in &servers { // Changed to iterate by reference
            print_server_details_brief(server);
        }

        thread::sleep(Duration::from_millis(200)); 
    }
}

fn print_server_details_brief(server: &Server) {
    println!(
        "S{}: Term={}, State={:?}, LogLen={}, Commit={}, Applied={}, VotedFor={:?}, VotesGot={}, KVStoreLen={}",
        server.id,
        server.current_term,
        server.state,
        server.log.len(),
        server.commit_index,
        server.last_applied,
        server.voted_for,
        server.votes_received.len(),
        server.kv_store.len()
    );
}