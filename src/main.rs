mod core_types;

use crate::core_types::{RpcMessage, Server, AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};
use std::thread;
use std::time::Duration;

fn main() {
    println!("--- Simplified Raft KV Store Simulation ---");

    let peer_ids: Vec<u64> = vec![1, 2, 3];
    let mut servers: Vec<Server> = peer_ids.iter().map(|&id| Server::new(id)).collect();

    for tick_num in 0..100 {
        println!("\n--- TICK {} ---", tick_num);
        
        let mut all_messages_this_tick: Vec<(u64, u64, RpcMessage)> = Vec::new();

        for server in servers.iter_mut() {
            let other_peer_ids: Vec<u64> = peer_ids.iter().cloned().filter(|&id| id != server.id).collect();
            let messages = server.tick(&other_peer_ids);
            for (target_id, rpc_message) in messages {
                all_messages_this_tick.push((server.id, target_id, rpc_message));
            }
        }

        if !all_messages_this_tick.is_empty() {
            println!("--- Delivering {} messages this tick ---", all_messages_this_tick.len());
        }
        for (sender_id, receiver_id, rpc_message) in all_messages_this_tick {
            if let Some(receiver_server) = servers.iter_mut().find(|s| s.id == receiver_id) {
                println!("[Network] Delivering message from S{} to S{}: {:?}", sender_id, receiver_id, rpc_message);
                match rpc_message {
                    RpcMessage::RequestVote(args) => {
                        let reply = receiver_server.handle_request_vote(args);
                        println!("[Server {}] Sent RequestVoteReply: {:?}", receiver_server.id, reply);
                    }
                    RpcMessage::AppendEntries(args) => {
                        let reply = receiver_server.handle_append_entries(args);
                        println!("[Server {}] Sent AppendEntriesReply: {:?}", receiver_server.id, reply);
                    }
                    RpcMessage::RequestVoteReply(reply_args) => {
                        println!("[Network] S{} received RequestVoteReply (handler TBD): {:?}", receiver_id, reply_args);
                    }
                    RpcMessage::AppendEntriesReply(reply_args) => {
                        println!("[Network] S{} received AppendEntriesReply (handler TBD): {:?}", receiver_id, reply_args);
                    }
                }
            }
        }

        for server in &servers {
            print_server_details_brief(server);
        }

        thread::sleep(Duration::from_millis(50));
    }
}

fn print_server_details_brief(server: &Server) {
    println!(
        "S{}: Term={}, State={:?}, LogLen={}, Commit={}, Applied={}, VotedFor={:?}",
        server.id,
        server.current_term,
        server.state,
        server.log.len(),
        server.commit_index,
        server.last_applied,
        server.voted_for
    );
}

fn print_server_details(label: &str, server: &Server) { 
    println!(
        "{}: ID={}, Term={}, State={:?}, VotedFor={:?}, LogLen={}, CommitIdx={}, AppliedIdx={}",
        label,
        server.id,
        server.current_term,
        server.state,
        server.voted_for,
        server.log.len(),
        server.commit_index,
        server.last_applied
    );
}