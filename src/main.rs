mod core_types;
use crate::core_types::{Server, NodeState, Command, LogEntry, RequestVoteArgs, RequestVoteReply, AppendEntriesArgs, AppendEntriesReply};
use std::collections::HashMap;

fn main() {
    println!("\n--- Server/Node Initialization (using modules) ---");
    let server1 = Server::new(1);
    let server2 = Server::new(2);

    println!("Server 1 initial state: {:?}", server1);
    println!("Server 2 initial state: {:?}", server2);

    let vote_request_args = RequestVoteArgs {
        term: server1.current_term + 1, 
        candidate_id: server1.id,
        last_log_index: server1.log.len() as u64, 
        last_log_term: server1.log.last().map_or(0, |entry| entry.term), 
    };
    println!("\nSample Vote Request Data: {:?}", vote_request_args);

    println!("\nNext steps will involve implementing server logic to handle these messages!");
}