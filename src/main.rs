mod core_types;
use crate::core_types::{Server, NodeState, Command, LogEntry};
use std::collections::HashMap;

fn main() {
    println!("\n--- Server/Node Initialization (using modules) ---");
    let server1 = Server::new(1);
    let server2 = Server::new(2);

    println!("Server 1 initial state: {:?}", server1);
    println!("Server 2 initial state: {:?}", server2);

    println!("Server 1 ID: {}", server1.id);
    println!("Server 1 initial term: {}", server1.current_term);

    let an_entry = LogEntry {
        term: server1.current_term + 1,
        command: Command::Set {
            key: String::from("module_key"),
            value: String::from("module_value"),
        },
    };
    println!("A sample log entry from module: {:?}", an_entry);
}