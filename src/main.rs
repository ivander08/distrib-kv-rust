use std::collections::HashMap;

#[derive(Debug)]
enum Command {
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug)]
struct LogEntry {
    term: u64,
    command: Command,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub struct Server {
    id: u64,
    state: NodeState,
    current_term: u64,
    voted_for: Option<u64>,
    log: Vec<LogEntry>,

    commit_index: u64,
    last_applied: u64,

    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,

    kv_store: HashMap<String, String>,
}

impl Server {
    pub fn new(id: u64) -> Self {
        Server {
            id,
            state: NodeState::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            kv_store: HashMap::new(),
        }
    }
}

fn main() {
    println!("Defining our first data structures for the KV store...\n");

    let entry1 = LogEntry {
        term: 1,
        command: Command::Set {
            key: String::from("my_first_key"),
            value: String::from("Hello, Rust KV Store!"),
        },
    };

    let entry2 = LogEntry {
        term: 1,
        command: Command::Delete {
            key: String::from("some_other_key"),
        },
    };

    println!("Log Entry 1: {:?}", entry1);
    println!("Log Entry 2: {:?}", entry2);
    println!("\n--- Simulating a simple Key-Value Store ---");

    let mut kv_store: HashMap<String, String> = HashMap::new();

    if let Command::Set { key, value } = entry1.command {
        println!("Applying Set: key='{}', value = '{}'", key, value);
        kv_store.insert(key, value);
    }

    println!("KV Store after Set: {:?}", kv_store);

    let key_to_get = String::from("my_first_key");
    match kv_store.get(&key_to_get) {
        Some(value) => println!("Got value for key '{}': '{}'", key_to_get, value),
        None => println!("Key '{}' not found.", key_to_get),
    }

    let entry3_command = Command::Set {
        key: String::from("another_key"),
        value: String::from("another value"),
    };

    if let Command::Set { key, value } = entry3_command {
        println!("Applying Set: key='{}', value = '{}'", key, value);
        kv_store.insert(key, value);
    }

    println!("KV Store after another Set: {:?}", kv_store);

    let key_to_delete = String::from("my_first_key");
    kv_store.remove(&key_to_delete);
    println!("KV Store after deleting '{}': {:?}", key_to_delete, kv_store);

    println!("\n--- Server/Node Initialization ---");
    let server1 = Server::new(1);
    let server2 = Server::new(2);

    println!("Server 1 initial state: {:?}", server1);
    println!("Server 2 initial state: {:?}", server2);


}

