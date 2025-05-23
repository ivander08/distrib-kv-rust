use std::collections::HashMap;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub enum Command {
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug)]
pub struct LogEntry {
    pub term: u64,
    pub command: Command,
}

#[derive(Debug)]
pub struct Server {
    pub id: u64,
    pub state: NodeState,
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub next_index: HashMap<u64, u64>,
    pub match_index: HashMap<u64, u64>,
    pub kv_store: HashMap<String, String>,
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