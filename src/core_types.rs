use std::collections::HashMap;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub enum Command {
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, Clone)]
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

    pub fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        println!(
            "[Server {} Term {} State {:?}] Received AppendEntries from Leader {} in Term {}",
            self.id, self.current_term, self.state, args.leader_id, args.term
        );

        // sender is outdated leader
        if args.term < self.current_term {
            println!(
                "[Server {}] Rejecting AppendEntries: Leader's term {} is old (our term is {})",
                self.id, args.term, self.current_term
            );
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
            };
        }

        // if newer/equal term, candidate/old leader becomes follower
        if args.term > self.current_term {
            println!(
                "[Server {}] New term {} from leader {}. Updating term and becoming Follower.",
                self.id, args.term, args.leader_id
            );
            self.current_term = args.term;
            self.state = NodeState::Follower;
            self.voted_for = None;
        } else if self.state == NodeState::Candidate {
            println!(
                "[Server {}] Candidate in term {} received valid AppendEntries from Leader {}. Becoming Follower.",
                self.id, self.current_term, args.leader_id
            );
            self.state = NodeState::Follower;
        }

        // reply false if log has no prev_log_index that match prev_log_term
        if args.prev_log_index > 0 {
            // if 0 then log is empty/leader send from beginning
            let vec_prev_log_index = (args.prev_log_index - 1) as usize;

            if vec_prev_log_index >= self.log.len() {
                println!(
                    "[Server {}] Rejecting AppendEntries: Log doesn't have entry at prev_log_index {} (our log len is {}). Mismatch.",
                    self.id,
                    args.prev_log_index,
                    self.log.len()
                );
                return AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                };
            }

            if self.log[vec_prev_log_index].term != args.prev_log_term {
                println!(
                    "[Server {}] Rejecting AppendEntries: Term mismatch at prev_log_index {}. Expected term {}, got {}. Mismatch.",
                    self.id,
                    args.prev_log_index,
                    args.prev_log_term,
                    self.log[vec_prev_log_index].term
                );
                return AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                };
            }
        }

        // if existing entry conflict with new one, delete and all that follow it, also append new ones
        let mut current_log_index = args.prev_log_index;
        for new_entry in args.entries.iter() {
            current_log_index += 1;
            let vec_current_log_index = (current_log_index - 1) as usize;

            if vec_current_log_index < self.log.len() {
                if self.log[vec_current_log_index].term != new_entry.term {
                    println!(
                        "[Server {}] Conflict at index {}: existing term {}, new term {}. Truncating log.",
                        self.id,
                        current_log_index,
                        self.log[vec_current_log_index].term,
                        new_entry.term
                    );
                    self.log.truncate(vec_current_log_index);
                    self.log.push(new_entry.clone());
                }
            } else {
                self.log.push(new_entry.clone());
            }
        }

        println!(
            "[Server {}] Log after AppendEntries: {:?}",
            self.id, self.log
        );

        // set commit_index = min(leader_commit, index of last new entry)
        if args.leader_commit > self.commit_index {
            let last_entry_index_in_our_log = self.log.len() as u64;

            self.commit_index = std::cmp::min(args.leader_commit, last_entry_index_in_our_log);
            println!(
                "[Server {}] Updated commit_index to {}",
                self.id, self.commit_index
            );
        }

        AppendEntriesReply {
            term: self.current_term,
            success: true,
        }
    }

    pub fn apply_committed_entries(&mut self) {
        // Corrected spelling
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let vec_index_to_apply = (self.last_applied - 1) as usize;

            if vec_index_to_apply < self.log.len() {
                let command_to_apply = self.log[vec_index_to_apply].command.clone();

                println!(
                    "[Server {} Term {}] Applying log index {} to KV store: {:?}",
                    self.id, self.current_term, self.last_applied, command_to_apply
                );

                match command_to_apply {
                    Command::Set { key, value } => {
                        self.kv_store.insert(key, value); // No semicolon needed here if it's the last expr in block
                    }
                    Command::Delete { key } => {
                        self.kv_store.remove(&key); // remove takes &String, key is owned String here
                    }
                }
            } else {
                eprintln!(
                    "[Server {}] CRITICAL ERROR: Trying to apply log index {} (vec index {}) but log length is {}. Halting application.",
                    self.id,
                    self.last_applied,
                    vec_index_to_apply,
                    self.log.len()
                );
                self.last_applied -= 1; // Revert the increment because we couldn't apply
                break;
            }
        }

        if self.last_applied > 0 {
            println!(
                "[Server {}] KV Store after applying entries up to index {}: {:?}",
                self.id, self.last_applied, self.kv_store
            );
        }
    }
}

#[derive(Debug, Clone)]
pub struct RequestVoteArgs {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone)]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
}

#[derive(Debug, Clone)]
pub enum RpcMessage {
    RequestVote(RequestVoteArgs),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntriesArgs),
    AppendEntriesReply(AppendEntriesReply),
}
