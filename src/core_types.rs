use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq, Clone, Copy, Serialize)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub command: Command,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    // reply to da leaders append/heartbeat
    pub term: u64,
    pub success: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestVoteReply {
    // reply to candidate's vote request
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    // sent by candidate to request votes
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesArgs {
    // sent by leader to replicate log/heartbeat
    pub term: u64, // da leaders term
    pub leader_id: u64,
    pub prev_log_index: u64, // index of log entry immediately preceding new ones
    pub prev_log_term: u64,  // the term of the prev_log_index
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcMessage {
    RequestVote(RequestVoteArgs),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntriesArgs),
    AppendEntriesReply(AppendEntriesReply),
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
    pub next_index: HashMap<u64, u64>, // for leader: next log index to send to each peer
    pub match_index: HashMap<u64, u64>, // for leader: highest log index known replicated on each peer
    pub kv_store: HashMap<String, String>,
    pub election_timeout_due: Instant,
    pub heartbeat_interval: Duration,
    pub election_timeout_base: Duration, // base for randomized election timeout
    pub peer_ids: Vec<u64>,
    pub votes_received: HashSet<u64>,
}

impl Server {
    pub fn new(id: u64, peer_ids: Vec<u64>) -> Self {
        let election_timeout_base = Duration::from_millis(150);
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
            election_timeout_due: Instant::now()
                + Self::randomized_election_timeout(election_timeout_base),
            heartbeat_interval: Duration::from_millis(50),
            election_timeout_base,
            peer_ids,
            votes_received: HashSet::new(),
        }
    }

    fn randomized_election_timeout(base_duration: Duration) -> Duration {
        base_duration + Duration::from_millis(base_duration.as_millis() as u64 / 2)
    }

    pub fn reset_election_timer(&mut self) {
        // call when hearing from leader or starting election
        self.election_timeout_due =
            Instant::now() + Self::randomized_election_timeout(self.election_timeout_base);
        println!(
            "[Server {} Term {}] Election timer reset. Due at: {:?}",
            self.id, self.current_term, self.election_timeout_due
        );
    }

    pub fn tick(&mut self) -> Vec<(u64, RpcMessage)> {
        let mut messages_to_send: Vec<(u64, RpcMessage)> = Vec::new();
        let now = Instant::now();

        match self.state {
            NodeState::Follower => {
                if now >= self.election_timeout_due {
                    // didn't hear from leader
                    println!(
                        "[Server {} Term {}] Election timeout! Becoming Candidate.",
                        self.id, self.current_term
                    );
                    self.state = NodeState::Candidate;
                }
            }
            NodeState::Candidate => {
                if now >= self.election_timeout_due || self.state == NodeState::Candidate {
                    println!(
                        "[Server {}] Candidate starting/restarting election for Term {}",
                        self.id,
                        self.current_term + 1
                    );
                    self.current_term += 1;
                    self.state = NodeState::Candidate;
                    self.voted_for = Some(self.id);
                    self.votes_received.clear();
                    self.votes_received.insert(self.id);
                    self.reset_election_timer();

                    for &peer_id in &self.peer_ids {
                        if peer_id != self.id {
                            let last_log_index = self.log.len() as u64;
                            let last_log_term = self.log.last().map_or(0, |entry| entry.term);

                            let args = RequestVoteArgs {
                                term: self.current_term,
                                candidate_id: self.id,
                                last_log_index,
                                last_log_term,
                            };
                            messages_to_send.push((peer_id, RpcMessage::RequestVote(args)));
                        }
                    }
                }
            }
            NodeState::Leader => {
                println!(
                    "[Server {} Term {}] Leader tick: Preparing AppendEntries.",
                    self.id, self.current_term
                );
                let mut leader_messages_this_tick = Vec::new();
                for &peer_id in &self.peer_ids {
                    if peer_id != self.id {
                        let next_log_idx_to_send = *self // what's the next entry this follower needs?
                            .next_index
                            .get(&peer_id)
                            .unwrap_or(&((self.log.len() + 1) as u64)); // default is after de last log entry

                        let prev_log_idx = next_log_idx_to_send.saturating_sub(1);
                        let prev_log_term = if prev_log_idx > 0 {
                            let vec_prev_idx = (prev_log_idx - 1) as usize;
                            if vec_prev_idx < self.log.len() {
                                self.log[vec_prev_idx].term
                            } else {
                                0
                            }
                        } else {
                            0
                        };

                        let mut entries_to_send: Vec<LogEntry> = Vec::new();

                        if next_log_idx_to_send <= (self.log.len() as u64) && !self.log.is_empty() {
                            let start_vec_idx = (next_log_idx_to_send - 1) as usize;
                            if start_vec_idx < self.log.len() {
                                entries_to_send = self.log[start_vec_idx..].to_vec(); // clone entries from log
                                println!(
                                    "[Server {} -> S{}] Sending {} entries starting from log index {}",
                                    self.id,
                                    peer_id,
                                    entries_to_send.len(),
                                    next_log_idx_to_send
                                );
                            } else {
                                println!(
                                    "[Server {} -> S{}] Sending HEARTBEAT (next_idx={} out of bounds for log_len={})",
                                    self.id,
                                    peer_id,
                                    next_log_idx_to_send,
                                    self.log.len()
                                );
                            }
                        } else {
                            println!(
                                "[Server {} -> S{}] Sending HEARTBEAT (no new entries, next_idx={})",
                                self.id, peer_id, next_log_idx_to_send
                            );
                        }

                        let args = AppendEntriesArgs {
                            term: self.current_term,
                            leader_id: self.id,
                            prev_log_index: prev_log_idx,
                            prev_log_term,
                            entries: entries_to_send,
                            leader_commit: self.commit_index,
                        };
                        leader_messages_this_tick.push((peer_id, RpcMessage::AppendEntries(args)));
                    }
                }
                messages_to_send.extend(leader_messages_this_tick);
            }
        }

        let old_last_applied = self.last_applied;
        if self.commit_index > self.last_applied {
            // if new entries committed apply to kv store
            self.apply_committed_entries();
            if self.last_applied > old_last_applied {
                println!(
                    "[Server {}] Applied {} entries. KV store: {:?}",
                    self.id,
                    self.last_applied - old_last_applied,
                    self.kv_store
                );
            }
        }

        messages_to_send
    }

    pub fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        println!(
            "[Server {} Term {} State {:?}] Received AppendEntries from Leader {} in Term {}",
            self.id, self.current_term, self.state, args.leader_id, args.term
        );

        if args.term < self.current_term {
            // leader is outdated
            println!(
                "[Server {}] Rejecting AppendEntries: Leader's term {} is old (our term is {})",
                self.id, args.term, self.current_term
            );
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
            };
        }

        self.reset_election_timer();

        if args.term > self.current_term {
            // new leader/term
            println!(
                "[Server {}] New term {} from leader {}. Updating term and becoming Follower.",
                self.id, args.term, args.leader_id
            );
            self.current_term = args.term;
            self.state = NodeState::Follower;
            self.voted_for = None;
        } else if self.state == NodeState::Candidate && args.term == self.current_term {
            println!(
                "[Server {}] Candidate in term {} received valid AppendEntries from Leader {}. Becoming Follower.",
                self.id, self.current_term, args.leader_id
            );
            self.state = NodeState::Follower;
        }

        if args.prev_log_index > 0 {
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
                // Corrected: removed extra parenthesis
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

        for (entry_offset, new_entry) in args.entries.iter().enumerate() {
            // append entries / handle conflicts
            let log_idx_for_this_entry = args.prev_log_index + 1 + entry_offset as u64;
            let vec_log_idx_for_this_entry = (log_idx_for_this_entry - 1) as usize;

            if vec_log_idx_for_this_entry < self.log.len() {
                // if entry exists at this index
                if self.log[vec_log_idx_for_this_entry].term != new_entry.term {
                    // conflict?
                    println!(
                        "[Server {}] Conflict at index {}: existing term {}, new entry term {}. Truncating log.",
                        self.id,
                        log_idx_for_this_entry,
                        self.log[vec_log_idx_for_this_entry].term,
                        new_entry.term
                    );
                    self.log.truncate(vec_log_idx_for_this_entry);
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

        if args.leader_commit > self.commit_index {
            // update commit_index based on leaders
            let last_idx_in_our_log = self.log.len() as u64;
            self.commit_index = std::cmp::min(args.leader_commit, last_idx_in_our_log);
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

    pub fn handle_append_entries_reply(
        &mut self,
        from_peer_id: u64,
        reply: AppendEntriesReply,
        request_prev_log_index: u64,
        request_entries_len: usize,
        total_servers: usize,
    ) {
        println!(
            "[Server {} Term {} State {:?}] Received AppendEntriesReply from Peer {} (Term {}, Success: {})",
            self.id, self.current_term, self.state, from_peer_id, reply.term, reply.success
        );

        if self.state != NodeState::Leader {
            println!(
                "[Server {}] Not a Leader, ignoring AppendEntriesReply.",
                self.id
            );
            return;
        }

        if reply.term > self.current_term {
            // oops, we are outdated leader
            println!(
                "[Server {}] AppendEntriesReply has newer term {}. Updating my term, becoming Follower.",
                self.id, reply.term
            );
            self.current_term = reply.term;
            self.state = NodeState::Follower;
            self.voted_for = None;
            self.votes_received.clear();
            self.reset_election_timer();
            return;
        }

        if reply.term < self.current_term {
            println!(
                "[Server {}] Ignoring stale AppendEntriesReply from term {}.",
                self.id, reply.term
            );
            return;
        }

        if reply.success {
            let new_match_index = request_prev_log_index + request_entries_len as u64;
            self.match_index.insert(from_peer_id, new_match_index);
            self.next_index.insert(from_peer_id, new_match_index + 1);

            println!(
                "[Server {}] AppendEntries to S{} successful. Updated match_index={}, next_index={}",
                self.id,
                from_peer_id,
                new_match_index,
                new_match_index + 1
            );
        } else {
            if let Some(ni) = self.next_index.get_mut(&from_peer_id) {
                if *ni > 1 {
                    *ni = (*ni).saturating_sub(1);
                    println!(
                        "[Server {}] Follower {} rejected. Decrementing its next_index to {}",
                        self.id, from_peer_id, *ni
                    );
                } else {
                    println!(
                        "[Server {}] Follower {} rejected. Its next_index is already 1.",
                        self.id, from_peer_id
                    );
                }
            } else {
                println!(
                    "[Server {}] Follower {} rejected. No next_index found, cannot decrement.",
                    self.id, from_peer_id
                );
            }
        }
        self.try_advance_commit_index(total_servers);
    }

    fn try_advance_commit_index(&mut self, total_servers: usize) {
        if self.state != NodeState::Leader {
            return;
        }
        let majority_needed = (total_servers / 2) + 1;
        let mut new_commit_index = self.commit_index;

        for n_candidate in (self.commit_index + 1)..=(self.log.len() as u64) {
            let vec_idx = (n_candidate - 1) as usize;
            if self.log[vec_idx].term == self.current_term {
                let mut replicated_count = 1;
                for &peer_id in &self.peer_ids {
                    if peer_id != self.id {
                        if *self.match_index.get(&peer_id).unwrap_or(&0) >= n_candidate {
                            replicated_count += 1;
                        }
                    }
                }
                if replicated_count >= majority_needed {
                    new_commit_index = n_candidate;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        if new_commit_index > self.commit_index {
            println!(
                "[Server {}] Leader's commit_index advanced from {} to {}",
                self.id, self.commit_index, new_commit_index
            );
            self.commit_index = new_commit_index;
        }
    }

    pub fn apply_committed_entries(&mut self) {
        // applyin to kv store
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
                        self.kv_store.insert(key, value);
                    }
                    Command::Delete { key } => {
                        self.kv_store.remove(&key);
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
                self.last_applied -= 1;
                break;
            }
        }
        if self.last_applied > 0 || !self.kv_store.is_empty() {
            println!(
                "[Server {}] KV Store after applying entries up to index {}: {:?}",
                self.id, self.last_applied, self.kv_store
            );
        }
    }

    pub fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        println!(
            "[Server {} Term {} State {:?}] Received RequestVote from Candidate {} in Term {}",
            self.id, self.current_term, self.state, args.candidate_id, args.term
        );
        let mut vote_granted = false;

        if args.term < self.current_term {
            println!(
                "[Server {}] Rejecting vote: Candidate's term {} is old (our term is {})",
                self.id, args.term, self.current_term
            );
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }

        if args.term > self.current_term {
            println!(
                "[Server {}] Candidate {} has newer term {}. Updating my term, becoming Follower, clearing vote.",
                self.id, args.candidate_id, args.term
            );
            self.current_term = args.term;
            self.state = NodeState::Follower;
            self.voted_for = None;
            self.reset_election_timer();
        }

        let can_grant_vote_based_on_prior_vote = match self.voted_for {
            None => true,
            Some(voted_candidate_id) => voted_candidate_id == args.candidate_id,
        };

        let mut our_last_log_term = 0;
        let mut our_last_log_index = 0;
        if let Some(last_entry) = self.log.last() {
            our_last_log_term = last_entry.term;
            our_last_log_index = self.log.len() as u64;
        }

        let candidate_log_is_at_least_as_up_to_date = if args.last_log_term > our_last_log_term {
            true
        } else if args.last_log_term == our_last_log_term {
            args.last_log_index >= our_last_log_index
        } else {
            false
        };

        if self.current_term == args.term
            && can_grant_vote_based_on_prior_vote
            && candidate_log_is_at_least_as_up_to_date
        {
            println!(
                "[Server {}] Granting vote to Candidate {} for Term {}",
                self.id, args.candidate_id, self.current_term
            );
            self.voted_for = Some(args.candidate_id);
            self.state = NodeState::Follower;
            self.reset_election_timer();
            vote_granted = true;
        } else {
            println!(
                "[Server {}] Rejecting vote for Candidate {} for Term {}. OurTerm ({}) vs ArgsTerm ({}). Prior vote: {:?}, Candidate up-to-date: {}",
                self.id,
                args.candidate_id,
                args.term,
                self.current_term,
                args.term,
                self.voted_for,
                candidate_log_is_at_least_as_up_to_date
            );
        }

        RequestVoteReply {
            term: self.current_term,
            vote_granted,
        }
    }

    pub fn handle_request_vote_reply(
        &mut self,
        reply: RequestVoteReply,
        from_peer_id: u64,
        total_servers: usize,
    ) -> Option<Vec<(u64, RpcMessage)>> {
        // returns messages if becomes leader
        println!(
            "[Server {} Term {} State {:?}] Received RequestVoteReply from Peer {} (Term {}, Granted: {})",
            self.id, self.current_term, self.state, from_peer_id, reply.term, reply.vote_granted
        );

        if self.state != NodeState::Candidate {
            println!("[Server {}] Not a candidate, ignoring vote reply.", self.id);
            return None;
        }

        if reply.term > self.current_term {
            // we are outdated
            println!(
                "[Server {}] Reply has newer term {}. Updating my term, becoming Follower.",
                self.id, reply.term
            );
            self.current_term = reply.term;
            self.state = NodeState::Follower;
            self.voted_for = None;
            self.votes_received.clear();
            self.reset_election_timer();
            return None;
        }

        if reply.term != self.current_term {
            println!(
                "[Server {}] Ignoring stale vote reply for term {} (our term is {}).",
                self.id, reply.term, self.current_term
            );
            return None;
        }

        if reply.vote_granted {
            self.votes_received.insert(from_peer_id);
            println!(
                "[Server {}] Vote granted by {}. Total votes for term {}: {}/{}",
                self.id,
                from_peer_id,
                self.current_term,
                self.votes_received.len(),
                total_servers
            );

            let majority_needed = (total_servers / 2) + 1;
            if self.votes_received.len() >= majority_needed {
                println!(
                    "[Server {} Term {}] Won election with {} votes! Becoming LEADER.",
                    self.id,
                    self.current_term,
                    self.votes_received.len()
                );
                self.state = NodeState::Leader;
                self.votes_received.clear();

                let mut initial_heartbeats = Vec::new();
                for &peer_id_target in &self.peer_ids {
                    // send initial heartbeats
                    if peer_id_target != self.id {
                        self.next_index
                            .insert(peer_id_target, (self.log.len() + 1) as u64);
                        self.match_index.insert(peer_id_target, 0);

                        let args = AppendEntriesArgs {
                            term: self.current_term,
                            leader_id: self.id,
                            prev_log_index: self.log.len() as u64,
                            prev_log_term: self.log.last().map_or(0, |e| e.term),
                            entries: Vec::new(),
                            leader_commit: self.commit_index,
                        };
                        initial_heartbeats.push((peer_id_target, RpcMessage::AppendEntries(args)));
                    }
                }
                return Some(initial_heartbeats); // signal to send these message
            }
        }
        None
    }
}
