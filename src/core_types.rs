use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

#[derive(Debug, PartialEq, Clone, Copy, Serialize)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Command {
    Set { key: String, value: String },
    Delete { key: String },
    NoOp,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogEntry {
    pub term: u64,
    pub command: Command,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcMessage {
    RequestVote(RequestVoteArgs),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntriesArgs),
    AppendEntriesReply(AppendEntriesReply),
    InstallSnapshot(InstallSnapshotArgs),
    InstallSnapshotReply(InstallSnapshotReply),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientRequest {
    Get { key: String },
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientReply {
    Value {
        key: String,
        value: Option<String>,
    },
    Success {
        command_applied_at_log_index: u64,
    },
    Error {
        msg: String,
    },
    LeaderRedirect {
        leader_id: u64,
        leader_addr: Option<String>,
    },
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct PersistentMetadata {
    current_term: u64,
    voted_for: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotArgs {
    pub term: u64,
    pub leader_id: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub offset: u64,
    pub data: Vec<u8>,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotReply {
    pub term: u64,
}

#[derive(Debug)]
struct PendingRead {
    key: String,
    registered_commit_index: u64,
    reply_channel: Option<oneshot::Sender<ClientReply>>,
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
    pub election_timeout_due: Instant,
    pub heartbeat_interval: Duration,
    pub last_heartbeat: Instant,
    pub election_timeout_base: Duration,
    pub peer_ids: Vec<u64>,
    pub votes_received: HashSet<u64>,
    pub pending_client_acks: HashMap<u64, oneshot::Sender<ClientReply>>,
    pending_reads: Vec<PendingRead>,
    pub current_leader_id: Option<u64>,
    metadata_path: String,
    log_path: String,
    pub snapshot_path: String,
    pub last_snapshot_index: u64,
    pub last_snapshot_term: u64,
    pub snapshotting: bool,
}

const CORE_DETAILED_LOGS: bool = true;

impl Server {
    pub fn new(id: u64, peer_ids: Vec<u64>, metadata_path: String, log_path: String) -> Self {
        let election_timeout_base = Duration::from_millis(300);
        let initial_due_placeholder =
            Instant::now() + Self::randomized_election_timeout(election_timeout_base);
        let snapshot_path = format!("{}.snap", log_path);

        let mut server = Server {
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
            election_timeout_due: initial_due_placeholder,
            heartbeat_interval: Duration::from_millis(50),
            last_heartbeat: Instant::now(),
            election_timeout_base,
            peer_ids,
            votes_received: HashSet::new(),
            pending_client_acks: HashMap::new(),
            pending_reads: Vec::new(),
            current_leader_id: None,
            metadata_path,
            log_path,
            snapshot_path,
            last_snapshot_index: 0,
            last_snapshot_term: 0,
            snapshotting: false,
        };

        server.load_persistent_state();
        let _ = server.load_snapshot();
        server.reset_election_timer();

        println!(
            "S{} NEW_PERSIST: Server ready (loaded/initialized). Term: {}, \
            VotedFor: {:?}, LogLen: {}. Election due: {:?}. Snapshot Index: {}.",
            server.id,
            server.current_term,
            server.voted_for,
            server.log.len(),
            server.election_timeout_due,
            server.last_snapshot_index
        );
        server
    }

    fn randomized_election_timeout(base_duration: Duration) -> Duration {
        let jitter_ms = rand::thread_rng().gen_range(0..=base_duration.as_millis() as u64);
        base_duration + Duration::from_millis(jitter_ms)
    }

    pub fn reset_election_timer(&mut self) {
        self.election_timeout_due =
            Instant::now() + Self::randomized_election_timeout(self.election_timeout_base);

        if CORE_DETAILED_LOGS {
            println!(
                "S{} T{} ELECTION_TIMER_RESET: Due at: {:?}",
                self.id, self.current_term, self.election_timeout_due
            );
        }
    }

    pub fn register_linearizable_read(
        &mut self,
        key: String,
        reply_channel: oneshot::Sender<ClientReply>,
    ) {
        let current_commit_idx = self.commit_index;

        if CORE_DETAILED_LOGS {
            println!(
                "S{} T{} REGISTER_LINEARIZABLE_READ: Key '{}' at commit_idx {}. KV_applied_idx: {}",
                self.id, self.current_term, key, current_commit_idx, self.last_applied
            );
        }

        self.pending_reads.push(PendingRead {
            key,
            registered_commit_index: current_commit_idx,
            reply_channel: Some(reply_channel),
        });
    }

    fn save_metadata(&self) {
        if CORE_DETAILED_LOGS {
            println!(
                "S{} T{} PERSIST_META_SAVE: Saving term {} and voted_for {:?}",
                self.id, self.current_term, self.current_term, self.voted_for
            );
        }

        let metadata = PersistentMetadata {
            current_term: self.current_term,
            voted_for: self.voted_for,
        };
        let temp_metadata_path = format!("{}.tmp", self.metadata_path);
        let parent_dir = Path::new(&self.metadata_path)
            .parent()
            .unwrap_or_else(|| Path::new("."));

        if !parent_dir.exists() {
            if let Err(e) = fs::create_dir_all(parent_dir) {
                eprintln!(
                    "S{} T{} PERSIST_META_SAVE_ERR: Failed to create parent directory {:?} \
                    for metadata: {}",
                    self.id, self.current_term, parent_dir, e
                );
                return;
            }
        }

        match File::create(&temp_metadata_path) {
            Ok(file) => {
                let mut writer = BufWriter::new(file);
                if let Err(e) = bincode::serialize_into(&mut writer, &metadata) {
                    eprintln!(
                        "S{} T{} PERSIST_META_SAVE_ERR: Failed to serialize metadata: {}",
                        self.id, self.current_term, e
                    );
                    let _ = fs::remove_file(&temp_metadata_path);
                    return;
                }

                if let Err(e) = writer.flush() {
                    eprintln!(
                        "S{} T{} PERSIST_META_SAVE_ERR: Failed to flush writer for metadata: {}",
                        self.id, self.current_term, e
                    );
                    let _ = fs::remove_file(&temp_metadata_path);
                    return;
                }

                if let Err(e) = writer.get_ref().sync_all() {
                    eprintln!(
                        "S{} T{} PERSIST_META_SAVE_ERR: Failed to fsync metadata file {}: {}",
                        self.id, self.current_term, temp_metadata_path, e
                    );
                    let _ = fs::remove_file(&temp_metadata_path);
                    return;
                }

                if let Err(e) = fs::rename(&temp_metadata_path, &self.metadata_path) {
                    eprintln!(
                        "S{} T{} PERSIST_META_SAVE_ERR: Failed to rename temp metadata file \
                        to {}: {}",
                        self.id, self.current_term, self.metadata_path, e
                    );
                    let _ = fs::remove_file(&temp_metadata_path);
                } else {
                    if let Ok(parent_dir_file) = File::open(parent_dir) {
                        if let Err(e_fsync_dir) = parent_dir_file.sync_all() {
                            eprintln!(
                                "S{} T{} PERSIST_META_SAVE_WARN: Failed to fsync parent directory \
                                {:?} after rename: {}",
                                self.id, self.current_term, parent_dir, e_fsync_dir
                            );
                        }
                    }

                    if CORE_DETAILED_LOGS {
                        println!(
                            "S{} T{} PERSIST_META_SAVE_OK: Metadata saved to {}",
                            self.id, self.current_term, self.metadata_path
                        );
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "S{} T{} PERSIST_META_SAVE_ERR: Failed to create temp metadata file {}: {}",
                    self.id, self.current_term, temp_metadata_path, e
                );
            }
        }
    }

    pub fn persist_log_entry(&self, entry_index_in_mem_log: usize) {
        if entry_index_in_mem_log >= self.log.len() {
            eprintln!(
                "S{} T{} PERSIST_LOG_APPEND_ERR: Index {} out of bounds for log len {}. \
                Critical error.",
                self.id,
                self.current_term,
                entry_index_in_mem_log,
                self.log.len()
            );
            return;
        }

        let log_entry = &self.log[entry_index_in_mem_log];
        if CORE_DETAILED_LOGS {
            println!(
                "S{} T{} PERSIST_LOG_APPEND: Persisting log entry at mem_idx {} (Raft idx {}): {:?}",
                self.id,
                self.current_term,
                entry_index_in_mem_log,
                entry_index_in_mem_log + 1,
                log_entry.command
            );
        }

        match OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.log_path)
        {
            Ok(file) => {
                let mut writer = BufWriter::new(file);
                match bincode::serialize(log_entry) {
                    Ok(serialized_entry) => {
                        let len_bytes = (serialized_entry.len() as u32).to_be_bytes();
                        if writer.write_all(&len_bytes).is_err()
                            || writer.write_all(&serialized_entry).is_err()
                        {
                            eprintln!(
                                "S{} T{} PERSIST_LOG_APPEND_ERR: Failed to write entry to log \
                                file {}: I/O error",
                                self.id, self.current_term, self.log_path
                            );
                            return;
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "S{} T{} PERSIST_LOG_APPEND_ERR: Failed to serialize log entry: {}",
                            self.id, self.current_term, e
                        );
                        return;
                    }
                }

                if let Err(e) = writer.flush() {
                    eprintln!(
                        "S{} T{} PERSIST_LOG_APPEND_ERR: Failed to flush writer for log file: {}",
                        self.id, self.current_term, e
                    );
                    return;
                }

                if let Err(e) = writer.get_ref().sync_all() {
                    eprintln!(
                        "S{} T{} PERSIST_LOG_APPEND_ERR: Failed to fsync log file {}: {}",
                        self.id, self.current_term, self.log_path, e
                    );
                } else if CORE_DETAILED_LOGS {
                    println!(
                        "S{} T{} PERSIST_LOG_APPEND_OK: Log entry persisted and fsynced to {}.",
                        self.id, self.current_term, self.log_path
                    );
                }
            }
            Err(e) => {
                eprintln!(
                    "S{} T{} PERSIST_LOG_APPEND_ERR: Failed to open/create log file {} for append: {}",
                    self.id, self.current_term, self.log_path, e
                );
            }
        }
    }

    fn rebuild_persisted_log(&self) {
        if CORE_DETAILED_LOGS {
            println!(
                "S{} T{} PERSIST_LOG_REBUILD: Rebuilding entire persisted log ({} entries) to {}",
                self.id,
                self.current_term,
                self.log.len(),
                self.log_path
            );
        }

        let temp_log_path = format!("{}.tmp", self.log_path);
        let parent_dir = Path::new(&self.log_path)
            .parent()
            .unwrap_or_else(|| Path::new("."));

        match File::create(&temp_log_path) {
            Ok(file) => {
                let mut writer = BufWriter::new(file);
                for log_entry in &self.log {
                    match bincode::serialize(log_entry) {
                        Ok(serialized_entry) => {
                            let len_bytes = (serialized_entry.len() as u32).to_be_bytes();
                            if writer.write_all(&len_bytes).is_err()
                                || writer.write_all(&serialized_entry).is_err()
                            {
                                eprintln!(
                                    "S{} T{} PERSIST_LOG_REBUILD_ERR: Failed to write entry \
                                    during log rebuild: I/O error",
                                    self.id, self.current_term
                                );
                                let _ = fs::remove_file(&temp_log_path);
                                return;
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "S{} T{} PERSIST_LOG_REBUILD_ERR: Failed to serialize log entry \
                                during rebuild: {}",
                                self.id, self.current_term, e
                            );
                            let _ = fs::remove_file(&temp_log_path);
                            return;
                        }
                    }
                }

                if let Err(e) = writer.flush() {
                    eprintln!(
                        "S{} T{} PERSIST_LOG_REBUILD_ERR: Failed to flush writer for temp log file: {}",
                        self.id, self.current_term, e
                    );
                    let _ = fs::remove_file(&temp_log_path);
                    return;
                }

                if let Err(e) = writer.get_ref().sync_all() {
                    eprintln!(
                        "S{} T{} PERSIST_LOG_REBUILD_ERR: Failed to fsync temp log file {}: {}",
                        self.id, self.current_term, temp_log_path, e
                    );
                    let _ = fs::remove_file(&temp_log_path);
                    return;
                }

                if let Err(e) = fs::rename(&temp_log_path, &self.log_path) {
                    eprintln!(
                        "S{} T{} PERSIST_LOG_REBUILD_ERR: Failed to rename temp log file \
                        to {}: {}",
                        self.id, self.current_term, self.log_path, e
                    );
                    let _ = fs::remove_file(&temp_log_path);
                } else {
                    if let Ok(parent_dir_file) = File::open(parent_dir) {
                        if let Err(e_fsync_dir) = parent_dir_file.sync_all() {
                            eprintln!(
                                "S{} T{} PERSIST_LOG_REBUILD_WARN: Failed to fsync parent \
                                directory {:?} after rename: {}",
                                self.id, self.current_term, parent_dir, e_fsync_dir
                            );
                        }
                    }

                    if CORE_DETAILED_LOGS {
                        println!(
                            "S{} T{} PERSIST_LOG_REBUILD_OK: Persisted log rebuilt and fsynced.",
                            self.id, self.current_term
                        );
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "S{} T{} PERSIST_LOG_REBUILD_ERR: Failed to create temp log file {}: {}",
                    self.id, self.current_term, temp_log_path, e
                );
            }
        }
    }

    fn load_persistent_state(&mut self) {
        match File::open(&self.metadata_path) {
            Ok(file) => {
                let reader = BufReader::new(file);
                match bincode::deserialize_from(reader) {
                    Ok(metadata @ PersistentMetadata { .. }) => {
                        self.current_term = metadata.current_term;
                        self.voted_for = metadata.voted_for;
                        println!(
                            "S{} PERSIST_META_LOAD_OK: Loaded term {} and voted_for {:?} from {}",
                            self.id, self.current_term, self.voted_for, self.metadata_path
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "S{} PERSIST_META_LOAD_ERR: Failed to deserialize metadata from {}: {}. \
                            Using defaults.",
                            self.id, self.metadata_path, e
                        );
                        self.save_metadata();
                    }
                }
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                println!(
                    "S{} PERSIST_META_LOAD_INFO: Metadata file {} not found. \
                    Assuming fresh start, saving defaults.",
                    self.id, self.metadata_path
                );
                self.save_metadata();
            }
            Err(e) => {
                eprintln!(
                    "S{} PERSIST_META_LOAD_ERR: Failed to open metadata file {}: {}. Using defaults.",
                    self.id, self.metadata_path, e
                );
            }
        }

        match OpenOptions::new().read(true).open(&self.log_path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                let mut temp_log_buffer = Vec::new();
                loop {
                    let mut len_bytes = [0u8; 4];
                    match reader.read_exact(&mut len_bytes) {
                        Ok(_) => {
                            let entry_len = u32::from_be_bytes(len_bytes) as usize;
                            if entry_len == 0 || entry_len > 2_097_152 {
                                eprintln!(
                                    "S{} PERSIST_LOG_LOAD_ERR: Invalid log entry length {} in {}. \
                                    Log load stopped.",
                                    self.id, entry_len, self.log_path
                                );
                                break;
                            }

                            temp_log_buffer.resize(entry_len, 0);
                            match reader.read_exact(&mut temp_log_buffer) {
                                Ok(_) => match bincode::deserialize::<LogEntry>(&temp_log_buffer) {
                                    Ok(log_entry) => self.log.push(log_entry),
                                    Err(e) => {
                                        eprintln!(
                                            "S{} PERSIST_LOG_LOAD_ERR: Failed to deserialize \
                                                log entry from {}: {}. Log load stopped.",
                                            self.id, self.log_path, e
                                        );
                                        break;
                                    }
                                },
                                Err(e) => {
                                    eprintln!(
                                        "S{} PERSIST_LOG_LOAD_ERR: Failed to read log entry body \
                                        from {}: {}. Log load stopped.",
                                        self.id, self.log_path, e
                                    );
                                    break;
                                }
                            }
                        }
                        Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                        Err(e) => {
                            eprintln!(
                                "S{} PERSIST_LOG_LOAD_ERR: Failed to read log entry length from {}: \
                                {}. Log load stopped.",
                                self.id, self.log_path, e
                            );
                            break;
                        }
                    }
                }
                println!(
                    "S{} PERSIST_LOG_LOAD_OK: Loaded {} entries from {}.",
                    self.id,
                    self.log.len(),
                    self.log_path
                );
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                println!(
                    "S{} PERSIST_LOG_LOAD_INFO: Log file {} not found. Assuming fresh start.",
                    self.id, self.log_path
                );
                if let Err(e_create) = File::create(&self.log_path) {
                    eprintln!(
                        "S{} PERSIST_LOG_LOAD_ERR: Failed to create empty log file {}: {}",
                        self.id, self.log_path, e_create
                    );
                }
            }
            Err(e) => {
                eprintln!(
                    "S{} PERSIST_LOG_LOAD_ERR: Failed to open log file {}: {}. \
                    Starting with empty log.",
                    self.id, self.log_path, e
                );
            }
        }

        self.kv_store.clear();
        self.last_applied = 0;

        if !self.log.is_empty() {
            println!(
                "S{} PERSIST_LOAD_REAPPLY: Re-applying {} loaded log entries to rebuild kv_store.",
                self.id,
                self.log.len()
            );
            self.commit_index = self.log.len() as u64;
            self.apply_committed_entries();
            println!(
                "S{} PERSIST_LOAD_REAPPLY_DONE: kv_store rebuilt. last_applied={}, \
                commit_index={}. kv_size={}",
                self.id,
                self.last_applied,
                self.commit_index,
                self.kv_store.len()
            );
        } else {
            self.commit_index = 0;
            println!(
                "S{} PERSIST_LOAD_NO_REAPPLY: Log is empty. last_applied=0, commit_index=0.",
                self.id
            );
        }
    }

    pub fn should_snapshot(&self) -> bool {
        self.last_applied - self.last_snapshot_index > 1000
    }

    pub fn take_snapshot(&mut self) -> std::io::Result<()> {
        if self.snapshotting {
            return Ok(());
        }
        self.snapshotting = true;

        println!(
            "S{} T{} SNAPSHOT_START: Creating snapshot at index {}",
            self.id, self.current_term, self.last_applied
        );

        let kv_store_copy = self.kv_store.clone();
        let snapshot = Snapshot {
            last_included_index: self.last_applied,
            last_included_term: self
                .log
                .get((self.last_applied as usize).saturating_sub(1))
                .map_or(0, |e| e.term),
            data: bincode::serialize(&kv_store_copy)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
        };

        let temp_path = format!("{}.tmp", self.snapshot_path);
        {
            let mut file = File::create(&temp_path)?;
            bincode::serialize_into(&mut file, &snapshot)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            file.sync_all()?;
        }

        fs::rename(temp_path, &self.snapshot_path)?;

        self.last_snapshot_index = self.last_applied;
        self.last_snapshot_term = snapshot.last_included_term;
        self.snapshotting = false;

        println!(
            "S{} T{} SNAPSHOT_COMPLETE: Saved snapshot for index {}",
            self.id, self.current_term, self.last_applied
        );

        Ok(())
    }

    pub fn load_snapshot(&mut self) -> std::io::Result<()> {
        if let Ok(file) = File::open(&self.snapshot_path) {
            let snapshot: Snapshot = bincode::deserialize_from(file)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            self.kv_store = bincode::deserialize(&snapshot.data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            let snapshot_idx = snapshot.last_included_index;

            let mut indices_to_keep = Vec::new();
            for (i, _) in self.log.iter().enumerate() {
                let raft_index = self.log_position_to_index(i);
                if raft_index > snapshot_idx {
                    indices_to_keep.push(i);
                }
            }

            let mut new_log = Vec::new();
            for i in indices_to_keep {
                if let Some(entry) = self.log.get(i) {
                    new_log.push(entry.clone());
                }
            }

            self.log = new_log;
            self.last_snapshot_index = snapshot.last_included_index;
            self.last_snapshot_term = snapshot.last_included_term;
            self.last_applied = snapshot.last_included_index;
            self.commit_index = snapshot.last_included_index;
        }
        Ok(())
    }

    pub fn log_position_to_index(&self, pos: usize) -> u64 {
        pos as u64 + self.last_snapshot_index + 1
    }

    pub fn handle_install_snapshot(&mut self, args: InstallSnapshotArgs) -> InstallSnapshotReply {
        if args.term < self.current_term {
            return InstallSnapshotReply {
                term: self.current_term,
            };
        }

        if args.done {
            self.kv_store = bincode::deserialize(&args.data)
                .map_err(|e| {
                    eprintln!("Snapshot deserialization error: {}", e);
                    e
                })
                .unwrap_or_default();

            self.last_applied = args.last_included_index;
            self.commit_index = args.last_included_index;

            // Store entries that should be retained
            let mut new_log = Vec::new();
            for (i, entry) in self.log.iter().enumerate() {
                let index = self.log_position_to_index(i);
                if index > args.last_included_index {
                    new_log.push(entry.clone());
                }
            }

            self.log = new_log;
        }

        InstallSnapshotReply {
            term: self.current_term,
        }
    }

    pub fn tick(&mut self) -> Vec<(u64, RpcMessage)> {
        let mut messages_to_send: Vec<(u64, RpcMessage)> = Vec::new();
        let now = Instant::now();
        let previous_state = self.state;

        match self.state {
            NodeState::Follower => {
                if now >= self.election_timeout_due {
                    println!(
                        "S{} T{} ELECTION_TIMEOUT: Follower timeout (due {:?}, now {:?}). \
                        Becoming Candidate.",
                        self.id, self.current_term, self.election_timeout_due, now
                    );
                    self.state = NodeState::Candidate;
                    self.current_leader_id = None;
                }
            }
            NodeState::Candidate => {
                if previous_state != NodeState::Candidate || now >= self.election_timeout_due {
                    let new_term_candidate = self.current_term + 1;
                    println!(
                        "S{} ELECTION_START: Candidate starting/restarting election for new Term {}.",
                        self.id, new_term_candidate
                    );
                    self.current_term = new_term_candidate;
                    self.voted_for = Some(self.id);
                    self.current_leader_id = None;
                    self.save_metadata();
                    self.votes_received.clear();
                    self.votes_received.insert(self.id);
                    self.reset_election_timer();

                    for &peer_id in &self.peer_ids {
                        if peer_id != self.id {
                            let args = RequestVoteArgs {
                                term: self.current_term,
                                candidate_id: self.id,
                                last_log_index: self.log.len() as u64,
                                last_log_term: self.log.last().map_or(0, |entry| entry.term),
                            };

                            if CORE_DETAILED_LOGS {
                                println!(
                                    "S{} T{} ELECTION_SEND_RV -> S{}: Sending RequestVote: {:?}",
                                    self.id, self.current_term, peer_id, args
                                );
                            }

                            messages_to_send.push((peer_id, RpcMessage::RequestVote(args)));
                        }
                    }
                }
            }
            NodeState::Leader => {
                // Check if we need to send heartbeats
                let elapsed = self.last_heartbeat.elapsed();
                if elapsed >= self.heartbeat_interval {
                    println!(
                        "S{} T{} LEADER_HEARTBEAT: Sending to all peers (last: {:?}, interval: {:?})",
                        self.id, self.current_term, self.last_heartbeat, self.heartbeat_interval
                    );

                    for &peer_id in &self.peer_ids {
                        if peer_id != self.id {
                            let next_idx = *self.next_index.get(&peer_id).unwrap_or(&1);
                            let prev_idx = next_idx.saturating_sub(1);
                            let prev_term = if prev_idx > 0 {
                                self.log.get((prev_idx - 1) as usize).map_or(0, |e| e.term)
                            } else {
                                0
                            };

                            let entries = if next_idx <= self.log.len() as u64 {
                                self.log[next_idx as usize..].to_vec()
                            } else {
                                Vec::new()
                            };

                            if CORE_DETAILED_LOGS {
                                if !entries.is_empty() {
                                    println!(
                                        "S{} T{} SENDING_ENTRIES -> S{}: {} entries from idx {}",
                                        self.id,
                                        self.current_term,
                                        peer_id,
                                        entries.len(),
                                        next_idx
                                    );
                                } else {
                                    println!(
                                        "S{} T{} SENDING_HEARTBEAT -> S{}",
                                        self.id, self.current_term, peer_id
                                    );
                                }
                            }

                            messages_to_send.push((
                                peer_id,
                                RpcMessage::AppendEntries(AppendEntriesArgs {
                                    term: self.current_term,
                                    leader_id: self.id,
                                    prev_log_index: prev_idx,
                                    prev_log_term: prev_term,
                                    entries,
                                    leader_commit: self.commit_index,
                                }),
                            ));
                        }
                    }
                    self.last_heartbeat = Instant::now();
                }
            }
        }

        let old_last_applied = self.last_applied;
        self.apply_committed_entries();


        if self.last_applied > old_last_applied && CORE_DETAILED_LOGS {
            println!(
                "S{} T{} APPLY_BATCH_DONE: Applied {} entries ({}->{}). KV store size: {}.",
                self.id,
                self.current_term,
                self.last_applied - old_last_applied,
                old_last_applied,
                self.last_applied,
                self.kv_store.len()
            );
        }

        messages_to_send 
    }

    pub fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        if CORE_DETAILED_LOGS {
            println!(
                "S{} T{} AE_RECV from L{}(T{}): state={:?}, prev_idx={}, \
                prev_term={}, entries_len={}, lc={}",
                self.id,
                self.current_term,
                args.leader_id,
                args.term,
                self.state,
                args.prev_log_index,
                args.prev_log_term,
                args.entries.len(),
                args.leader_commit
            );
        }

        if args.term < self.current_term {
            println!(
                "S{} T{} AE_REJECT_OLD_TERM from L{}(T{}).",
                self.id, self.current_term, args.leader_id, args.term
            );
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
            };
        }

        self.reset_election_timer();
        self.current_leader_id = Some(args.leader_id);

        if args.term > self.current_term {
            println!(
                "S{} TERM_UPDATE_AE from L{}(T{}): New term (our T{} -> T{}). Becoming Follower.",
                self.id, args.leader_id, args.term, self.current_term, args.term
            );
            self.current_term = args.term;
            self.state = NodeState::Follower;
            self.voted_for = None;
            self.save_metadata();
        } else if self.state == NodeState::Candidate {
            println!(
                "S{} T{} CANDIDATE_TO_FOLLOWER_AE from L{}: Stepping down.",
                self.id, self.current_term, args.leader_id
            );
            self.state = NodeState::Follower;
        }

        if args.prev_log_index > 0 {
            let vec_prev_idx = (args.prev_log_index - 1) as usize;
            if vec_prev_idx >= self.log.len() || self.log[vec_prev_idx].term != args.prev_log_term {
                println!(
                    "S{} T{} AE_REJECT_LOG_MISMATCH from L{}(T{}): prev_idx {}, prev_term {} \
                    vs our log_len {}, term at idx {:?}.",
                    self.id,
                    self.current_term,
                    args.leader_id,
                    args.term,
                    args.prev_log_index,
                    args.prev_log_term,
                    self.log.len(),
                    if vec_prev_idx < self.log.len() {
                        Some(self.log[vec_prev_idx].term)
                    } else {
                        None
                    }
                );
                return AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                };
            }
        }

        let mut log_changed = false;
        for (offset, new_entry) in args.entries.iter().enumerate() {
            let entry_idx_raft = args.prev_log_index + 1 + offset as u64;
            let vec_entry_idx = (entry_idx_raft - 1) as usize;

            if vec_entry_idx < self.log.len() {
                if self.log[vec_entry_idx].term != new_entry.term {
                    println!(
                        "S{} T{} LOG_TRUNCATE_CONFLICT at raft_idx {}: existing T{}, new T{}. \
                        Truncating.",
                        self.id,
                        self.current_term,
                        entry_idx_raft,
                        self.log[vec_entry_idx].term,
                        new_entry.term
                    );
                    self.log.truncate(vec_entry_idx);
                    self.rebuild_persisted_log();
                    log_changed = true;

                    self.log.push(new_entry.clone());
                    self.persist_log_entry(self.log.len() - 1);
                }
            } else {
                self.log.push(new_entry.clone());
                self.persist_log_entry(self.log.len() - 1);
                log_changed = true;
            }
        }

        if CORE_DETAILED_LOGS && log_changed {
            println!(
                "S{} T{} LOG_STATE_AFTER_AE_PERSIST: Log len now {}.",
                self.id,
                self.current_term,
                self.log.len()
            );
        }

        if args.leader_commit > self.commit_index {
            let old_ci = self.commit_index;
            self.commit_index = std::cmp::min(args.leader_commit, self.log.len() as u64);
            if self.commit_index > old_ci {
                println!(
                    "S{} T{} FOLLOWER_COMMIT_UPDATE: Commit idx {} -> {} by L{}(lc={}).",
                    self.id,
                    self.current_term,
                    old_ci,
                    self.commit_index,
                    args.leader_id,
                    args.leader_commit
                );
            }
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
        req_prev_log_idx: u64,
        req_entries_len: usize,
        total_servers: usize,
    ) {
        if CORE_DETAILED_LOGS {
            println!(
                "S{} T{} AE_REPLY_RECV from P{}(T{}): state={:?}, success={}, \
                req_prev_idx={}, req_entries_len={}",
                self.id,
                self.current_term,
                from_peer_id,
                reply.term,
                self.state,
                reply.success,
                req_prev_log_idx,
                req_entries_len
            );
        }

        if self.state != NodeState::Leader {
            println!(
                "S{} T{} AE_REPLY_IGNORE_NOT_LEADER from P{}.",
                self.id, self.current_term, from_peer_id
            );
            return;
        }

        if reply.term > self.current_term {
            println!(
                "S{} T{} TERM_UPDATE_AE_REPLY from P{}(T{}): Newer term. Becoming Follower.",
                self.id, self.current_term, from_peer_id, reply.term
            );
            self.current_term = reply.term;
            self.state = NodeState::Follower;
            self.voted_for = None;
            self.votes_received.clear();
            self.save_metadata();
            self.reset_election_timer();
            return;
        }

        if reply.term < self.current_term {
            if CORE_DETAILED_LOGS {
                println!(
                    "S{} T{} AE_REPLY_IGNORE_STALE_TERM from P{}(T{}).",
                    self.id, self.current_term, from_peer_id, reply.term
                );
            }
            return;
        }

        if reply.success {
            let new_match = req_prev_log_idx + req_entries_len as u64;
            self.match_index.insert(from_peer_id, new_match);
            self.next_index.insert(from_peer_id, new_match + 1);

            if CORE_DETAILED_LOGS {
                println!(
                    "S{} T{} AE_REPLY_SUCCESS from P{}: New match_idx={}, next_idx={}",
                    self.id,
                    self.current_term,
                    from_peer_id,
                    new_match,
                    new_match + 1
                );
            }
        } else {
            if let Some(ni) = self.next_index.get_mut(&from_peer_id) {
                let old_ni = *ni;
                *ni = (*ni).saturating_sub(1).max(1);
                println!(
                    "S{} T{} AE_REPLY_FAIL_DEC_NEXT_IDX from P{}: Peer rejected. \
                    Decrementing next_idx {} -> {}.",
                    self.id, self.current_term, from_peer_id, old_ni, *ni
                );
            } else {
                eprintln!(
                    "S{} T{} AE_REPLY_FAIL_NO_NEXT_IDX for P{}.",
                    self.id, self.current_term, from_peer_id
                );
            }
        }

        self.try_advance_commit_index(total_servers);
    }

    fn try_advance_commit_index(&mut self, total_servers: usize) {
        if self.state != NodeState::Leader {
            return;
        }

        let majority = (total_servers / 2) + 1;
        let mut new_ci = self.commit_index;

        for n_candidate in (self.commit_index + 1)..=(self.log.len() as u64) {
            if self
                .log
                .get((n_candidate - 1) as usize)
                .map_or(false, |e| e.term == self.current_term)
            {
                let count = 1 + self
                    .peer_ids
                    .iter()
                    .filter(|&&p| {
                        p != self.id && *self.match_index.get(&p).unwrap_or(&0) >= n_candidate
                    })
                    .count();

                if count >= majority {
                    new_ci = n_candidate;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        if new_ci > self.commit_index {
            println!(
                "S{} T{} LEADER_COMMIT_ADVANCE: Commit idx {} -> {}.",
                self.id, self.current_term, self.commit_index, new_ci
            );
            self.commit_index = new_ci;
        }
    }

    pub fn apply_committed_entries(&mut self) {
        while self.last_applied < self.commit_index {
            let current_apply_idx_raft = self.last_applied + 1;
            let vec_idx_to_apply = self.last_applied as usize;

            if vec_idx_to_apply < self.log.len() {
                let entry = self.log[vec_idx_to_apply].clone();

                match entry.command {
                    Command::Set { key, value } => {
                        if CORE_DETAILED_LOGS {
                            println!(
                                "S{} T{} APPLY_LOG_ENTRY_SET: Applying Set cmd for key '{}' at raft_idx {}.",
                                self.id, self.current_term, key, current_apply_idx_raft
                            );
                        }
                        self.kv_store.insert(key, value);
                    }
                    Command::Delete { key } => {
                        if CORE_DETAILED_LOGS {
                            println!(
                                "S{} T{} APPLY_LOG_ENTRY_DELETE: Applying Delete cmd for key '{}' at raft_idx {}.",
                                self.id, self.current_term, key, current_apply_idx_raft
                            );
                        }
                        self.kv_store.remove(&key);
                    }
                    Command::NoOp => {
                        if CORE_DETAILED_LOGS {
                            println!(
                                "S{} T{} APPLY_LOG_ENTRY_NOOP: Applying NoOp at raft_idx {}.",
                                self.id, self.current_term, current_apply_idx_raft
                            );
                        }
                    }
                }

                self.last_applied = current_apply_idx_raft;

                if let Some(ack_sender) = self.pending_client_acks.remove(&current_apply_idx_raft) {
                    let reply = ClientReply::Success {
                        command_applied_at_log_index: current_apply_idx_raft,
                    };
                    if ack_sender.send(reply).is_ok() {
                        if CORE_DETAILED_LOGS {
                            println!(
                                "S{} T{} CLIENT_ACK_SENT_APPLY: Sent commit ACK for log raft_idx {}.",
                                self.id, self.current_term, current_apply_idx_raft
                            );
                        }
                    } else {
                        eprintln!(
                            "S{} T{} CLIENT_ACK_FAIL_APPLY: Failed to send commit ACK for log raft_idx {} (client likely disconnected).",
                            self.id, self.current_term, current_apply_idx_raft
                        );
                    }
                }
            } else {
                eprintln!(
                    "S{} T{} APPLY_LOG_CRITICAL_ERR: Trying to apply log raft_idx {} (vec_idx {}) but log_len is {}. Halting apply loop.",
                    self.id,
                    self.current_term,
                    current_apply_idx_raft,
                    vec_idx_to_apply,
                    self.log.len()
                );
                break;
            }
        }
        if self.state == NodeState::Leader {
            let current_last_applied_for_reads = self.last_applied;
            let mut reads_processed_this_cycle = 0;

            self.pending_reads.retain_mut(|read_op| {
            if current_last_applied_for_reads >= read_op.registered_commit_index {
                if CORE_DETAILED_LOGS {
                    println!(
                        "S{} T{} PROCESS_LINEARIZABLE_READ: Key '{}'. Registered commit_idx: {}, Current last_applied: {}. Ready to process.",
                        self.id, self.current_term, read_op.key, read_op.registered_commit_index, current_last_applied_for_reads
                    );
                }

                let value = self.kv_store.get(&read_op.key).cloned();
                let client_reply = ClientReply::Value { key: read_op.key.clone(), value };

                if let Some(sender) = read_op.reply_channel.take() {
                    if sender.send(client_reply).is_err() {
                        if CORE_DETAILED_LOGS {
                            println!(
                                "S{} T{} PROCESS_LINEARIZABLE_READ_WARN: Client for key '{}' (reg_idx {}) disconnected or channel closed before reply could be sent.",
                                self.id, self.current_term, read_op.key, read_op.registered_commit_index
                            );
                        }
                    }
                } else {
                    if CORE_DETAILED_LOGS {
                        eprintln!(
                            "S{} T{} PROCESS_LINEARIZABLE_READ_ERROR: Reply channel for key '{}' was already taken or None.",
                            self.id, self.current_term, read_op.key
                        );
                    }
                }
                reads_processed_this_cycle += 1;
                false 
            } else {
                true 
            }
        });

            if reads_processed_this_cycle > 0 && CORE_DETAILED_LOGS {
                println!(
                    "S{} T{} PROCESS_LINEARIZABLE_READ_CYCLE_DONE: Processed {} reads. {} still pending.",
                    self.id,
                    self.current_term,
                    reads_processed_this_cycle,
                    self.pending_reads.len()
                );
            }
        } else {
            if !self.pending_reads.is_empty() {
                if CORE_DETAILED_LOGS {
                    println!(
                        "S{} T{} NOT_LEADER_FAIL_PENDING_READS: No longer leader. Failing {} pending read operations.",
                        self.id,
                        self.current_term,
                        self.pending_reads.len()
                    );
                }
                for mut read_op in self.pending_reads.drain(..) {
                    if let Some(sender) = read_op.reply_channel.take() {
                        let _ = sender.send(ClientReply::Error {
                            msg: "Leader changed or stepped down during read processing.".to_string(),
                        });
                    }
                }
            }
        }

        if self.should_snapshot() {
            if CORE_DETAILED_LOGS {
                println!(
                    "S{} T{} SNAPSHOT_TRIGGER: last_applied ({}) - last_snapshot_index ({}) > threshold.",
                    self.id, self.current_term, self.last_applied, self.last_snapshot_index
                );
            }
            let _ = self.take_snapshot();
        }

        if CORE_DETAILED_LOGS && (self.last_applied > 0 || !self.kv_store.is_empty()) {
            println!(
                "S{} T{} KV_STATE_AFTER_APPLY: KV store size {} after applying up to raft_idx {}.",
                self.id,
                self.current_term,
                self.kv_store.len(),
                self.last_applied
            );
        }
    }

    pub fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if CORE_DETAILED_LOGS {
            println!(
                "S{} T{} RV_RECV from C{}(T{}): state={:?}, last_log_idx={}, last_log_term={}",
                self.id,
                self.current_term,
                args.candidate_id,
                args.term,
                self.state,
                args.last_log_index,
                args.last_log_term
            );
        }

        if args.term < self.current_term {
            if CORE_DETAILED_LOGS {
                println!(
                    "S{} T{} RV_REJECT_OLD_TERM from C{}(T{}).",
                    self.id, self.current_term, args.candidate_id, args.term
                );
            }
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }

        if args.term > self.current_term {
            println!(
                "S{} T{} TERM_UPDATE_RV from C{}(T{}): Newer term (our T{} -> T{}). \
                Becoming Follower, clearing vote.",
                self.id,
                self.current_term,
                args.candidate_id,
                args.term,
                self.current_term,
                args.term
            );
            self.current_term = args.term;
            self.state = NodeState::Follower;
            self.voted_for = None;
            self.save_metadata();
            self.reset_election_timer();
        }

        let log_ok = args.last_log_term > self.log.last().map_or(0, |e| e.term)
            || (args.last_log_term == self.log.last().map_or(0, |e| e.term)
                && args.last_log_index >= self.log.len() as u64);

        let grant_vote =
            (self.voted_for.is_none() || self.voted_for == Some(args.candidate_id)) && log_ok;

        if grant_vote && self.current_term == args.term {
            println!(
                "S{} T{} RV_VOTE_GRANTED: To C{} for T{}.",
                self.id, self.current_term, args.candidate_id, self.current_term
            );
            self.voted_for = Some(args.candidate_id);
            if self.state != NodeState::Follower {
                self.state = NodeState::Follower;
            }
            self.save_metadata();
            self.reset_election_timer();
            RequestVoteReply {
                term: self.current_term,
                vote_granted: true,
            }
        } else {
            if CORE_DETAILED_LOGS {
                println!(
                    "S{} T{} RV_VOTE_REJECTED for C{}(T{}): OurT({}), ArgsT({}), \
                    PriorVote({:?}), CandLogUpToDate({}).",
                    self.id,
                    self.current_term,
                    args.candidate_id,
                    args.term,
                    self.current_term,
                    args.term,
                    self.voted_for,
                    log_ok
                );
            }
            RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            }
        }
    }

    pub fn handle_request_vote_reply(
        &mut self,
        reply: RequestVoteReply,
        from_peer_id: u64,
        total_servers: usize,
    ) -> Option<Vec<(u64, RpcMessage)>> {
        if CORE_DETAILED_LOGS {
            println!(
                "S{} T{} RV_REPLY from P{}(T{}): granted={}",
                self.id, self.current_term, from_peer_id, reply.term, reply.vote_granted
            );
        }

        if self.state != NodeState::Candidate {
            if CORE_DETAILED_LOGS {
                println!(
                    "S{} T{} IGNORE_VOTE_REPLY: Not candidate anymore",
                    self.id, self.current_term
                );
            }
            return None;
        }

        if reply.term > self.current_term {
            println!(
                "S{} T{} STEPPING_DOWN: Newer term {} from P{}",
                self.id, self.current_term, reply.term, from_peer_id
            );
            self.current_term = reply.term;
            self.state = NodeState::Follower;
            self.voted_for = None;
            self.save_metadata();
            self.reset_election_timer();
            return None;
        }

        if reply.vote_granted {
            self.votes_received.insert(from_peer_id);

            println!(
                "S{} T{} VOTE_COUNT: {}/{}",
                self.id,
                self.current_term,
                self.votes_received.len(),
                (total_servers / 2) + 1
            );

            if self.state == NodeState::Candidate && self.votes_received.len() > total_servers / 2 {
                println!(
                    "S{} T{} ELECTED_LEADER: With {} votes",
                    self.id,
                    self.current_term,
                    self.votes_received.len(),
                );

                self.state = NodeState::Leader;
                self.current_leader_id = Some(self.id);
                self.last_heartbeat = Instant::now();

                println!(
                    "S{} T{} BECOME_LEADER_CHECK: current_term={}, commit_index={}, log_len={}",
                    self.id, self.current_term, self.current_term, self.commit_index, self.log.len()
                );
                if self.commit_index > 0 {
                    let commit_log_entry_vec_idx_check = (self.commit_index - 1) as usize;
                    if commit_log_entry_vec_idx_check < self.log.len() {
                        println!(
                            "S{} T{} BECOME_LEADER_CHECK: Entry at (commit_index-1) has term: {}",
                            self.id, self.current_term, self.log[commit_log_entry_vec_idx_check].term
                        );
                    } else {
                        println!(
                            "S{} T{} BECOME_LEADER_CHECK: commit_index-1 ({}) is out of bounds for log_len {}.",
                            self.id, self.current_term, commit_log_entry_vec_idx_check, self.log.len()
                        );
                    }
                }

                let mut no_op_needed = true;
                if self.commit_index > 0 {
                    let commit_log_entry_vec_idx = (self.commit_index - 1) as usize;
                    if commit_log_entry_vec_idx < self.log.len() {
                        if self.log[commit_log_entry_vec_idx].term == self.current_term {
                            no_op_needed = false;
                        }
                    }
                }

                if no_op_needed {
                    println!(
                        "S{} T{} LEADER_PROPOSE_NOOP: Proposing NoOp to anchor current term.",
                        self.id, self.current_term
                    );
                    let no_op_entry = LogEntry {
                        term: self.current_term,
                        command: Command::NoOp,
                    };
                    self.log.push(no_op_entry);
                    self.persist_log_entry(self.log.len() - 1);
                }

                let current_leader_last_log_idx = self.log.len() as u64;
                let _current_leader_last_log_term = self.log.last().map_or(0, |e| e.term); // For reference

                self.peer_ids
                    .iter()
                    .filter(|&&p| p != self.id)
                    .for_each(|&p| {
                        self.next_index.insert(p, current_leader_last_log_idx + 1);
                        self.match_index.insert(p, 0);
                    });

                let mut messages = Vec::new();
                for &peer_id in &self.peer_ids {
                    if peer_id != self.id {
                        messages.push((
                            peer_id,
                            RpcMessage::AppendEntries(AppendEntriesArgs {
                                term: self.current_term,
                                leader_id: self.id,
                                prev_log_index: current_leader_last_log_idx,
                                prev_log_term: self.log.last().map_or(0, |e| e.term),
                                entries: Vec::new(),
                                leader_commit: self.commit_index,
                            }),
                        ));
                    }
                }
                return Some(messages);
            }
        }
        None
    }
}
