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

}

