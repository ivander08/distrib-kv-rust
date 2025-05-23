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
}

