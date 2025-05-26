// src/main.rs
mod core_types;

// We'll need these for actual server logic later, keep them for now
use crate::core_types::{
    Server, // RpcMessage, NodeState, Command, LogEntry,
    // AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply,
};
// Tokio specific imports
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt}; // For reading/writing to streams
use std::error::Error; // For basic error handling

// Our main function now becomes an async function executed by Tokio
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> { // main can return a Result for error handling
    println!("--- Distributed KV Store Server Starting (with Tokio) ---");

    // Configuration for our first server
    let server_id: u64 = 1;
    let listen_addr = "127.0.0.1:8081".to_string(); // Address for this server to listen on
    
    // For now, peer_ids will be empty as this server won't connect out yet in this step
    let peer_ids: Vec<u64> = vec![2, 3]; // Example peers it *would* talk to later

    // Create the server instance (we'll make it mutable when it handles state)
    let _server_logic = Server::new(server_id, peer_ids); 
    // The `_server_logic` will be used inside connection handling tasks later.
    // For now, it's unused in this minimal example.

    println!("[Server {}] Attempting to listen on {}...", server_id, listen_addr);

    // 1. Create a TCP listener
    let listener = TcpListener::bind(&listen_addr).await?;
    println!("[Server {}] Successfully listening on {}", server_id, listen_addr);

    // 2. Loop to accept incoming connections
    loop {
        // Asynchronously wait for an inbound connection.
        // `accept()` returns a tuple: (socket, address_of_peer)
        match listener.accept().await {
            Ok((socket, addr)) => {
                println!("[Server {}] New connection from: {}", server_id, addr);
                
                // For each connection, spawn a new Tokio task to handle it.
                // This allows the server to handle multiple clients concurrently.
                tokio::spawn(async move {
                    // `socket` is the TcpStream for this specific connection.
                    // We would read messages, deserialize, pass to server_logic,
                    // get reply, serialize, and send back.
                    // For now, let's just try to read a few bytes and echo them back.
                    handle_connection(socket, server_id).await;
                });
            }
            Err(e) => {
                eprintln!("[Server {}] Failed to accept connection: {:?}", server_id, e);
            }
        }
    }
    // Ok(()) // Main loop runs forever, so this might not be reached in this simple form
}

// A simple handler for each connection
async fn handle_connection(mut stream: TcpStream, server_id: u64) {
    println!("[S{}] Handling new stream: {:?}", server_id, stream.peer_addr().unwrap());
    let mut buffer = [0; 1024]; // A 1KB buffer

    loop {
        // Try to read data from the stream
        match stream.read(&mut buffer).await {
            Ok(0) => {
                // 0 bytes read means the connection was closed by the client
                println!("[S{}] Connection closed by peer {:?}", server_id, stream.peer_addr().unwrap());
                return;
            }
            Ok(n) => {
                // We received n bytes. For now, let's just print them as a string (if they are UTF-8)
                // and echo them back.
                let received_data = String::from_utf8_lossy(&buffer[0..n]);
                println!("[S{}] Received {} bytes: {}", server_id, n, received_data);

                // Echo the data back
                if let Err(e) = stream.write_all(&buffer[0..n]).await {
                    eprintln!("[S{}] Failed to write to socket: {:?}", server_id, e);
                    return;
                }
                println!("[S{}] Echoed back {} bytes.", server_id, n);
            }
            Err(e) => {
                // An error occurred during read
                eprintln!("[S{}] Failed to read from socket: {:?}", server_id, e);
                return;
            }
        }
    }
}