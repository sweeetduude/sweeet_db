use anyhow::{anyhow, Context, Result};
use bincode;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Semaphore;
use tokio::task;
use tokio::time::{timeout, Duration};
use tokio_util::codec::{BytesCodec, FramedRead};

#[derive(Serialize, Deserialize, Debug)]
struct KeyValueStore {
    store: HashMap<String, String>,
}

#[derive(Debug)]
enum WriteOperation {
    Set(String, String),
    Del(String),
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub max_connections: usize,
    pub timeout: u64,
    pub port: u16,
    pub storage_file_path: PathBuf,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_connections: 100,
            timeout: 15,
            port: 8080,
            storage_file_path: PathBuf::from("data.bin"),
        }
    }
}

#[derive(Debug)]
enum Command<'a> {
    Set(&'a str, &'a str),
    Get(&'a str),
    Del(&'a str),
}

fn parse_command(command_str: &str) -> Result<Command, String> {
    let mut parts = command_str.splitn(3, ' ');
    let command_type = parts.next().ok_or("Missing command type")?;

    match command_type.to_uppercase().as_str() {
        "SET" => {
            let key = parts.next().ok_or("Missing key for SET command")?;
            let value = parts.next().ok_or("Missing value for SET command")?;
            Ok(Command::Set(key, value))
        }
        "GET" => {
            let key = parts.next().ok_or("Missing key for GET command")?;
            Ok(Command::Get(key))
        }
        "DEL" => {
            let key = parts.next().ok_or("Missing key for DEL command")?;
            Ok(Command::Del(key))
        }
        _ => Err(format!("Invalid command type: {}", command_type)),
    }
}

// Handle a client's request by reading from the socket and calling the appropriate handler function.
// Returns an error if the request is invalid or an error occurs while processing the request.
async fn handle_client(
    mut stream: TcpStream,
    storage: Arc<AsyncMutex<KeyValueStore>>,
    write_tx: mpsc::Sender<WriteOperation>,
) -> Result<()> {
    let mut buffer = [0; 1024];

    // Read from the socket
    let n = stream
        .read(&mut buffer)
        .await
        .context("Failed to read from the socket")?;

    // Extract the request
    let request = String::from_utf8_lossy(&buffer[..n]).trim().to_owned();

    // Match the command and call the appropriate handler function
    match parse_command(&request) {
        Ok(Command::Set(key, value)) => {
            handle_set(&key, &value, &storage, &mut stream, &write_tx).await
        }
        Ok(Command::Get(key)) => handle_get(&key, &storage, &mut stream).await,
        Ok(Command::Del(key)) => handle_del(&key, &storage, &mut stream, &write_tx).await,
        Err(err_msg) => {
            let response = b"INVALID REQUEST\n";
            stream
                .write_all(response)
                .await
                .context("Error writing response")?;
            Err(anyhow!(err_msg))
        }
    }
}

// Handle the 'del' operation by removing the specified key from the storage.
// Responds with 'OK' if the key is removed or 'NOT FOUND' if the key is not in the storage.
// If the operation is successful, it also writes the updated storage to the file.
async fn handle_del(
    key: &str,
    storage: &Arc<AsyncMutex<KeyValueStore>>,
    stream: &mut TcpStream,
    write_tx: &mpsc::Sender<WriteOperation>,
) -> Result<()> {
    // Lock the storage for exclusive access
    let mut storage = storage.lock().await;

    // Attempt to remove the key from the storage
    if storage.store.remove(key).is_some() {
        // Respond with 'OK' if the key is removed
        let response = b"OK\n";
        stream
            .write_all(response)
            .await
            .context("Error writing response")?;

        // Write the updated storage to the background task
        if let Err(e) = write_tx.send(WriteOperation::Del(key.to_owned())).await {
            println!("Failed to send write operation to background task: {}", e);
        }
        Ok(())
    } else {
        // Respond with 'NOT FOUND' if the key is not in the storage
        let response = b"NOT FOUND\n";
        stream
            .write_all(response)
            .await
            .context("Error writing response")?;

        Ok(())
    }
}

// Handle the 'get' operation by retrieving the value for the specified key from the storage.
// Responds with 'OK <value>' if the key is found or 'NOT FOUND' if the key is not in the storage.
async fn handle_get(
    key: &str,
    storage: &Arc<AsyncMutex<KeyValueStore>>,
    stream: &mut TcpStream,
) -> Result<()> {
    // Lock the storage for shared access
    let storage = storage.lock().await;

    // Attempt to get the value for the key from the storage
    if let Some(value) = storage.store.get(key) {
        // Respond with 'OK <value>' if the key is found
        let response = format!("{}\n", value);
        stream
            .write_all(response.as_bytes())
            .await
            .context("Error writing response")?;

        Ok(())
    } else {
        // Respond with 'NOT FOUND' if the key is not in the storage
        let response = b"NOT FOUND\n";
        stream
            .write_all(response)
            .await
            .context("Error writing response")?;

        Ok(())
    }
}

// Handle the 'set' operation by adding or updating the specified key-value pair in the storage.
// Responds with 'OK' if the key-value pair is added or updated.
// If the operation is successful, it also writes the updated storage to the file.
async fn handle_set(
    key: &str,
    value: &str,
    storage: &Arc<AsyncMutex<KeyValueStore>>,
    stream: &mut TcpStream,
    write_tx: &mpsc::Sender<WriteOperation>,
) -> Result<()> {
    // Lock the storage for exclusive access
    let mut storage = storage.lock().await;

    // Insert or update the key-value pair in the storage
    storage.store.insert(key.to_owned(), value.to_string());

    // Respond with 'OK' if the key-value pair is added or updated
    let response = b"OK\n";
    stream
        .write_all(response)
        .await
        .context("Error writing response")?;

    // Write the updated storage to the background task
    if let Err(e) = write_tx
        .send(WriteOperation::Set(key.to_owned(), value.to_string()))
        .await
    {
        println!("Failed to send write operation to background task: {}", e);
    }
    Ok(())
}

// Load data from the file, returning an empty HashMap if the file does not exist or is empty.
// Returns an error if there is a problem reading the file or deserializing the contents.
async fn load_data(storage_file_path: &PathBuf) -> Result<HashMap<String, String>> {
    match File::open(storage_file_path).await {
        Ok(file) => read_from_file(file).await,
        Err(_) => Ok(HashMap::new()),
    }
}

// Read data from the specified file, returning an empty HashMap if the file is empty.
// Returns an error if there is a problem deserializing the contents of the file.
async fn read_from_file(file: File) -> Result<HashMap<String, String>> {
    let mut framed_read = FramedRead::new(file, BytesCodec::new());
    if let Some(Ok(buf)) = framed_read.next().await {
        match bincode::deserialize::<HashMap<String, String>>(&buf) {
            Ok(decoded) => Ok(decoded),
            Err(_) => Err(anyhow!("Error deserializing the content")),
        }
    } else {
        Ok(HashMap::new())
    }
}

// Write the specified HashMap to the file, overwriting any existing data.
// Returns an error if there is a problem opening the file, serializing the data, or writing to the file.
async fn write_to_file(store: &HashMap<String, String>, storage_file_path: &PathBuf) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(storage_file_path)
        .await
        .context("Error opening file")?;

    let mut buffered_writer = BufWriter::new(file);

    let serialized_data = bincode::serialize(&store).context("Error serializing data")?;
    buffered_writer
        .write_all(&serialized_data)
        .await
        .context("Error writing to file")?;
    buffered_writer
        .flush()
        .await
        .context("Error writing to file")?;

    Ok(())
}

#[tokio::main]
async fn main() {
    // Create server configuration
    let config = ServerConfig::default();

    // Run the server and handle any errors that may occur.
    // If the server shuts down gracefully, print a message to inform the user.
    // If there is an error during server execution, print the error message.
    match run_server(&config).await {
        Ok(()) => println!("Server shut down gracefully."),
        Err(e) => eprintln!("Server error: {}", e),
    }
}

// Background task to synchronize write operations with the file storage
async fn file_storage_sync(mut write_rx: mpsc::Receiver<WriteOperation>, config: ServerConfig) {
    // Load the initial data from the file
    let mut file_storage = load_data(&config.storage_file_path)
        .await
        .unwrap_or_else(|_| HashMap::new());

    // Process write operations from the queue
    while let Some(operation) = write_rx.recv().await {
        match operation {
            WriteOperation::Set(key, value) => {
                file_storage.insert(key, value);
            }
            WriteOperation::Del(key) => {
                file_storage.remove(&key);
            }
        }

        // Write the updated storage to the file
        if let Err(e) = write_to_file(&file_storage, &config.storage_file_path).await {
            println!("Failed to write to file: {}", e);
        }
    }
}

async fn run_server(config: &ServerConfig) -> Result<()> {
    // Create a channel for write operations
    let (write_tx, write_rx) = mpsc::channel::<WriteOperation>(100);

    // Spawn the background task for synchronization
    task::spawn(file_storage_sync(write_rx, config.clone()));

    // Bind the TcpListener to a local IP address and port.
    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).await?;

    // Create an asynchronous Mutex-protected KeyValueStore, initialized with data loaded from file.
    let storage = Arc::new(AsyncMutex::new(KeyValueStore {
        store: load_data(&config.storage_file_path).await?,
    }));

    // Create a semaphore to limit the number of concurrent client connections.
    let connection_semaphore = Arc::new(Semaphore::new(config.max_connections));

    // Enter an infinite loop to listen for incoming client connections.
    loop {
        // Accept an incoming connection and get the TcpStream and the client's address.
        let (stream, _) = listener
            .accept()
            .await
            .context("Could not get the client")?;

        // Clone the Arc storage and connection_semaphore to share them safely among multiple tasks.
        let storage = storage.clone();
        let connection_semaphore = connection_semaphore.clone();
        let write_tx = write_tx.clone();
        let config = config.clone();

        // Spawn a new asynchronous task to handle the client connection..
        task::spawn(async move {
            // Acquire a permit from the semaphore. If all permits are in use, this will
            // asynchronously wait until a permit becomes available.
            let permit = connection_semaphore.acquire().await;

            // Wrap the handle_client call with a timeout of 5 seconds.
            match timeout(
                Duration::from_secs(config.timeout),
                handle_client(stream, storage, write_tx),
            )
            .await
            {
                Ok(result) => {
                    if let Err(e) = result {
                        println!("Error handling client: {:?}", e);
                    }
                }
                Err(_) => {
                    println!(
                        "Client connection timed out after {} seconds",
                        config.timeout
                    );
                }
            }

            // Drop the permit, allowing another connection to be processed.
            drop(permit);
        });
    }
}
