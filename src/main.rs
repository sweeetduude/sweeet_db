use anyhow::{anyhow, Context, Result};
use bincode;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
            timeout: 120,
            port: 8080,
            storage_file_path: PathBuf::from("data.bin"),
        }
    }
}

#[derive(Debug)]
enum WriteOperation {
    Set(String, String),
    Del(String),
}

#[derive(Serialize, Deserialize, Debug)]
struct KeyValueStore {
    store: HashMap<String, String>,
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
        let ping_timer = config.timeout.clone();

        // Spawn a new asynchronous task to handle the client connection..
        task::spawn(async move {
            // Acquire a permit from the semaphore. If all permits are in use, this will
            // asynchronously wait until a permit becomes available.
            let permit = connection_semaphore.acquire().await;

            // Removed the timeout wrapper, calling handle_client directly
            if let Err(e) = handle_client(stream, storage, write_tx, ping_timer).await {
                println!("Error handling client: {:?}", e);
            }

            // Drop the permit, allowing another connection to be processed.
            drop(permit);
        });
    }
}

// Handle a client's request by reading from the socket and calling the appropriate handler function.
// Returns an error if the request is invalid or an error occurs while processing the request.
async fn handle_client(
    mut stream: TcpStream,
    storage: Arc<AsyncMutex<KeyValueStore>>,
    write_tx: mpsc::Sender<WriteOperation>,
    ping_timer: u64,
) -> Result<()> {
    let time_elapsed = Arc::new(AtomicU64::new(0));
    let stop_client = Arc::new(AtomicBool::new(false));
    let time_elapsed_clone = time_elapsed.clone();
    let stop_client_clone = stop_client.clone();

    // Require client to send a ping every x seconds. stop_client will break the socket look
    task::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let elapsed = time_elapsed_clone.load(Ordering::SeqCst) + 1;
            if elapsed > ping_timer {
                stop_client_clone.store(true, Ordering::SeqCst);
                break;
            }
            time_elapsed_clone.store(elapsed, Ordering::SeqCst);
        }
    });

    let mut buffer = [0; 1024];

    loop {
        // Close connection if Arc variable stop clint is called
        if stop_client.load(Ordering::SeqCst) {
            break;
        }

        // Read from the socket every 500ms
        let n = match timeout(Duration::from_secs(5), stream.read(&mut buffer)).await {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => {
                return Err(anyhow!(e));
            }
            Err(_) => {
                continue;
            }
        };

        // If n is 0, the client closed the connection
        if n == 0 {
            break;
        }

        // Extract the request
        let request = String::from_utf8_lossy(&buffer[..n]).trim().to_string();

        /**** END OF CLIENT ****/

        // Call the appropriate handler function directly
        match handle_request(&request, &storage, &mut stream, &write_tx, &time_elapsed).await {
            Ok(()) => {}
            Err(e) => {
                let response = format!("INVALID REQUEST: {}\n", e);
                stream
                    .write_all(response.as_bytes())
                    .await
                    .context("Error writing response")?;
            }
        }
    }
    Ok(())
}

/**** COMMAND MODULE ****/

async fn handle_request(
    request_str: &str,
    storage: &Arc<AsyncMutex<KeyValueStore>>,
    stream: &mut TcpStream,
    write_tx: &mpsc::Sender<WriteOperation>,
    time_elapsed: &Arc<AtomicU64>,
) -> Result<()> {
    let mut parts = request_str.splitn(2, ' ');

    let command_type = match parts.next() {
        Some(command) => command,
        None => return Err(anyhow!("Invalid command type")),
    };

    match command_type.to_uppercase().as_str() {
        "GET" => handle_get(request_str, storage, stream).await,
        "SET" => handle_set(request_str, storage, stream, write_tx).await,
        "DEL" => handle_del(request_str, storage, stream, write_tx).await,

        "PING" => {
            time_elapsed.store(0, Ordering::SeqCst);
            let response = b"PONG\n";
            stream
                .write_all(response)
                .await
                .context("Error writing response")?;
            Ok(())
        }
        _ => Err(anyhow!("Invalid command type")),
    }
}

// Handle the 'set' operation by adding or updating the specified key-value pair in the storage.
// Responds with 'OK' if the key-value pair is added or updated.
// If the operation is successful, it also writes the updated storage to the file.
async fn handle_set(
    request_str: &str,
    storage: &Arc<AsyncMutex<KeyValueStore>>,
    stream: &mut TcpStream,
    write_tx: &mpsc::Sender<WriteOperation>,
) -> Result<()> {
    let mut parts = request_str.splitn(3, ' ');

    let key = match parts.nth(1) {
        Some(key) => key,
        None => return Err(anyhow!("Invalid command type")),
    };

    let value = match parts.next() {
        Some(value) => value,
        None => return Err(anyhow!("Invalid command type")),
    };

    // Lock the storage for exclusive access
    let mut storage = storage.lock().await;

    // Insert or update the key-value pair in the storage
    storage.store.insert(key.to_string(), value.to_string());

    // Respond with 'OK' if the key-value pair is added or updated
    let response = b"OK\n";
    stream
        .write_all(response)
        .await
        .context("Error writing response")?;

    write_tx
        .send(WriteOperation::Set(key.to_string(), value.to_string()))
        .await
        .context("Failed to send write operation to background task")?;

    Ok(())
}

// Handle the 'get' operation by retrieving the value for the specified key from the storage.
// Responds with 'OK <value>' if the key is found or 'NOT FOUND' if the key is not in the storage.
async fn handle_get(
    //key: &str,
    request_str: &str,
    storage: &Arc<AsyncMutex<KeyValueStore>>,
    stream: &mut TcpStream,
) -> Result<()> {
    let mut parts = request_str.splitn(3, ' ');

    let key = match parts.nth(1) {
        Some(key) => key,
        None => return Err(anyhow!("Invalid command type")),
    };
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

// Handle the 'del' operation by removing the specified key from the storage.
// Responds with 'OK' if the key is removed or 'NOT FOUND' if the key is not in the storage.
// If the operation is successful, it also writes the updated storage to the file.
async fn handle_del(
    //key: &str,
    request_str: &str,
    storage: &Arc<AsyncMutex<KeyValueStore>>,
    stream: &mut TcpStream,
    write_tx: &mpsc::Sender<WriteOperation>,
) -> Result<()> {
    let mut parts = request_str.splitn(3, ' ');

    let key = match parts.nth(1) {
        Some(key) => key,
        None => return Err(anyhow!("Invalid command type")),
    };
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
        write_tx.send(WriteOperation::Del(key.to_string())).await?;

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

/**** STORAGE MODULE ****/

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
