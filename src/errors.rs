#[derive(Debug)]
pub enum KeyValueStoreError {
    ReadError(std::io::Error),
    WriteError(std::io::Error),
    InvalidRequest,
    KeyNotProvided,
    ValueNotProvided,
    DeserializeError(bincode::Error),
    SerializeError(bincode::Error),
}

impl std::error::Error for KeyValueStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            KeyValueStoreError::ReadError(e) => Some(e),
            KeyValueStoreError::WriteError(e) => Some(e),
            KeyValueStoreError::DeserializeError(e) => Some(e),
            KeyValueStoreError::SerializeError(e) => Some(e),
            _ => None,
        }
    }
}

impl std::fmt::Display for KeyValueStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyValueStoreError::ReadError(e) => write!(f, "Failed to read from socket: {}", e),
            KeyValueStoreError::WriteError(e) => write!(f, "Failed to write to socket: {}", e),
            KeyValueStoreError::InvalidRequest => write!(f, "Invalid request"),
            KeyValueStoreError::KeyNotProvided => write!(f, "Key not provided"),
            KeyValueStoreError::ValueNotProvided => write!(f, "Value not provided"),
            KeyValueStoreError::DeserializeError(e) => {
                write!(f, "Failed to deserialize data: {}", e)
            }
            KeyValueStoreError::SerializeError(e) => write!(f, "Failed to serialize data: {}", e),
        }
    }
}
