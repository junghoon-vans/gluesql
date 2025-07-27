use {
    gluesql_json_storage::JsonStorage,
    gluesql_memory_storage::MemoryStorage,
    gluesql_shared_memory_storage::SharedMemoryStorage,
    gluesql_sled_storage::SledStorage,
};

use crate::error::GlueSQLError;

#[derive(Clone)]
pub struct JavaMemoryStorage(pub MemoryStorage);

impl JavaMemoryStorage {
    pub fn new() -> Self {
        JavaMemoryStorage(MemoryStorage::default())
    }
}

impl Default for JavaMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

pub struct JavaJsonStorage(pub JsonStorage);

impl JavaJsonStorage {
    pub fn new(path: String) -> Result<Self, GlueSQLError> {
        let storage = JsonStorage::new(&path)
            .map_err(|e| GlueSQLError::new(format!("Failed to create JsonStorage: {}", e)))?;
        Ok(JavaJsonStorage(storage))
    }
}

impl Default for JavaJsonStorage {
    fn default() -> Self {
        Self::new("/tmp".to_string()).unwrap()
    }
}

pub struct JavaSledStorage(pub SledStorage);

impl JavaSledStorage {
    pub fn new(path: String) -> Result<Self, GlueSQLError> {
        let storage = SledStorage::new(&path)
            .map_err(|e| GlueSQLError::new(format!("Failed to create SledStorage: {}", e)))?;
        Ok(JavaSledStorage(storage))
    }
}

impl Default for JavaSledStorage {
    fn default() -> Self {
        Self::new("/tmp".to_string()).unwrap()
    }
}

#[derive(Clone)]
pub struct JavaSharedMemoryStorage(pub SharedMemoryStorage);

impl JavaSharedMemoryStorage {
    pub fn new(_namespace: String) -> Result<Self, GlueSQLError> {
        // SharedMemoryStorage::new() doesn't take any arguments
        let storage = SharedMemoryStorage::new();
        Ok(JavaSharedMemoryStorage(storage))
    }
}

impl Default for JavaSharedMemoryStorage {
    fn default() -> Self {
        Self::new("default".to_string()).unwrap()
    }
}

pub enum JavaStorageEngine {
    Memory(JavaMemoryStorage),
    Json(JavaJsonStorage),
    Sled(JavaSledStorage),
    SharedMemory(JavaSharedMemoryStorage),
}