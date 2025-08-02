use {
    gluesql_json_storage::JsonStorage, gluesql_memory_storage::MemoryStorage,
    gluesql_redb_storage::RedbStorage, gluesql_shared_memory_storage::SharedMemoryStorage,
    gluesql_sled_storage::SledStorage,
};

use crate::error::JavaGlueSQLError;

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

#[derive(Clone)]
pub struct JavaSharedMemoryStorage(pub SharedMemoryStorage);

impl JavaSharedMemoryStorage {
    pub fn new() -> Self {
        JavaSharedMemoryStorage(SharedMemoryStorage::new())
    }
}

impl Default for JavaSharedMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

pub struct JavaJsonStorage(pub JsonStorage);

impl JavaJsonStorage {
    pub fn new(path: String) -> Result<Self, JavaGlueSQLError> {
        let storage = JsonStorage::new(&path)
            .map_err(|e| JavaGlueSQLError::new(format!("Failed to create JsonStorage: {}", e)))?;
        Ok(JavaJsonStorage(storage))
    }
}

pub struct JavaSledStorage(pub SledStorage);

impl JavaSledStorage {
    pub fn new(path: String) -> Result<Self, JavaGlueSQLError> {
        let storage = SledStorage::new(&path)
            .map_err(|e| JavaGlueSQLError::new(format!("Failed to create SledStorage: {}", e)))?;
        Ok(JavaSledStorage(storage))
    }
}

pub struct JavaRedbStorage(pub RedbStorage);

impl JavaRedbStorage {
    pub fn new(path: String) -> Result<Self, JavaGlueSQLError> {
        let storage = RedbStorage::new(&path)
            .map_err(|e| JavaGlueSQLError::new(format!("Failed to create RedbStorage: {}", e)))?;
        Ok(JavaRedbStorage(storage))
    }
}

pub enum JavaStorageEngine {
    Memory(JavaMemoryStorage),
    SharedMemory(JavaSharedMemoryStorage),
    Json(JavaJsonStorage),
    Sled(JavaSledStorage),
    Redb(JavaRedbStorage),
}
