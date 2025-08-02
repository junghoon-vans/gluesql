use {
    gluesql_json_storage::JsonStorage,
    gluesql_memory_storage::MemoryStorage,
    gluesql_shared_memory_storage::SharedMemoryStorage,
    gluesql_sled_storage::SledStorage,
    gluesql_redb_storage::RedbStorage
};

use crate::error::GlueSQLError;

#[derive(Clone)]
pub struct JavaMemoryStorage(pub MemoryStorage);

impl JavaMemoryStorage {
    pub fn new() -> Self {
        JavaMemoryStorage(MemoryStorage::default())
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

pub struct JavaSledStorage(pub SledStorage);

impl JavaSledStorage {
    pub fn new(path: String) -> Result<Self, GlueSQLError> {
        let storage = SledStorage::new(&path)
            .map_err(|e| GlueSQLError::new(format!("Failed to create SledStorage: {}", e)))?;
        Ok(JavaSledStorage(storage))
    }
}

#[derive(Clone)]
pub struct JavaSharedMemoryStorage(pub SharedMemoryStorage);

impl JavaSharedMemoryStorage {
    pub fn new() -> Self {
        JavaSharedMemoryStorage(SharedMemoryStorage::new())
    }
}

pub struct JavaRedbStorage(pub RedbStorage);

impl JavaRedbStorage {
    pub fn new(path: String) -> Result<Self, GlueSQLError> {
        let storage = RedbStorage::new(&path)
            .map_err(|e| GlueSQLError::new(format!("Failed to create RedbStorage: {}", e)))?;
        Ok(JavaRedbStorage(storage))
    }
}

pub enum JavaStorageEngine {
    Memory(JavaMemoryStorage),
    Json(JavaJsonStorage),
    Sled(JavaSledStorage),
    SharedMemory(JavaSharedMemoryStorage),
    Redb(JavaRedbStorage),
}
