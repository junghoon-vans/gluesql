use {
    gluesql_core::prelude::{execute, parse, translate},
    jni::{
        JNIEnv,
        objects::{JClass, JObject, JString},
        sys::jlong,
    },
    std::sync::{Arc, RwLock},
    tokio::runtime::Runtime,
};

mod callback;
mod error;
mod payload;
mod storages;

use {
    callback::{CallbackData, call_java_callback},
    error::JavaGlueSQLError,
    payload::convert,
    storages::{
        JavaJsonStorage, JavaMemoryStorage, JavaRedbStorage, JavaSharedMemoryStorage,
        JavaSledStorage, JavaStorageEngine,
    },
};

pub struct JavaGlue {
    pub storage: Arc<RwLock<JavaStorageEngine>>,
    pub runtime: Arc<Runtime>,
}

macro_rules! execute {
    ($storage:expr, $statements:expr) => {{
        execute(&mut $storage.0, $statements)
            .await
            .map_err(|e| JavaGlueSQLError::new(e.to_string()))
    }};
}

impl JavaGlue {
    pub fn new(storage: JavaStorageEngine) -> Result<Self, JavaGlueSQLError> {
        let runtime = Runtime::new()
            .map_err(|e| JavaGlueSQLError::new(format!("Failed to create Tokio runtime: {}", e)))?;
        Ok(JavaGlue {
            storage: Arc::new(RwLock::new(storage)),
            runtime: Arc::new(runtime),
        })
    }

    pub async fn query_async_internal(&self, sql: String) -> Result<String, JavaGlueSQLError> {
        let queries = parse(&sql).map_err(|e| JavaGlueSQLError::new(e.to_string()))?;

        let mut payloads = Vec::new();

        for query in queries.iter() {
            let statement = translate(query).map_err(|e| JavaGlueSQLError::new(e.to_string()))?;

            // Execute each statement individually and immediately release the lock
            let payload = self.execute_single_statement(&statement).await?;
            payloads.push(payload);
        }

        convert(payloads).map_err(|e| JavaGlueSQLError::new(e.to_string()))
    }

    #[allow(clippy::await_holding_lock)]
    async fn execute_single_statement(
        &self,
        statement: &gluesql_core::ast::Statement,
    ) -> Result<gluesql_core::executor::Payload, JavaGlueSQLError> {
        let mut storage_guard = self
            .storage
            .write()
            .map_err(|_| JavaGlueSQLError::new("Failed to acquire storage lock".to_string()))?;

        let result = match &mut *storage_guard {
            JavaStorageEngine::Memory(s) => execute!(s, statement),
            JavaStorageEngine::Json(s) => execute!(s, statement),
            JavaStorageEngine::Sled(s) => execute!(s, statement),
            JavaStorageEngine::SharedMemory(s) => execute!(s, statement),
            JavaStorageEngine::Redb(s) => execute!(s, statement),
        };

        // Lock is released here when storage_guard goes out of scope
        drop(storage_guard);

        result
    }

    pub fn query_async(&self, sql: String, callback_data: CallbackData) {
        let glue_clone = JavaGlue {
            storage: Arc::clone(&self.storage),
            runtime: Arc::clone(&self.runtime),
        };

        let runtime = Arc::clone(&self.runtime);

        std::thread::spawn(move || {
            let result = runtime.block_on(glue_clone.query_async_internal(sql));
            call_java_callback(callback_data, result);
        });
    }
}

fn handle_storage_creation_error() -> jlong {
    0
}

// JNI exports
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_gluesql_GlueSQL_nativeNewMemory(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let storage = JavaStorageEngine::Memory(JavaMemoryStorage::new());
    match JavaGlue::new(storage) {
        Ok(glue) => Box::into_raw(Box::new(glue)) as jlong,
        Err(_) => handle_storage_creation_error(),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_gluesql_GlueSQL_nativeNewSharedMemory(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let storage = JavaStorageEngine::SharedMemory(JavaSharedMemoryStorage::new());
    match JavaGlue::new(storage) {
        Ok(glue) => Box::into_raw(Box::new(glue)) as jlong,
        Err(_) => handle_storage_creation_error(),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_gluesql_GlueSQL_nativeNewSled(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
) -> jlong {
    let path_str: String = match env.get_string(&path) {
        Ok(jstr) => jstr.into(),
        Err(_) => return handle_storage_creation_error(),
    };

    match JavaSledStorage::new(path_str) {
        Ok(storage) => {
            let storage = JavaStorageEngine::Sled(storage);
            match JavaGlue::new(storage) {
                Ok(glue) => Box::into_raw(Box::new(glue)) as jlong,
                Err(_) => handle_storage_creation_error(),
            }
        }
        Err(_) => handle_storage_creation_error(),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_gluesql_GlueSQL_nativeNewJson(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
) -> jlong {
    let path_str: String = match env.get_string(&path) {
        Ok(jstr) => jstr.into(),
        Err(_) => return handle_storage_creation_error(),
    };

    match JavaJsonStorage::new(path_str) {
        Ok(storage) => {
            let storage = JavaStorageEngine::Json(storage);
            match JavaGlue::new(storage) {
                Ok(glue) => Box::into_raw(Box::new(glue)) as jlong,
                Err(_) => handle_storage_creation_error(),
            }
        }
        Err(_) => handle_storage_creation_error(),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_gluesql_GlueSQL_nativeNewRedb(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
) -> jlong {
    let path_str: String = match env.get_string(&path) {
        Ok(jstr) => jstr.into(),
        Err(_) => return handle_storage_creation_error(),
    };

    match JavaRedbStorage::new(path_str) {
        Ok(storage) => {
            let storage = JavaStorageEngine::Redb(storage);
            match JavaGlue::new(storage) {
                Ok(glue) => Box::into_raw(Box::new(glue)) as jlong,
                Err(_) => handle_storage_creation_error(),
            }
        }
        Err(_) => handle_storage_creation_error(),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_gluesql_GlueSQL_nativeQueryAsync(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    sql: JString,
    callback: JObject,
) {
    // SAFETY: handle is guaranteed to be a valid pointer to JavaGlue
    let glue = unsafe { &*(handle as *mut JavaGlue) };

    let sql_str: String = match env.get_string(&sql) {
        Ok(jstr) => jstr.into(),
        Err(_) => {
            // Call onError on callback if string conversion fails
            callback::call_error_callback(&mut env, &callback, "Failed to parse SQL string");
            return;
        }
    };

    // Get JavaVM for callback
    let jvm = match env.get_java_vm() {
        Ok(vm) => vm,
        Err(_) => {
            // Call onError on callback if JavaVM retrieval fails
            callback::call_error_callback(&mut env, &callback, "Failed to get JavaVM");
            return;
        }
    };

    // Create global reference to callback
    let global_callback = match env.new_global_ref(&callback) {
        Ok(global_ref) => global_ref,
        Err(_) => {
            // Call onError on callback if global ref creation fails
            callback::call_error_callback(&mut env, &callback, "Failed to create global reference");
            return;
        }
    };

    let callback_data = CallbackData {
        jvm,
        callback: global_callback,
    };

    // Execute async query
    glue.query_async(sql_str, callback_data);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_gluesql_GlueSQL_nativeFree(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    if handle != 0 {
        // SAFETY: handle is guaranteed to be a valid pointer to JavaGlue
        // that was created by one of the nativeNew* functions
        let _boxed = unsafe { Box::from_raw(handle as *mut JavaGlue) };
        // Box is automatically dropped here, freeing the memory
    }
}
