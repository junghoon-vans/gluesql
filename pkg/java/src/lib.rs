use {
    gluesql_core::prelude::{execute, parse, translate},
    jni::{
        JNIEnv,
        objects::{JClass, JString, JObject},
        sys::{jlong, jstring},
    },
    std::sync::{Arc, Mutex},
};

mod error;
mod payload;
mod storages;

use {
    error::GlueSQLError,
    payload::convert,
    storages::{
        JavaMemoryStorage, JavaJsonStorage, JavaSledStorage, 
        JavaSharedMemoryStorage, JavaStorageEngine,
    },
};

pub struct JavaGlue {
    pub storage: Arc<Mutex<JavaStorageEngine>>,
}

macro_rules! execute {
    ($storage:expr, $statements:expr) => {{
        execute(&mut $storage.0, $statements)
            .await
            .map_err(|e| GlueSQLError::new(e.to_string()))
    }};
}

impl JavaGlue {
    pub fn new(storage: JavaStorageEngine) -> Self {
        JavaGlue {
            storage: Arc::new(Mutex::new(storage))
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Runtime::new().unwrap()
    }

    pub fn query(&self, sql: String) -> Result<String, GlueSQLError> {
        let rt = Self::runtime();
        
        rt.block_on(async {
            let queries = parse(&sql)
                .map_err(|e| GlueSQLError::new(e.to_string()))?;

            let mut payloads = Vec::new();
            
            for query in queries.iter() {
                let statement = translate(query)
                    .map_err(|e| GlueSQLError::new(e.to_string()))?;

                let mut storage = self.storage.lock().unwrap();
                let result = match &mut *storage {
                    JavaStorageEngine::Memory(storage) => execute!(storage, &statement),
                    JavaStorageEngine::Json(storage) => execute!(storage, &statement),
                    JavaStorageEngine::Sled(storage) => execute!(storage, &statement),
                    JavaStorageEngine::SharedMemory(storage) => execute!(storage, &statement),
                };

                match result {
                    Ok(payload) => {
                        payloads.push(payload);
                    }
                    Err(e) => return Err(GlueSQLError::new(e.to_string()))
                }
            }

            convert(payloads).map_err(|e| GlueSQLError::new(e.to_string()))
        })
    }
}

// JNI exports
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_gluesql_GlueSQL_nativeNewMemory(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let storage = JavaStorageEngine::Memory(JavaMemoryStorage::new());
    let glue = JavaGlue::new(storage);
    Box::into_raw(Box::new(glue)) as jlong
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_gluesql_GlueSQL_nativeNewSled(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
) -> jlong {
    let path_str: String = match env.get_string(&path) {
        Ok(jstr) => jstr.into(),
        Err(_) => return 0,
    };
    
    match JavaSledStorage::new(path_str) {
        Ok(storage) => {
            let storage = JavaStorageEngine::Sled(storage);
            let glue = JavaGlue::new(storage);
            Box::into_raw(Box::new(glue)) as jlong
        }
        Err(_) => 0,
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
        Err(_) => return 0,
    };
    
    match JavaJsonStorage::new(path_str) {
        Ok(storage) => {
            let storage = JavaStorageEngine::Json(storage);
            let glue = JavaGlue::new(storage);
            Box::into_raw(Box::new(glue)) as jlong
        }
        Err(_) => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_gluesql_GlueSQL_nativeNewSharedMemory(
    mut env: JNIEnv,
    _class: JClass,
    namespace: JString,
) -> jlong {
    let _namespace_str: String = match env.get_string(&namespace) {
        Ok(jstr) => jstr.into(),
        Err(_) => return 0,
    };
    
    match JavaSharedMemoryStorage::new(_namespace_str) {
        Ok(storage) => {
            let storage = JavaStorageEngine::SharedMemory(storage);
            let glue = JavaGlue::new(storage);
            Box::into_raw(Box::new(glue)) as jlong
        }
        Err(_) => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_gluesql_GlueSQL_nativeQuery(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    sql: JString,
) -> jstring {
    let glue = unsafe { &*(handle as *mut JavaGlue) };
    let sql_str: String = match env.get_string(&sql) {
        Ok(jstr) => jstr.into(),
        Err(_) => {
            let _ = env.throw_new("org/gluesql/GlueSQLException", "Failed to parse SQL string");
            return JObject::null().into_raw();
        }
    };
    
    match glue.query(sql_str) {
        Ok(result) => {
            match env.new_string(result) {
                Ok(jstr) => jstr.into_raw(),
                Err(_) => JObject::null().into_raw(),
            }
        }
        Err(e) => {
            let _ = env.throw_new("org/gluesql/GlueSQLException", e.message);
            JObject::null().into_raw()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_gluesql_GlueSQL_nativeFree(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    if handle != 0 {
        let _boxed = unsafe { Box::from_raw(handle as *mut JavaGlue) };
        // Box is automatically dropped here, freeing the memory
    }
}
