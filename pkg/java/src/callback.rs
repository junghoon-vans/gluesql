use crate::error::JavaGlueSQLError;
use jni::{
    JNIEnv, JavaVM,
    objects::{GlobalRef, JObject},
};

// Callback data for async operations
pub struct CallbackData {
    pub jvm: JavaVM,
    pub callback: GlobalRef,
}

/// Handle Java callback for async query results
pub fn call_java_callback(callback_data: CallbackData, result: Result<String, JavaGlueSQLError>) {
    let env_result = callback_data.jvm.attach_current_thread();
    if let Ok(mut env) = env_result {
        let callback_obj = callback_data.callback.as_obj();

        match result {
            Ok(json_result) => {
                // Call onSuccess(String result)
                if let Ok(result_jstring) = env.new_string(&json_result) {
                    let _ = env.call_method(
                        callback_obj,
                        "onSuccess",
                        "(Ljava/lang/String;)V",
                        &[(&result_jstring).into()],
                    );
                }
            }
            Err(error) => {
                // Call onError(String error)
                call_error_callback(&mut env, callback_obj, &error.to_string());
            }
        }
    }
}

/// Handle error callback to Java
pub fn call_error_callback(env: &mut JNIEnv, callback: &JObject, message: &str) {
    if let Ok(error_str) = env.new_string(message) {
        let _ = env.call_method(
            callback,
            "onError",
            "(Ljava/lang/String;)V",
            &[(&error_str).into()],
        );
    }
}
