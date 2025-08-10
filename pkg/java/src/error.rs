use jni::JNIEnv;

#[derive(Debug, Clone)]
pub struct JavaGlueSQLError {
    pub message: String,
}

impl JavaGlueSQLError {
    pub fn new(message: String) -> Self {
        JavaGlueSQLError { message }
    }
    pub fn throw_to_java(self, env: &mut JNIEnv) {
        let _ = env.throw_new("org/gluesql/GlueSQLException", &self.message);
    }
}

impl std::fmt::Display for JavaGlueSQLError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for JavaGlueSQLError {}
