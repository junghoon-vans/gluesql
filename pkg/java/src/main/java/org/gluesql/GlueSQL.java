package org.gluesql;

/**
 * GlueSQL Java bindings - Main interface for interacting with GlueSQL database.
 * 
 * GlueSQL is an open-source SQL database engine written in Rust that supports
 * multiple storage backends and provides SQL query capabilities.
 */
public class GlueSQL {
    
    static {
        System.loadLibrary("gluesql_java");
    }

    private long nativeHandle;

    private GlueSQL(long handle) {
        this.nativeHandle = handle;
    }

    /**
     * Create a new GlueSQL instance with in-memory storage.
     * 
     * @return GlueSQL instance with memory storage
     */
    public static GlueSQL newMemory() throws GlueSQLException {
        long handle = nativeNewMemory();
        if (handle == 0) {
            throw new GlueSQLException("Failed to create memory storage");
        }
        return new GlueSQL(handle);
    }

    /**
     * Create a new GlueSQL instance with Sled storage.
     * 
     * @param path Path to the Sled database directory
     * @return GlueSQL instance with Sled storage
     * @throws GlueSQLException if storage creation fails
     */
    public static GlueSQL newSled(String path) throws GlueSQLException {
        long handle = nativeNewSled(path);
        if (handle == 0) {
            throw new GlueSQLException("Failed to create Sled storage at path: " + path);
        }
        return new GlueSQL(handle);
    }

    /**
     * Create a new GlueSQL instance with JSON storage.
     * 
     * @param path Path to the JSON storage directory
     * @return GlueSQL instance with JSON storage
     * @throws GlueSQLException if storage creation fails
     */
    public static GlueSQL newJson(String path) throws GlueSQLException {
        long handle = nativeNewJson(path);
        if (handle == 0) {
            throw new GlueSQLException("Failed to create JSON storage at path: " + path);
        }
        return new GlueSQL(handle);
    }

    /**
     * Create a new GlueSQL instance with shared memory storage.
     * 
     * @param namespace Namespace for the shared memory storage
     * @return GlueSQL instance with shared memory storage
     * @throws GlueSQLException if storage creation fails
     */
    public static GlueSQL newSharedMemory(String namespace) throws GlueSQLException {
        long handle = nativeNewSharedMemory(namespace);
        if (handle == 0) {
            throw new GlueSQLException("Failed to create shared memory storage with namespace: " + namespace);
        }
        return new GlueSQL(handle);
    }

    /**
     * Execute a SQL query and return the results as JSON string.
     * 
     * @param sql SQL query string to execute
     * @return JSON string containing query results
     * @throws GlueSQLException if query execution fails
     */
    public String query(String sql) throws GlueSQLException {
        if (nativeHandle == 0) {
            throw new GlueSQLException("Database instance has been closed");
        }
        return nativeQuery(nativeHandle, sql);
    }

    /**
     * Executes a SQL query and returns a QueryResult object for easier handling.
     * 
     * @param sql SQL query string to execute
     * @return QueryResult object containing parsed results
     * @throws GlueSQLException if query execution fails
     */
    public QueryResult execute(String sql) throws GlueSQLException {
        String jsonResult = query(sql);
        return QueryResult.fromJson(jsonResult);
    }

    /**
     * Close the database and free native resources.
     */
    public void close() {
        if (nativeHandle != 0) {
            // Note: In a full implementation, we would add a native method to free the handle
            nativeHandle = 0;
        }
    }

    /**
     * Free native resources when the object is garbage collected.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
        }
    }

    // Native method declarations
    private static native long nativeNewMemory();
    private static native long nativeNewSled(String path);
    private static native long nativeNewJson(String path);
    private static native long nativeNewSharedMemory(String namespace);
    private native String nativeQuery(long handle, String sql) throws GlueSQLException;
}
