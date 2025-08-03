package org.gluesql;

import java.util.concurrent.CompletableFuture;

/**
 * GlueSQL Java bindings - Main interface for interacting with GlueSQL database.
 * <p>
 * GlueSQL is an open-source SQL database engine written in Rust that supports
 * multiple storage backends and provides SQL query capabilities.
 */
public class GlueSQL implements AutoCloseable {
    
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
     * Create a new GlueSQL instance with shared memory storage.
     *
     * @return GlueSQL instance with shared memory storage
     * @throws GlueSQLException if storage creation fails
     */
    public static GlueSQL newSharedMemory() throws GlueSQLException {
        long handle = nativeNewSharedMemory();
        if (handle == 0) {
            throw new GlueSQLException("Failed to create shared memory storage");
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
     * Create a new GlueSQL instance with redb storage.
     * @param path Path to the Redb database directory
     * @return GlueSQL instance with Redb storage
     * @throws GlueSQLException if storage creation fails
     */
    public static GlueSQL newRedb(String path) throws GlueSQLException {
        long handle = nativeNewRedb(path);
        if (handle == 0) {
            throw new GlueSQLException("Failed to create Redb storage at path: " + path);
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
     * Execute a SQL query asynchronously and return a CompletableFuture with the results.
     * <p>
     * This method allows non-blocking query execution, similar to JavaScript Promises.
     * The query is executed on the common ForkJoinPool.
     * 
     * @param sql SQL query string to execute
     * @return CompletableFuture that will contain the JSON string results
     */
    public CompletableFuture<String> queryAsync(String sql) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return query(sql);
            } catch (GlueSQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Execute a SQL query asynchronously on a custom thread pool.
     * 
     * @param sql SQL query string to execute
     * @param executor Custom executor for running the query
     * @return CompletableFuture that will contain the JSON string results
     */
    public CompletableFuture<String> queryAsync(String sql, java.util.concurrent.Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return query(sql);
            } catch (GlueSQLException e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }
    /**
     * Close the database and free native resources.
     * This method is called automatically when used with try-with-resources.
     */
    @Override
    public void close() {
        if (nativeHandle != 0) {
            nativeFree(nativeHandle);
            nativeHandle = 0;
        }
    }

    // Native method declarations
    private static native long nativeNewMemory();
    private static native long nativeNewSharedMemory();
    private static native long nativeNewSled(String path);
    private static native long nativeNewJson(String path);
    private static native long nativeNewRedb(String path);
    private static native String nativeQuery(long handle, String sql) throws GlueSQLException;
    private static native void nativeFree(long handle);
}
