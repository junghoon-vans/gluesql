package org.gluesql;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * GlueSQL Java bindings - Main interface for interacting with GlueSQL database.
 *
 * <p>GlueSQL is an open-source SQL database engine written in Rust that supports multiple storage
 * backends and provides SQL query capabilities.
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
     *
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
     * <p>This method blocks until the query completes. Internally, it uses the async implementation
     * and waits for the result, providing a simple synchronous interface.
     *
     * @param sql SQL query string to execute
     * @return JSON string containing query results
     * @throws GlueSQLException if query execution fails
     */
    public String query(String sql) throws GlueSQLException {
        try {
            return queryAsync(sql).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GlueSQLException("Query was interrupted: " + e.getMessage());
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof GlueSQLException glueSQLExceptionCause) {
                throw glueSQLExceptionCause;
            }
            throw new GlueSQLException("Query execution failed: " + cause.getMessage());
        }
    }

    /**
     * Execute a SQL query asynchronously and return a CompletableFuture with the results.
     *
     * @param sql SQL query string to execute
     * @return CompletableFuture that will contain the JSON string results
     */
    public CompletableFuture<String> queryAsync(String sql) {
        if (nativeHandle == 0) {
            CompletableFuture<String> future = new CompletableFuture<>();
            future.completeExceptionally(new GlueSQLException("Database instance has been closed"));
            return future;
        }

        CompletableFuture<String> future = new CompletableFuture<>();

        nativeQueryAsync(nativeHandle, sql, new QueryCallback() {
            @Override
            public void onSuccess(String result) {
                future.complete(result);
            }

            @Override
            public void onError(String error) {
                future.completeExceptionally(new GlueSQLException(error));
            }
        });

        return future;
    }

    /**
     * Close the database and free native resources. This method is called automatically when used
     * with try-with-resources.
     */
    @Override
    public void close() {
        if (nativeHandle != 0) {
            nativeFree(nativeHandle);
            nativeHandle = 0;
        }
    }

    // region JNI methods declarations
    private static native long nativeNewMemory();

    private static native long nativeNewSharedMemory();

    private static native long nativeNewSled(String path);

    private static native long nativeNewJson(String path);

    private static native long nativeNewRedb(String path);

    private static native void nativeQueryAsync(long handle, String sql, QueryCallback callback);

    private static native void nativeFree(long handle);
    // endregion
}
