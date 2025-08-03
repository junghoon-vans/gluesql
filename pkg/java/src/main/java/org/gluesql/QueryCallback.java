package org.gluesql;

/**
 * Callback interface for asynchronous query execution.
 * This enables true non-blocking async operations at the native level.
 */
public interface QueryCallback {
    /**
     * Called when the query completes successfully.
     * 
     * @param result JSON string containing query results
     */
    void onSuccess(String result);
    
    /**
     * Called when the query fails with an error.
     * 
     * @param error The error message that occurred during query execution
     */
    void onError(String error);
}