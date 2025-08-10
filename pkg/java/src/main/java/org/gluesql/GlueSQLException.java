package org.gluesql;

/**
 * Exception thrown when GlueSQL operations fail.
 *
 * <p>This exception is used to wrap native Rust errors and provide Java-friendly error handling for
 * GlueSQL operations.
 */
public class GlueSQLException extends Exception {

    /**
     * Create a new GlueSQLException with the specified message.
     *
     * @param message Error message describing the failure
     */
    public GlueSQLException(String message) {
        super(message);
    }

    /**
     * Create a new GlueSQLException with the specified message and cause.
     *
     * @param message Error message describing the failure
     * @param cause The underlying cause of the exception
     */
    public GlueSQLException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Create a new GlueSQLException with the specified cause.
     *
     * @param cause The underlying cause of the exception
     */
    public GlueSQLException(Throwable cause) {
        super(cause);
    }
}
