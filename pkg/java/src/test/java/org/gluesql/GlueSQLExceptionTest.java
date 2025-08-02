package org.gluesql;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for GlueSQLException handling, particularly focusing on exceptions thrown from Rust native code and proper Java
 * exception propagation. Tests cover SQL syntax errors, runtime errors, and resource management failures.
 */
class GlueSQLExceptionTest {

    private GlueSQL database;

    @BeforeEach
    void setUp() throws GlueSQLException {
        database = GlueSQL.newMemory();
    }

    @AfterEach
    void tearDown() {
        database.close();
    }

    @Test
    @DisplayName("Parser Error")
    void testInvalidSqlSyntax() {
        GlueSQLException exception = assertThrows(GlueSQLException.class, () -> database.query("INVALID SQL SYNTAX"));
        assertSame(GlueSQLException.class, exception.getClass());
        assertTrue(exception.getMessage().contains("ParserError"));
    }

    @Test
    @DisplayName("Table not found error")
    void testTableNotFound() {
        GlueSQLException exception = assertThrows(GlueSQLException.class,
                () -> database.query("SELECT * FROM non_existent_table"));
        assertSame(GlueSQLException.class, exception.getClass());
        assertTrue(exception.getMessage().contains("GlueSQLError"));
    }

    @Test
    @DisplayName("Type mismatch error")
    void testTypeMismatch() throws GlueSQLException {
        database.query("CREATE TABLE type_test (id INTEGER, name TEXT)");

        GlueSQLException exception = assertThrows(GlueSQLException.class,
                () -> database.query("INSERT INTO type_test VALUES ('not_a_number', 123)"));

        assertSame(GlueSQLException.class, exception.getClass());
        assertTrue(exception.getMessage().contains("GlueSQLError"));
    }

    @Test
    @DisplayName("Invalid storage path errors")
    void testInvalidStoragePath() {
        String invalidPath = "/invalid/path";
        try {
            GlueSQL.newJson(invalidPath);
        } catch (GlueSQLException e) {
            assertSame(GlueSQLException.class, e.getClass());
            assertTrue(e.getMessage().contains(invalidPath));
        }
    }
}
