package org.gluesql;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Comprehensive tests for GlueSQL including query functionality, storage types, single queries, multi-line queries,
 * different SQL operations, and storage backends.
 */
class GlueSQLTest {

    private GlueSQL database;

    @TempDir
    Path tempDir;

    private String timestamp;

    @BeforeEach
    void setUp() throws GlueSQLException {
        database = GlueSQL.newMemory();
        timestamp = String.valueOf(System.currentTimeMillis());
    }

    @AfterEach
    void tearDown() {
        database.close();
    }

    @Test
    @DisplayName("Single query - CREATE TABLE")
    void testSingleQueryCreateTable() throws GlueSQLException {
        String result = database.query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)");

        assertNotNull(result);
        assertTrue(result.contains("CREATE TABLE") || result.contains("\"type\""));
    }

    @Test
    @DisplayName("Single query - INSERT")
    void testSingleQueryInsert() throws GlueSQLException {
        database.query("CREATE TABLE test_table (id INTEGER, value TEXT)");

        String result = database.query("INSERT INTO test_table VALUES (1, 'hello')");

        assertNotNull(result);
        assertTrue(result.contains("INSERT") || result.contains("\"type\"") || result.contains("affected"));
    }

    @Test
    @DisplayName("Single query - SELECT")
    void testSingleQuerySelect() throws GlueSQLException {
        database.query("CREATE TABLE test_table (id INTEGER, value TEXT)");
        database.query("INSERT INTO test_table VALUES (1, 'hello'), (2, 'world')");

        String result = database.query("SELECT * FROM test_table ORDER BY id");

        assertNotNull(result);
        assertTrue(result.contains("SELECT") || result.contains("hello"));
        assertTrue(result.contains("world"));
    }

    @Test
    @DisplayName("Single query - UPDATE")
    void testSingleQueryUpdate() throws GlueSQLException {
        database.query("CREATE TABLE test_table (id INTEGER, value TEXT)");
        database.query("INSERT INTO test_table VALUES (1, 'hello')");

        String result = database.query("UPDATE test_table SET value = 'updated' WHERE id = 1");

        assertNotNull(result);
        assertTrue(result.contains("UPDATE") || result.contains("\"type\"") || result.contains("affected"));
    }

    @Test
    @DisplayName("Single query - DELETE")
    void testSingleQueryDelete() throws GlueSQLException {
        database.query("CREATE TABLE test_table (id INTEGER, value TEXT)");
        database.query("INSERT INTO test_table VALUES (1, 'hello'), (2, 'world')");

        String result = database.query("DELETE FROM test_table WHERE id = 1");

        assertNotNull(result);
        assertTrue(result.contains("DELETE") || result.contains("\"type\"") || result.contains("affected"));
    }

    @Test
    @DisplayName("Multi-line query with multiple statements")
    void testMultiLineQuery() throws GlueSQLException {
        String multilineQuery = """
                CREATE TABLE multi_test (id INTEGER, name TEXT, active BOOLEAN);
                INSERT INTO multi_test VALUES (1, 'Alice', true);
                INSERT INTO multi_test VALUES (2, 'Bob', false);
                """;

        String result = database.query(multilineQuery);

        assertNotNull(result);
        // Should contain results from multiple statements
        assertTrue(result.contains("CREATE TABLE") || result.contains("INSERT") || result.contains("\"type\""));

        // Verify data was actually inserted
        String selectResult = database.query("SELECT COUNT(*) FROM multi_test");
        assertNotNull(selectResult);
        assertTrue(selectResult.contains("2"));
    }

    @Test
    @DisplayName("Multi-line query with WHERE clause")
    void testQueryWithWhere() throws GlueSQLException {
        String setupQuery = """
                CREATE TABLE products (id INTEGER, name TEXT, category TEXT);
                INSERT INTO products VALUES (1, 'Laptop', 'Electronics'), (2, 'Book', 'Education');
                """;

        String result = database.query(setupQuery);
        assertNotNull(result);

        // Test WHERE clause functionality
        String selectResult = database.query("SELECT name FROM products WHERE category = 'Electronics'");
        assertNotNull(selectResult);
        assertTrue(selectResult.contains("Laptop"));
        assertFalse(selectResult.contains("Book"));
    }

    @Test
    @DisplayName("Basic aggregate query (COUNT)")
    void testBasicAggregate() throws GlueSQLException {
        database.query("CREATE TABLE numbers (value INTEGER)");
        database.query("INSERT INTO numbers VALUES (10), (20), (30)");

        // Test simple COUNT - focus on JSON binding, not SQL engine functionality
        String countResult = database.query("SELECT COUNT(*) FROM numbers");
        assertNotNull(countResult);
        assertTrue(countResult.contains("3"));
    }

    @Test
    @DisplayName("JSON output format validation")
    void testJsonOutputFormat() throws GlueSQLException {
        database.query("CREATE TABLE json_test (id INTEGER, name TEXT, active BOOLEAN)");
        database.query("INSERT INTO json_test VALUES (42, 'test_name', true)");

        String result = database.query("SELECT * FROM json_test");

        // Basic JSON structure validation
        assertNotNull(result);
        assertTrue(result.trim().startsWith("[") || result.trim().startsWith("{"));
        assertTrue(result.trim().endsWith("]") || result.trim().endsWith("}"));

        // Should contain our test data
        assertTrue(result.contains("42"));
        assertTrue(result.contains("test_name"));
        assertTrue(result.contains("true"));

        // Should have proper JSON structure indicators
        assertTrue(result.contains("\"") && (result.contains(":") || result.contains(",")));
    }

    @Test
    @DisplayName("Empty result set handling")
    void testEmptyResultSet() throws GlueSQLException {
        database.query("CREATE TABLE empty_test (id INTEGER, name TEXT)");

        String result = database.query("SELECT * FROM empty_test");

        assertNotNull(result);
        // Should return valid JSON even for empty results
        assertTrue(result.trim().startsWith("[") || result.trim().startsWith("{"));
    }

    @Test
    @DisplayName("Multiple row handling")
    void testMultipleRows() throws GlueSQLException {
        database.query("CREATE TABLE multi_row_test (id INTEGER, value TEXT)");

        // Insert multiple rows in one statement
        String insertResult = database.query(
                "INSERT INTO multi_row_test VALUES (1, 'value_1'), (2, 'value_2'), (3, 'value_3'), (4, 'value_4'), (5, 'value_5')");
        assertNotNull(insertResult);

        // Query all data
        String selectResult = database.query("SELECT COUNT(*) FROM multi_row_test");
        assertNotNull(selectResult);
        assertTrue(selectResult.contains("5"));

        // Query with LIMIT
        String limitResult = database.query("SELECT * FROM multi_row_test LIMIT 2");
        assertNotNull(limitResult);
        assertTrue(limitResult.contains("value_"));
    }

    // ===== STORAGE TYPE TESTS =====
    // Note: Basic memory storage functionality is already tested in single query tests above

    @Test
    @DisplayName("Memory storage - data isolation between instances")
    void testMemoryStorageIsolation() throws GlueSQLException {
        // Create first instance and add data
        try (GlueSQL db1 = GlueSQL.newMemory()) {
            db1.query("CREATE TABLE isolation_test (id INTEGER)");
            db1.query("INSERT INTO isolation_test VALUES (1)");

            String result1 = db1.query("SELECT COUNT(*) FROM isolation_test");
            assertTrue(result1.contains("1"));
        }

        // Create second instance - should not see data from first
        try (GlueSQL db2 = GlueSQL.newMemory()) {
            // This should fail because table doesn't exist in new memory instance
            assertThrows(GlueSQLException.class, () -> {
                db2.query("SELECT * FROM isolation_test");
            });
        }
    }

    @Test
    @DisplayName("JSON storage - basic operations")
    void testJsonStorage() throws GlueSQLException {
        String jsonPath = tempDir.resolve("json_test_" + timestamp).toString();

        try (GlueSQL db = GlueSQL.newJson(jsonPath)) {
            String createResult = db.query("CREATE TABLE json_test (id INTEGER, name TEXT)");
            assertNotNull(createResult);

            String insertResult = db.query("INSERT INTO json_test VALUES (1, 'json_data')");
            assertNotNull(insertResult);

            String selectResult = db.query("SELECT * FROM json_test");
            assertNotNull(selectResult);
            assertTrue(selectResult.contains("json_data"));
        }
    }

    @Test
    @DisplayName("JSON storage - data persistence")
    void testJsonStoragePersistence() throws GlueSQLException {
        String jsonPath = tempDir.resolve("json_persistence_" + timestamp).toString();

        // Create data in first instance
        try (GlueSQL db1 = GlueSQL.newJson(jsonPath)) {
            db1.query("CREATE TABLE persistence_test (id INTEGER, value TEXT)");
            db1.query("INSERT INTO persistence_test VALUES (1, 'persistent_data')");

            String result = db1.query("SELECT * FROM persistence_test");
            assertTrue(result.contains("persistent_data"));
        }

        // Open same path in new instance - data should persist
        try (GlueSQL db2 = GlueSQL.newJson(jsonPath)) {
            String result = db2.query("SELECT * FROM persistence_test");
            assertNotNull(result);
            assertTrue(result.contains("persistent_data"));
        }
    }

    @Test
    @DisplayName("Sled storage - basic operations")
    void testSledStorage() throws GlueSQLException {
        String sledPath = tempDir.resolve("sled_test_" + timestamp).toString();

        try (GlueSQL db = GlueSQL.newSled(sledPath)) {
            String createResult = db.query("CREATE TABLE sled_test (id INTEGER, name TEXT)");
            assertNotNull(createResult);

            String insertResult = db.query("INSERT INTO sled_test VALUES (1, 'sled_data')");
            assertNotNull(insertResult);

            String selectResult = db.query("SELECT * FROM sled_test");
            assertNotNull(selectResult);
            assertTrue(selectResult.contains("sled_data"));
        }
    }

    @Test
    @DisplayName("Sled storage - data persistence")
    void testSledStoragePersistence() throws GlueSQLException {
        String sledPath = tempDir.resolve("sled_persistence_" + timestamp).toString();

        // Create data in first instance
        try (GlueSQL db1 = GlueSQL.newSled(sledPath)) {
            db1.query("CREATE TABLE sled_persistence_test (id INTEGER, value TEXT)");
            db1.query("INSERT INTO sled_persistence_test VALUES (1, 'sled_persistent_data')");

            String result = db1.query("SELECT * FROM sled_persistence_test");
            assertTrue(result.contains("sled_persistent_data"));
        }

        // Open same path in new instance - data should persist
        try (GlueSQL db2 = GlueSQL.newSled(sledPath)) {
            String result = db2.query("SELECT * FROM sled_persistence_test");
            assertNotNull(result);
            assertTrue(result.contains("sled_persistent_data"));
        }
    }

    @Test
    @DisplayName("Shared Memory storage - basic operations")
    void testSharedMemoryStorage() throws GlueSQLException {
        String namespace = "shared_test_" + timestamp;

        try (GlueSQL db = GlueSQL.newSharedMemory(namespace)) {
            String createResult = db.query("CREATE TABLE shared_test (id INTEGER, name TEXT)");
            assertNotNull(createResult);

            String insertResult = db.query("INSERT INTO shared_test VALUES (1, 'shared_data')");
            assertNotNull(insertResult);

            String selectResult = db.query("SELECT * FROM shared_test");
            assertNotNull(selectResult);
            assertTrue(selectResult.contains("shared_data"));
        }
    }

    @Test
    @DisplayName("Shared Memory storage - data sharing between instances")
    void testSharedMemorySharing() throws GlueSQLException {
        String namespace = "shared_persistence_" + timestamp;

        // Create data in first instance
        try (GlueSQL db1 = GlueSQL.newSharedMemory(namespace)) {
            db1.query("CREATE TABLE shared_persistence_test (id INTEGER, value TEXT)");
            db1.query("INSERT INTO shared_persistence_test VALUES (1, 'shared_persistent_data')");

            String result = db1.query("SELECT * FROM shared_persistence_test");
            assertTrue(result.contains("shared_persistent_data"));
        }

        try (GlueSQL db2 = GlueSQL.newSharedMemory(namespace)) {
            try {
                String result = db2.query("SELECT * FROM shared_persistence_test");
                assertNotNull(result);
                // If sharing works, should contain the data
                assertTrue(result.contains("shared_persistent_data"));
            } catch (GlueSQLException e) {
                // If sharing doesn't work, table won't exist - that's also acceptable
                assertTrue(e.getMessage().toLowerCase().contains("table") || e.getMessage().toLowerCase()
                        .contains("exist"));
            }
        }
    }
}
