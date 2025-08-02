package org.gluesql;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Comprehensive tests for GlueSQL including query functionality, storage types, single queries, multi-line queries,
 * different SQL operations, and storage backends.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GlueSQLTest {

    @TempDir
    static Path tempDir;

    private static Stream<Arguments> databaseProvider() throws GlueSQLException {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String jsonPath = tempDir.resolve("json_test_" + timestamp).toString();
        String sledPath = tempDir.resolve("sled_test_" + timestamp).toString();
        String redbPath = tempDir.resolve("redb_test_" + timestamp).toString();

        return Stream.of(
            Arguments.of(GlueSQL.newMemory(), "Memory"),
            Arguments.of(GlueSQL.newSharedMemory(), "SharedMemory"),
            Arguments.of(GlueSQL.newJson(jsonPath), "JSON"),
            Arguments.of(GlueSQL.newSled(sledPath), "Sled"),
            Arguments.of(GlueSQL.newRedb(redbPath), "Redb")
        );
    }

    @ParameterizedTest
    @MethodSource("databaseProvider")
    @DisplayName("Single query - CREATE TABLE")
    void testSingleQueryCreateTable(GlueSQL database, String storageName) throws GlueSQLException {
        try (database) {
            String result = database.query("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)");

            assertNotNull(result, "CREATE TABLE should work for " + storageName);
            assertTrue(result.contains("CREATE TABLE") || result.contains("\"type\""), 
                "Result should contain CREATE TABLE indicator for " + storageName);
        }
    }

    @ParameterizedTest
    @MethodSource("databaseProvider")
    @DisplayName("Single query - INSERT")
    void testSingleQueryInsert(GlueSQL database, String storageName) throws GlueSQLException {
        try (database) {
            database.query("CREATE TABLE test_table (id INTEGER, value TEXT)");

            String result = database.query("INSERT INTO test_table VALUES (1, 'hello')");

            assertNotNull(result, "INSERT should work for " + storageName);
            assertTrue(result.contains("INSERT") || result.contains("\"type\"") || result.contains("affected"),
                "Result should contain INSERT indicator for " + storageName);
        }
    }

    @ParameterizedTest
    @MethodSource("databaseProvider")
    @DisplayName("Single query - SELECT")
    void testSingleQuerySelect(GlueSQL database, String storageName) throws GlueSQLException {
        try (database) {
            database.query("CREATE TABLE test_table (id INTEGER, value TEXT)");
            database.query("INSERT INTO test_table VALUES (1, 'hello'), (2, 'world')");

            String result = database.query("SELECT * FROM test_table ORDER BY id");

            assertNotNull(result, "SELECT should work for " + storageName);
            assertTrue(result.contains("SELECT") || result.contains("hello"),
                "Result should contain data for " + storageName);
            assertTrue(result.contains("world"), "Result should contain all data for " + storageName);
        }
    }

    @ParameterizedTest
    @MethodSource("databaseProvider")
    @DisplayName("Single query - UPDATE")
    void testSingleQueryUpdate(GlueSQL database, String storageName) throws GlueSQLException {
        try (database) {
            database.query("CREATE TABLE test_table (id INTEGER, value TEXT)");
            database.query("INSERT INTO test_table VALUES (1, 'hello')");

            String result = database.query("UPDATE test_table SET value = 'updated' WHERE id = 1");

            assertNotNull(result, "UPDATE should work for " + storageName);
            assertTrue(result.contains("UPDATE") || result.contains("\"type\"") || result.contains("affected"),
                "Result should contain UPDATE indicator for " + storageName);
        }
    }

    @ParameterizedTest
    @MethodSource("databaseProvider")
    @DisplayName("Single query - DELETE")
    void testSingleQueryDelete(GlueSQL database, String storageName) throws GlueSQLException {
        try (database) {
            database.query("CREATE TABLE test_table (id INTEGER, value TEXT)");
            database.query("INSERT INTO test_table VALUES (1, 'hello'), (2, 'world')");

            String result = database.query("DELETE FROM test_table WHERE id = 1");

            assertNotNull(result, "DELETE should work for " + storageName);
            assertTrue(result.contains("DELETE") || result.contains("\"type\"") || result.contains("affected"),
                "Result should contain DELETE indicator for " + storageName);
        }
    }

    @ParameterizedTest
    @MethodSource("databaseProvider")
    @DisplayName("Multi-line query with multiple statements")
    void testMultiLineQuery(GlueSQL database, String storageName) throws GlueSQLException {
        try (database) {
            String multilineQuery = """
                    CREATE TABLE multi_test (id INTEGER, name TEXT, active BOOLEAN);
                    INSERT INTO multi_test VALUES (1, 'Alice', true);
                    INSERT INTO multi_test VALUES (2, 'Bob', false);
                    """;

            String result = database.query(multilineQuery);
            assertNotNull(result, "Multi-line query should work for " + storageName);

            // Verify data was actually inserted
            String selectResult = database.query("SELECT COUNT(*) FROM multi_test");
            assertNotNull(selectResult, "Count query should work for " + storageName);
            assertTrue(selectResult.contains("2"), "Should have 2 rows for " + storageName);
        }
    }

    @ParameterizedTest
    @MethodSource("databaseProvider")
    @DisplayName("Query with WHERE clause")
    void testQueryWithWhere(GlueSQL database, String storageName) throws GlueSQLException {
        try (database) {
            String setupQuery = """
                    CREATE TABLE products (id INTEGER, name TEXT, category TEXT);
                    INSERT INTO products VALUES (1, 'Laptop', 'Electronics'), (2, 'Book', 'Education');
                    """;

            String result = database.query(setupQuery);
            assertNotNull(result, "Setup should work for " + storageName);

            // Test WHERE clause functionality
            String selectResult = database.query("SELECT name FROM products WHERE category = 'Electronics'");
            assertNotNull(selectResult, "WHERE query should work for " + storageName);
            assertTrue(selectResult.contains("Laptop"), "Should contain filtered data for " + storageName);
            assertFalse(selectResult.contains("Book"), "Should not contain filtered out data for " + storageName);
        }
    }

    @ParameterizedTest
    @MethodSource("databaseProvider")
    @DisplayName("Basic aggregate query (COUNT)")
    void testBasicAggregate(GlueSQL database, String storageName) throws GlueSQLException {
        try (database) {
            database.query("CREATE TABLE numbers (value INTEGER)");
            database.query("INSERT INTO numbers VALUES (10), (20), (30)");

            // Test simple COUNT - focus on JSON binding, not SQL engine functionality
            String countResult = database.query("SELECT COUNT(*) FROM numbers");
            assertNotNull(countResult, "COUNT should work for " + storageName);
            assertTrue(countResult.contains("3"), "Should return correct count for " + storageName);
        }
    }

    @ParameterizedTest
    @MethodSource("databaseProvider")
    @DisplayName("JSON output format validation")
    void testJsonOutputFormat(GlueSQL database, String storageName) throws GlueSQLException {
        try (database) {
            database.query("CREATE TABLE json_test (id INTEGER, name TEXT, active BOOLEAN)");
            database.query("INSERT INTO json_test VALUES (42, 'test_name', true)");

            String result = database.query("SELECT * FROM json_test");

            // Basic JSON structure validation
            assertNotNull(result, "Query result should not be null for " + storageName);
            assertTrue(result.trim().startsWith("[") || result.trim().startsWith("{"),
                "Result should be valid JSON structure for " + storageName);
            assertTrue(result.trim().endsWith("]") || result.trim().endsWith("}"),
                "Result should be valid JSON structure for " + storageName);

            // Should contain our test data
            assertTrue(result.contains("42"), "Should contain test data for " + storageName);
            assertTrue(result.contains("test_name"), "Should contain test data for " + storageName);
            assertTrue(result.contains("true"), "Should contain test data for " + storageName);

            // Should have proper JSON structure indicators
            assertTrue(result.contains("\"") && (result.contains(":") || result.contains(",")),
                "Should have JSON syntax for " + storageName);
        }
    }

    @ParameterizedTest
    @MethodSource("databaseProvider")
    @DisplayName("Empty result set handling")
    void testEmptyResultSet(GlueSQL database, String storageName) throws GlueSQLException {
        try (database) {
            database.query("CREATE TABLE empty_test (id INTEGER, name TEXT)");

            String result = database.query("SELECT * FROM empty_test");

            assertNotNull(result, "Empty result should not be null for " + storageName);
            // Should return valid JSON even for empty results
            assertTrue(result.trim().startsWith("[") || result.trim().startsWith("{"),
                "Empty result should be valid JSON for " + storageName);
        }
    }

    @ParameterizedTest
    @MethodSource("databaseProvider")
    @DisplayName("Multiple row handling")
    void testMultipleRows(GlueSQL database, String storageName) throws GlueSQLException {
        try (database) {
            database.query("CREATE TABLE multi_row_test (id INTEGER, value TEXT)");

            // Insert multiple rows in one statement
            String insertResult = database.query(
                    "INSERT INTO multi_row_test VALUES (1, 'value_1'), (2, 'value_2'), (3, 'value_3'), (4, 'value_4'), (5, 'value_5')");
            assertNotNull(insertResult, "Multi-row insert should work for " + storageName);

            // Query all data
            String selectResult = database.query("SELECT COUNT(*) FROM multi_row_test");
            assertNotNull(selectResult, "Count should work for " + storageName);
            assertTrue(selectResult.contains("5"), "Should have 5 rows for " + storageName);

            // Query with LIMIT
            String limitResult = database.query("SELECT * FROM multi_row_test LIMIT 2");
            assertNotNull(limitResult, "LIMIT should work for " + storageName);
            assertTrue(limitResult.contains("value_"), "Should contain data for " + storageName);
        }
    }
}