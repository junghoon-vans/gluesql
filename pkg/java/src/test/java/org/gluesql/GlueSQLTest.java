package org.gluesql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Unit tests for GlueSQL Java bindings.
 */
class GlueSQLTest {

    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("gluesql-test");
    }

    @AfterEach
    void tearDown() {
        // Clean up temporary directory
        if (tempDir != null) {
            deleteDirectory(tempDir.toFile());
        }
    }

    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }

    private int extractIntegerValue(Object obj) {
        if (obj instanceof Number) {
            return ((Number) obj).intValue();
        } else if (obj instanceof java.util.Map) {
            // Handle GlueSQL Value structure
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> map = (java.util.Map<String, Object>) obj;
            if (map.containsKey("I64")) {
                Object value = map.get("I64");
                return ((Number) value).intValue();
            } else if (map.containsKey("I32")) {
                Object value = map.get("I32");
                return ((Number) value).intValue();
            }
        }
        throw new RuntimeException("Cannot extract integer from: " + obj.getClass() + " = " + obj);
    }

    @Test
    @DisplayName("Create in-memory database")
    void testCreateMemoryDatabase() {
        GlueSQL memoryDb = GlueSQL.newMemory();
        assertNotNull(memoryDb);
    }

    @Test
    @DisplayName("Create and drop table")
    void testCreateAndDropTable() throws GlueSQLException {
        GlueSQL memoryDb = GlueSQL.newMemory();
        
        // Create table
        QueryResult createResult = memoryDb.execute("CREATE TABLE test_table (id INTEGER, name TEXT)");
        assertEquals("Create", createResult.getFirstResultType());

        // Insert some data to verify table exists
        QueryResult insertResult = memoryDb.execute("INSERT INTO test_table VALUES (1, 'test')");
        assertEquals("Insert", insertResult.getFirstResultType());
        assertEquals(1, insertResult.getAffectedRows());

        // Drop table
        QueryResult dropResult = memoryDb.execute("DROP TABLE test_table");
        assertEquals("Drop", dropResult.getFirstResultType());
    }

    @Test
    @DisplayName("Basic CRUD operations")
    void testBasicCrudOperations() throws GlueSQLException {
        GlueSQL memoryDb = GlueSQL.newMemory();
        
        // Create table
        memoryDb.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");

        // Insert data
        QueryResult insertResult = memoryDb.execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)");
        assertEquals("Insert", insertResult.getFirstResultType());
        assertEquals(2, insertResult.getAffectedRows());

        // Select data
        QueryResult selectResult = memoryDb.execute("SELECT * FROM users ORDER BY id");
        assertTrue(selectResult.isSelectResult());
        
        List<String> labels = selectResult.getSelectLabels();
        assertEquals(3, labels.size());
        assertTrue(labels.contains("id"));
        assertTrue(labels.contains("name"));
        assertTrue(labels.contains("age"));

        List<List<Object>> rows = selectResult.getSelectRows();
        assertEquals(2, rows.size());
        
        // Check first row
        List<Object> firstRow = rows.get(0);
        assertEquals(3, firstRow.size());
        
        // Handle different data types safely
        Object idObj = firstRow.get(0);
        int id = extractIntegerValue(idObj);
        assertEquals(1, id);
        
        assertEquals("Alice", firstRow.get(1));
        
        Object ageObj = firstRow.get(2);
        int age = extractIntegerValue(ageObj);
        assertEquals(30, age);

        // Update data
        QueryResult updateResult = memoryDb.execute("UPDATE users SET age = 31 WHERE id = 1");
        assertEquals("Update", updateResult.getFirstResultType());
        assertEquals(1, updateResult.getAffectedRows());

        // Verify update
        QueryResult verifyResult = memoryDb.execute("SELECT age FROM users WHERE id = 1");
        List<List<Object>> verifyRows = verifyResult.getSelectRows();
        assertEquals(31, ((Number) verifyRows.get(0).get(0)).intValue());

        // Delete data
        QueryResult deleteResult = memoryDb.execute("DELETE FROM users WHERE id = 2");
        assertEquals("Delete", deleteResult.getFirstResultType());
        assertEquals(1, deleteResult.getAffectedRows());

        // Verify deletion
        QueryResult countResult = memoryDb.execute("SELECT COUNT(*) FROM users");
        List<List<Object>> countRows = countResult.getSelectRows();
        assertEquals(1, ((Number) countRows.get(0).get(0)).intValue());
    }

    @Test
    @DisplayName("Complex queries with WHERE and ORDER BY")
    void testComplexQueries() throws GlueSQLException {
        GlueSQL memoryDb = GlueSQL.newMemory();
        
        // Create and populate table
        memoryDb.execute("CREATE TABLE products (id INTEGER, name TEXT, price DECIMAL, category TEXT)");
        memoryDb.execute("INSERT INTO products VALUES " +
                "(1, 'Laptop', 999.99, 'Electronics'), " +
                "(2, 'Book', 19.99, 'Education'), " +
                "(3, 'Phone', 599.99, 'Electronics'), " +
                "(4, 'Desk', 299.99, 'Furniture')");

        // Test WHERE clause
        QueryResult whereResult = memoryDb.execute("SELECT name, price FROM products WHERE category = 'Electronics' ORDER BY price DESC");
        assertTrue(whereResult.isSelectResult());
        
        List<List<Object>> whereRows = whereResult.getSelectRows();
        assertEquals(2, whereRows.size());
        assertEquals("Laptop", whereRows.get(0).get(0)); // First should be laptop (higher price)
        assertEquals("Phone", whereRows.get(1).get(0));  // Second should be phone

        // Test aggregate function
        QueryResult avgResult = memoryDb.execute("SELECT AVG(price) as avg_price FROM products WHERE category = 'Electronics'");
        List<List<Object>> avgRows = avgResult.getSelectRows();
        assertNotNull(avgRows.get(0).get(0));
    }

    @Test
    @DisplayName("Error handling for invalid SQL")
    void testErrorHandling() {
        GlueSQL memoryDb = GlueSQL.newMemory();
        
        // Test invalid SQL syntax
        assertThrows(GlueSQLException.class, () -> {
            memoryDb.execute("INVALID SQL SYNTAX");
        });

        // Test query on non-existent table
        assertThrows(GlueSQLException.class, () -> {
            memoryDb.execute("SELECT * FROM non_existent_table");
        });
    }

    @Test
    @DisplayName("Test JSON storage")
    void testJsonStorage() throws GlueSQLException {
        GlueSQL jsonDb = GlueSQL.newJson(tempDir.toString());
        assertNotNull(jsonDb);

        // Basic operations
        jsonDb.execute("CREATE TABLE json_test (id INTEGER, data TEXT)");
        QueryResult insertResult = jsonDb.execute("INSERT INTO json_test VALUES (1, 'test data')");
        assertEquals(1, insertResult.getAffectedRows());

        QueryResult selectResult = jsonDb.execute("SELECT * FROM json_test");
        assertTrue(selectResult.isSelectResult());
        assertEquals(1, selectResult.getSelectRows().size());
    }

    @Test
    @DisplayName("Test Sled storage")
    void testSledStorage() throws GlueSQLException {
        Path sledPath = tempDir.resolve("sled_test");
        GlueSQL sledDb = GlueSQL.newSled(sledPath.toString());
        assertNotNull(sledDb);

        // Basic operations
        sledDb.execute("CREATE TABLE sled_test (id INTEGER, value TEXT)");
        QueryResult insertResult = sledDb.execute("INSERT INTO sled_test VALUES (1, 'sled data')");
        assertEquals(1, insertResult.getAffectedRows());

        QueryResult selectResult = sledDb.execute("SELECT * FROM sled_test");
        assertTrue(selectResult.isSelectResult());
        assertEquals(1, selectResult.getSelectRows().size());
    }

    @Test
    @DisplayName("Test shared memory storage")
    void testSharedMemoryStorage() throws GlueSQLException {
        GlueSQL sharedDb = GlueSQL.newSharedMemory("test_namespace");
        assertNotNull(sharedDb);

        // Basic operations
        sharedDb.execute("CREATE TABLE shared_test (id INTEGER, info TEXT)");
        QueryResult insertResult = sharedDb.execute("INSERT INTO shared_test VALUES (1, 'shared data')");
        assertEquals(1, insertResult.getAffectedRows());

        QueryResult selectResult = sharedDb.execute("SELECT * FROM shared_test");
        assertTrue(selectResult.isSelectResult());
        assertEquals(1, selectResult.getSelectRows().size());
    }

    @Test
    @DisplayName("Test multiple statements")
    void testMultipleStatements() throws GlueSQLException {
        GlueSQL memoryDb = GlueSQL.newMemory();
        
        // Execute multiple statements in sequence
        memoryDb.execute("CREATE TABLE multi_test (id INTEGER, name TEXT)");
        memoryDb.execute("INSERT INTO multi_test VALUES (1, 'first')");
        memoryDb.execute("INSERT INTO multi_test VALUES (2, 'second')");
        
        QueryResult result = memoryDb.execute("SELECT COUNT(*) FROM multi_test");
        List<List<Object>> rows = result.getSelectRows();
        assertEquals(2, ((Number) rows.get(0).get(0)).intValue());
    }

    @Test
    @DisplayName("Test data types")
    void testDataTypes() throws GlueSQLException {
        GlueSQL memoryDb = GlueSQL.newMemory();
        
        memoryDb.execute("CREATE TABLE types_test (" +
                "int_col INTEGER, " +
                "text_col TEXT, " +
                "bool_col BOOLEAN, " +
                "decimal_col DECIMAL" +
                ")");

        memoryDb.execute("INSERT INTO types_test VALUES (42, 'hello', true, 123.45)");
        
        QueryResult result = memoryDb.execute("SELECT * FROM types_test");
        List<List<Object>> rows = result.getSelectRows();
        assertEquals(1, rows.size());
        
        List<Object> row = rows.get(0);
        assertEquals(4, row.size());
        assertNotNull(row.get(0)); // int
        assertNotNull(row.get(1)); // text
        assertNotNull(row.get(2)); // boolean
        assertNotNull(row.get(3)); // decimal
    }

    @Test
    @DisplayName("Test transaction simulation")
    void testTransactionSimulation() throws GlueSQLException {
        GlueSQL memoryDb = GlueSQL.newMemory();
        
        // Create table
        memoryDb.execute("CREATE TABLE transaction_test (id INTEGER, balance DECIMAL)");
        memoryDb.execute("INSERT INTO transaction_test VALUES (1, 100.0), (2, 200.0)");

        // Simulate a transaction (subtract from one, add to another)
        memoryDb.execute("UPDATE transaction_test SET balance = balance - 50 WHERE id = 1");
        memoryDb.execute("UPDATE transaction_test SET balance = balance + 50 WHERE id = 2");

        // Verify balances
        QueryResult result = memoryDb.execute("SELECT id, balance FROM transaction_test ORDER BY id");
        List<List<Object>> rows = result.getSelectRows();
        
        // First account should have 50
        assertEquals(50.0, ((Number) rows.get(0).get(1)).doubleValue(), 0.01);
        // Second account should have 250
        assertEquals(250.0, ((Number) rows.get(1).get(1)).doubleValue(), 0.01);
    }
}
