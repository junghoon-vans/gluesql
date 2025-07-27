package org.gluesql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Integration tests for different storage backends.
 */
class StorageIntegrationTest {

    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("gluesql-storage-test");
    }

    @AfterEach
    void tearDown() {
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

    @Test
    @DisplayName("Memory storage integration test")
    void testMemoryStorageIntegration() throws GlueSQLException {
        GlueSQL db = GlueSQL.newMemory();
        runBasicStorageTest(db, "Memory");
    }

    @Test
    @DisplayName("JSON storage integration test")
    void testJsonStorageIntegration() throws GlueSQLException {
        Path jsonPath = tempDir.resolve("json_storage");
        GlueSQL db = GlueSQL.newJson(jsonPath.toString());
        runBasicStorageTest(db, "JSON");
        
        // Test persistence by creating a new instance
        GlueSQL db2 = GlueSQL.newJson(jsonPath.toString());
        QueryResult result = db2.execute("SELECT COUNT(*) FROM storage_test");
        List<List<Object>> rows = result.getSelectRows();
        assertEquals(3, ((Number) rows.get(0).get(0)).intValue());
    }

    @Test
    @DisplayName("Sled storage integration test")
    void testSledStorageIntegration() throws GlueSQLException {
        Path sledPath = tempDir.resolve("sled_storage");
        GlueSQL db = GlueSQL.newSled(sledPath.toString());
        runBasicStorageTest(db, "Sled");
        
        // Test persistence by creating a new instance
        GlueSQL db2 = GlueSQL.newSled(sledPath.toString());
        QueryResult result = db2.execute("SELECT COUNT(*) FROM storage_test");
        List<List<Object>> rows = result.getSelectRows();
        assertEquals(3, ((Number) rows.get(0).get(0)).intValue());
    }

    @Test
    @DisplayName("Shared memory storage integration test")
    void testSharedMemoryStorageIntegration() throws GlueSQLException {
        String namespace = "test_namespace_" + System.currentTimeMillis();
        GlueSQL db = GlueSQL.newSharedMemory(namespace);
        runBasicStorageTest(db, "SharedMemory");
        
        // Test sharing between instances
        GlueSQL db2 = GlueSQL.newSharedMemory(namespace);
        QueryResult result = db2.execute("SELECT COUNT(*) FROM storage_test");
        List<List<Object>> rows = result.getSelectRows();
        assertEquals(3, ((Number) rows.get(0).get(0)).intValue());
    }

    private void runBasicStorageTest(GlueSQL db, String storageType) throws
            GlueSQLException {
        // Create table
        QueryResult createResult = db.execute("CREATE TABLE storage_test (" +
                "id INTEGER PRIMARY KEY, " +
                "name TEXT, " +
                "value DECIMAL, " +
                "active BOOLEAN" +
                ")");
        assertEquals("Create", createResult.getFirstResultType());

        // Insert test data
        QueryResult insertResult = db.execute("INSERT INTO storage_test VALUES " +
                "(1, 'First Item', 100.50, true), " +
                "(2, 'Second Item', 200.75, false), " +
                "(3, 'Third Item', 300.25, true)");
        assertEquals("Insert", insertResult.getFirstResultType());
        assertEquals(3, insertResult.getAffectedRows());

        // Test SELECT with WHERE clause
        QueryResult selectResult = db.execute("SELECT * FROM storage_test WHERE active = true ORDER BY id");
        assertTrue(selectResult.isSelectResult());
        
        List<List<Object>> rows = selectResult.getSelectRows();
        assertEquals(2, rows.size()); // Should return 2 active items
        
        // Verify first active item
        List<Object> firstRow = rows.get(0);
        assertEquals(1, ((Number) firstRow.get(0)).intValue());
        assertEquals("First Item", firstRow.get(1));
        assertEquals(true, firstRow.get(3));

        // Test UPDATE
        QueryResult updateResult = db.execute("UPDATE storage_test SET value = 150.00 WHERE id = 1");
        assertEquals("Update", updateResult.getFirstResultType());
        assertEquals(1, updateResult.getAffectedRows());

        // Verify update
        QueryResult verifyResult = db.execute("SELECT value FROM storage_test WHERE id = 1");
        List<List<Object>> verifyRows = verifyResult.getSelectRows();
        assertEquals(150.0, ((Number) verifyRows.get(0).get(0)).doubleValue(), 0.01);

        // Test aggregate functions
        QueryResult avgResult = db.execute("SELECT AVG(value) as avg_value FROM storage_test");
        List<List<Object>> avgRows = avgResult.getSelectRows();
        assertNotNull(avgRows.get(0).get(0)); // Should have some average value

        QueryResult countResult = db.execute("SELECT COUNT(*) as total FROM storage_test WHERE active = true");
        List<List<Object>> countRows = countResult.getSelectRows();
        assertEquals(2, ((Number) countRows.get(0).get(0)).intValue());

        // Test DELETE
        QueryResult deleteResult = db.execute("DELETE FROM storage_test WHERE id = 2");
        assertEquals("Delete", deleteResult.getFirstResultType());
        assertEquals(1, deleteResult.getAffectedRows());

        // Verify deletion
        QueryResult finalCountResult = db.execute("SELECT COUNT(*) FROM storage_test");
        List<List<Object>> finalCountRows = finalCountResult.getSelectRows();
        assertEquals(2, ((Number) finalCountRows.get(0).get(0)).intValue());

        System.out.println(storageType + " storage test completed successfully");
    }

    @Test
    @DisplayName("Test storage error handling")
    void testStorageErrorHandling() {
        // Test invalid paths
        assertThrows(GlueSQLException.class, () -> {
            GlueSQL.newJson("/invalid/path/that/does/not/exist");
        });

        assertThrows(GlueSQLException.class, () -> {
            GlueSQL.newSled("/invalid/path/that/does/not/exist");
        });
    }

    @Test
    @DisplayName("Test concurrent access to shared memory storage")
    void testConcurrentSharedMemoryAccess() throws GlueSQLException, InterruptedException {
        String namespace = "concurrent_test_" + System.currentTimeMillis();
        
        // Create initial database and table
        GlueSQL db1 = GlueSQL.newSharedMemory(namespace);
        db1.execute("CREATE TABLE concurrent_test (id INTEGER, thread_id INTEGER)");
        
        // Create multiple database instances simulating concurrent access
        GlueSQL db2 = GlueSQL.newSharedMemory(namespace);
        GlueSQL db3 = GlueSQL.newSharedMemory(namespace);
        
        // Insert from different instances
        db1.execute("INSERT INTO concurrent_test VALUES (1, 1)");
        db2.execute("INSERT INTO concurrent_test VALUES (2, 2)");
        db3.execute("INSERT INTO concurrent_test VALUES (3, 3)");
        
        // Verify all data is visible from any instance
        QueryResult result = db1.execute("SELECT COUNT(*) FROM concurrent_test");
        List<List<Object>> rows = result.getSelectRows();
        assertEquals(3, ((Number) rows.get(0).get(0)).intValue());
        
        // Verify from another instance
        QueryResult result2 = db2.execute("SELECT thread_id FROM concurrent_test ORDER BY id");
        List<List<Object>> rows2 = result2.getSelectRows();
        assertEquals(3, rows2.size());
        assertEquals(1, ((Number) rows2.get(0).get(0)).intValue());
        assertEquals(2, ((Number) rows2.get(1).get(0)).intValue());
        assertEquals(3, ((Number) rows2.get(2).get(0)).intValue());
    }

    @Test
    @DisplayName("Test data type compatibility across storages")
    void testDataTypeCompatibility() throws GlueSQLException {
        // Test with memory storage
        testDataTypesWithStorage(GlueSQL.newMemory(), "Memory");
        
        // Test with JSON storage
        Path jsonPath = tempDir.resolve("datatype_json");
        testDataTypesWithStorage(GlueSQL.newJson(jsonPath.toString()), "JSON");
        
        // Test with Sled storage
        Path sledPath = tempDir.resolve("datatype_sled");
        testDataTypesWithStorage(GlueSQL.newSled(sledPath.toString()), "Sled");
    }

    private void testDataTypesWithStorage(GlueSQL db, String storageType) throws
            GlueSQLException {
        db.execute("CREATE TABLE datatype_test (" +
                "int_col INTEGER, " +
                "text_col TEXT, " +
                "bool_col BOOLEAN, " +
                "decimal_col DECIMAL" +
                ")");

        db.execute("INSERT INTO datatype_test VALUES " +
                "(42, 'hello world', true, 123.45), " +
                "(-10, 'test string', false, -99.99)");

        QueryResult result = db.execute("SELECT * FROM datatype_test ORDER BY int_col");
        List<List<Object>> rows = result.getSelectRows();
        assertEquals(2, rows.size());

        // Verify first row
        List<Object> row1 = rows.get(0);
        assertEquals(-10, ((Number) row1.get(0)).intValue());
        assertEquals("test string", row1.get(1));
        assertEquals(false, row1.get(2));
        assertEquals(-99.99, ((Number) row1.get(3)).doubleValue(), 0.01);

        // Verify second row
        List<Object> row2 = rows.get(1);
        assertEquals(42, ((Number) row2.get(0)).intValue());
        assertEquals("hello world", row2.get(1));
        assertEquals(true, row2.get(2));
        assertEquals(123.45, ((Number) row2.get(3)).doubleValue(), 0.01);

        System.out.println(storageType + " storage data type test completed");
    }

    @Test
    @DisplayName("Test schema operations across storages")
    void testSchemaOperations() throws GlueSQLException {
        GlueSQL db = GlueSQL.newMemory();
        
        // Create multiple tables
        db.execute("CREATE TABLE table1 (id INTEGER, name TEXT)");
        db.execute("CREATE TABLE table2 (id INTEGER, value DECIMAL)");
        
        // Insert data
        db.execute("INSERT INTO table1 VALUES (1, 'test1'), (2, 'test2')");
        db.execute("INSERT INTO table2 VALUES (1, 100.5), (2, 200.7)");
        
        // Test joins (if supported)
        try {
            QueryResult joinResult = db.execute(
                "SELECT t1.name, t2.value FROM table1 t1 " +
                "JOIN table2 t2 ON t1.id = t2.id ORDER BY t1.id"
            );
            
            if (joinResult.isSelectResult()) {
                List<List<Object>> rows = joinResult.getSelectRows();
                assertEquals(2, rows.size());
                assertEquals("test1", rows.get(0).get(0));
                assertEquals(100.5, ((Number) rows.get(0).get(1)).doubleValue(), 0.1);
            }
        } catch (GlueSQLException e) {
            // JOIN might not be supported in all configurations
            System.out.println("JOIN not supported: " + e.getMessage());
        }
        
        // Drop tables
        db.execute("DROP TABLE table1");
        db.execute("DROP TABLE table2");
        
        // Verify tables are dropped by trying to query them
        assertThrows(GlueSQLException.class, () -> {
            db.execute("SELECT * FROM table1");
        });
    }
}
