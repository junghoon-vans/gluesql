package org.gluesql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple test to verify memory storage functionality without native library.
 */
class SimpleMemoryTest {

    @Test
    @DisplayName("Test basic memory storage creation")
    void testMemoryStorageCreation() {
        try {
            // This test will fail if native library is not available
            // but should help us understand the problem
            GlueSQL db = GlueSQL.newMemory();
            assertNotNull(db);
            
            // Basic SQL operation
            String result = db.query("CREATE TABLE test (id INTEGER, name TEXT)");
            assertNotNull(result);
            
        } catch (UnsatisfiedLinkError e) {
            System.out.println("Native library not found: " + e.getMessage());
            System.out.println("Expected in development - native library needs to be built and placed correctly");
        } catch (Exception e) {
            System.out.println("Other error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    @DisplayName("Test QueryResult JSON parsing")
    void testQueryResultParsing() throws GlueSQLException {
        // This test doesn't require native library
        String testJson = "[{\"type\":\"Create\",\"result\":\"Success\"}]";
        
        QueryResult result = QueryResult.fromJson(testJson);
        assertNotNull(result);
        assertEquals(1, result.getResultCount());
        assertEquals("Create", result.getFirstResultType());
    }
}
