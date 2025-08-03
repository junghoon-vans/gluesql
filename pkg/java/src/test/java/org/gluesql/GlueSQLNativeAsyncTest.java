package org.gluesql;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for native async query functionality using callbacks.
 * This tests true non-blocking async execution at the native level.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GlueSQLNativeAsyncTest {

    @TempDir
    static Path tempDir;

    private static Stream<Arguments> databaseProvider() throws GlueSQLException {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String jsonPath = tempDir.resolve("json_async_test_" + timestamp).toString();
        String sledPath = tempDir.resolve("sled_async_test_" + timestamp).toString();
        String redbPath = tempDir.resolve("redb_async_test_" + timestamp).toString();

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
    @DisplayName("Native async query success with CompletableFuture")
    void testNativeAsyncQuerySuccess(GlueSQL database, String storageName) throws Exception {
        try (database) {
            // Setup test data
            database.query("CREATE TABLE async_native_test (id INTEGER, value TEXT)");
            database.query("INSERT INTO async_native_test VALUES (1, 'native_async_value')");

            // Test native async query with CompletableFuture
            CompletableFuture<String> future = database.queryAsync("SELECT * FROM async_native_test");
            
            assertNotNull(future, "Future should not be null for " + storageName);
            
            String result = future.get(5, TimeUnit.SECONDS); // Wait with timeout
            
            assertNotNull(result, "Result should not be null for " + storageName);
            assertTrue(result.contains("native_async_value"), 
                "Should contain test data for " + storageName);
        }
    }

    @ParameterizedTest
    @MethodSource("databaseProvider")
    @DisplayName("Native async query error handling")
    void testNativeAsyncQueryError(GlueSQL database, String storageName) {
        try (database) {
            // Test native async query with invalid SQL
            CompletableFuture<String> future = database.queryAsync("INVALID SQL STATEMENT");
            
            assertNotNull(future, "Future should not be null for " + storageName);
            
            // Should complete exceptionally
            assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS), 
                "Should throw ExecutionException for invalid SQL for " + storageName);
        }
    }

    @ParameterizedTest
    @MethodSource("databaseProvider")
    @DisplayName("Multiple concurrent native async queries")
    void testConcurrentNativeAsyncQueries(GlueSQL database, String storageName) throws Exception {
        try (database) {
            // Setup test data
            database.query("CREATE TABLE concurrent_native_test (id INTEGER, value TEXT)");
            database.query("INSERT INTO concurrent_native_test VALUES (1, 'value1'), (2, 'value2'), (3, 'value3')");

            // Execute multiple concurrent native async queries
            CompletableFuture<String> future1 = database.queryAsync("SELECT * FROM concurrent_native_test WHERE id = 1");
            CompletableFuture<String> future2 = database.queryAsync("SELECT * FROM concurrent_native_test WHERE id = 2");
            CompletableFuture<String> future3 = database.queryAsync("SELECT * FROM concurrent_native_test WHERE id = 3");
            
            // Wait for all to complete
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(future1, future2, future3);
            allFutures.get(10, TimeUnit.SECONDS);
            
            // Check results
            String result1 = future1.get();
            String result2 = future2.get();
            String result3 = future3.get();
            
            assertNotNull(result1, "First result should not be null for " + storageName);
            assertNotNull(result2, "Second result should not be null for " + storageName);
            assertNotNull(result3, "Third result should not be null for " + storageName);
            
            assertTrue(result1.contains("value1"), "Should contain first value for " + storageName);
            assertTrue(result2.contains("value2"), "Should contain second value for " + storageName);
            assertTrue(result3.contains("value3"), "Should contain third value for " + storageName);
        }
    }

    @ParameterizedTest
    @MethodSource("databaseProvider")
    @DisplayName("Native async query non-blocking verification")
    void testNativeAsyncNonBlocking(GlueSQL database, String storageName) throws Exception {
        try (database) {
            // Setup test data
            database.query("CREATE TABLE nonblocking_test (id INTEGER, value TEXT)");
            database.query("INSERT INTO nonblocking_test VALUES (1, 'nonblocking_value')");

            // Record start time
            long startTime = System.currentTimeMillis();

            // Start async query
            CompletableFuture<String> future = database.queryAsync("SELECT * FROM nonblocking_test");

            // Immediately check that we're not blocked
            long immediateTime = System.currentTimeMillis();
            long timeDiff = immediateTime - startTime;
            
            // Should return immediately (within a few milliseconds)
            assertTrue(timeDiff < 100, 
                "Native async call should return immediately (took " + timeDiff + "ms) for " + storageName);
            
            // Wait for actual completion and verify result
            String result = future.get(5, TimeUnit.SECONDS);
            
            assertNotNull(result, "Async result should not be null for " + storageName);
            assertTrue(result.contains("nonblocking_value"), 
                "Should contain test data for " + storageName);
        }
    }

    @ParameterizedTest
    @MethodSource("databaseProvider")
    @DisplayName("Native async query chaining with thenCompose")
    void testNativeAsyncQueryChaining(GlueSQL database, String storageName) throws Exception {
        try (database) {
            // Setup test data
            database.query("CREATE TABLE users (id INTEGER, name TEXT)");
            database.query("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)");
            database.query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");
            database.query("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)");

            // Chain async queries - get user, then get their orders
            CompletableFuture<String> chainedResult = database
                .queryAsync("SELECT * FROM users WHERE id = 1")
                .thenCompose(userResult -> {
                    // Should contain user data
                    assertTrue(userResult.contains("Alice"), "Should contain user data for " + storageName);
                    
                    // Chain with another async query
                    return database.queryAsync("SELECT * FROM orders WHERE user_id = 1");
                })
                .thenApply(ordersResult -> {
                    // Transform the result
                    assertTrue(ordersResult.contains("100"), "Should contain order data for " + storageName);
                    assertTrue(ordersResult.contains("200"), "Should contain order data for " + storageName);
                    return "Processed: " + ordersResult;
                });

            String finalResult = chainedResult.get(10, TimeUnit.SECONDS);
            
            assertNotNull(finalResult, "Final result should not be null for " + storageName);
            assertTrue(finalResult.startsWith("Processed:"), "Should be processed result for " + storageName);
        }
    }
}