package org.gluesql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for QueryResult class.
 */
public class QueryResultTest {

    @Test
    @DisplayName("Parse SELECT result JSON")
    void testParseSelectResult() throws GlueSQLException {
        String selectJson = """
            [
                {
                    "type": "Select",
                    "labels": ["id", "name", "age"],
                    "rows": [
                        [1, "Alice", 30],
                        [2, "Bob", 25]
                    ]
                }
            ]
            """;

        QueryResult result = QueryResult.fromJson(selectJson);
        
        assertEquals(1, result.getResultCount());
        assertTrue(result.isSelectResult());
        assertEquals("Select", result.getFirstResultType());
        
        List<String> labels = result.getSelectLabels();
        assertEquals(3, labels.size());
        assertEquals("id", labels.get(0));
        assertEquals("name", labels.get(1));
        assertEquals("age", labels.get(2));
        
        List<List<Object>> rows = result.getSelectRows();
        assertEquals(2, rows.size());
        
        List<Object> firstRow = rows.get(0);
        assertEquals(1, ((Number) firstRow.get(0)).intValue());
        assertEquals("Alice", firstRow.get(1));
        assertEquals(30, ((Number) firstRow.get(2)).intValue());
    }

    @Test
    @DisplayName("Parse INSERT result JSON")
    void testParseInsertResult() throws GlueSQLException {
        String insertJson = """
            [
                {
                    "type": "Insert",
                    "inserted_rows": 3
                }
            ]
            """;

        QueryResult result = QueryResult.fromJson(insertJson);
        
        assertEquals(1, result.getResultCount());
        assertFalse(result.isSelectResult());
        assertEquals("Insert", result.getFirstResultType());
        assertEquals(3, result.getAffectedRows());
    }

    @Test
    @DisplayName("Parse UPDATE result JSON")
    void testParseUpdateResult() throws GlueSQLException {
        String updateJson = """
            [
                {
                    "type": "Update",
                    "updated_rows": 2
                }
            ]
            """;

        QueryResult result = QueryResult.fromJson(updateJson);
        
        assertEquals(1, result.getResultCount());
        assertEquals("Update", result.getFirstResultType());
        assertEquals(2, result.getAffectedRows());
    }

    @Test
    @DisplayName("Parse DELETE result JSON")
    void testParseDeleteResult() throws GlueSQLException {
        String deleteJson = """
            [
                {
                    "type": "Delete",
                    "deleted_rows": 1
                }
            ]
            """;

        QueryResult result = QueryResult.fromJson(deleteJson);
        
        assertEquals(1, result.getResultCount());
        assertEquals("Delete", result.getFirstResultType());
        assertEquals(1, result.getAffectedRows());
    }

    @Test
    @DisplayName("Parse CREATE result JSON")
    void testParseCreateResult() throws GlueSQLException {
        String createJson = """
            [
                {
                    "type": "Create",
                    "result": "Success"
                }
            ]
            """;

        QueryResult result = QueryResult.fromJson(createJson);
        
        assertEquals(1, result.getResultCount());
        assertEquals("Create", result.getFirstResultType());
        assertEquals(0, result.getAffectedRows()); // CREATE doesn't affect rows
    }

    @Test
    @DisplayName("Parse DROP result JSON")
    void testParseDropResult() throws GlueSQLException {
        String dropJson = """
            [
                {
                    "type": "Drop",
                    "result": "Success"
                }
            ]
            """;

        QueryResult result = QueryResult.fromJson(dropJson);
        
        assertEquals(1, result.getResultCount());
        assertEquals("Drop", result.getFirstResultType());
    }

    @Test
    @DisplayName("Parse multiple results JSON")
    void testParseMultipleResults() throws GlueSQLException {
        String multipleJson = """
            [
                {
                    "type": "Insert",
                    "inserted_rows": 2
                },
                {
                    "type": "Select",
                    "labels": ["count"],
                    "rows": [[2]]
                }
            ]
            """;

        QueryResult result = QueryResult.fromJson(multipleJson);
        
        assertEquals(2, result.getResultCount());
        assertEquals("Insert", result.getFirstResultType()); // First result type
        
        // Check first result
        Map<String, Object> firstResult = result.getResult(0);
        assertEquals("Insert", firstResult.get("type"));
        assertEquals(2, ((Number) firstResult.get("inserted_rows")).intValue());
        
        // Check second result
        Map<String, Object> secondResult = result.getResult(1);
        assertEquals("Select", secondResult.get("type"));
    }

    @Test
    @DisplayName("Handle empty result JSON")
    void testEmptyResult() throws GlueSQLException {
        String emptyJson = "[]";

        QueryResult result = QueryResult.fromJson(emptyJson);
        
        assertEquals(0, result.getResultCount());
        assertNull(result.getFirstResultType());
        assertFalse(result.isSelectResult());
        assertEquals(0, result.getAffectedRows());
        assertTrue(result.getSelectRows().isEmpty());
        assertTrue(result.getSelectLabels().isEmpty());
    }

    @Test
    @DisplayName("Handle SELECT with no rows")
    void testSelectWithNoRows() throws GlueSQLException {
        String noRowsJson = """
            [
                {
                    "type": "Select",
                    "labels": ["id", "name"],
                    "rows": []
                }
            ]
            """;

        QueryResult result = QueryResult.fromJson(noRowsJson);
        
        assertTrue(result.isSelectResult());
        assertEquals(2, result.getSelectLabels().size());
        assertTrue(result.getSelectRows().isEmpty());
    }

    @Test
    @DisplayName("Convert result back to JSON")
    void testToJson() throws GlueSQLException {
        String originalJson = """
            [
                {
                    "type": "Select",
                    "labels": ["id"],
                    "rows": [[1]]
                }
            ]
            """;

        QueryResult result = QueryResult.fromJson(originalJson);
        String convertedJson = result.toJson();
        
        assertNotNull(convertedJson);
        assertFalse(convertedJson.isEmpty());
        assertTrue(convertedJson.contains("Select"));
    }

    @Test
    @DisplayName("Error handling for invalid JSON")
    void testInvalidJson() {
        String invalidJson = "{ invalid json }";
        
        assertThrows(GlueSQLException.class, () -> {
            QueryResult.fromJson(invalidJson);
        });
    }

    @Test
    @DisplayName("Test toString method")
    void testToString() throws GlueSQLException {
        String selectJson = """
            [
                {
                    "type": "Select",
                    "labels": ["id"],
                    "rows": [[1]]
                }
            ]
            """;

        QueryResult result = QueryResult.fromJson(selectJson);
        String toString = result.toString();
        
        assertNotNull(toString);
        assertTrue(toString.contains("QueryResult"));
        assertTrue(toString.contains("Select"));
        assertTrue(toString.contains("resultCount=1"));
    }

    @Test
    @DisplayName("Test getAllResults method")
    void testGetAllResults() throws GlueSQLException {
        String multipleJson = """
            [
                {
                    "type": "Create",
                    "result": "Success"
                },
                {
                    "type": "Insert",
                    "inserted_rows": 1
                }
            ]
            """;

        QueryResult result = QueryResult.fromJson(multipleJson);
        List<Map<String, Object>> allResults = result.getAllResults();
        
        assertEquals(2, allResults.size());
        assertEquals("Create", allResults.get(0).get("type"));
        assertEquals("Insert", allResults.get(1).get("type"));
        
        // Verify it's a copy (modifying returned list shouldn't affect original)
        allResults.clear();
        assertEquals(2, result.getResultCount()); // Original should be unchanged
    }

    @Test
    @DisplayName("Test complex SELECT with various data types")
    void testComplexSelectResult() throws GlueSQLException {
        String complexJson = """
            [
                {
                    "type": "Select",
                    "labels": ["id", "name", "active", "score", "metadata"],
                    "rows": [
                        [1, "Alice", true, 95.5, null],
                        [2, "Bob", false, 87.2, "some data"]
                    ]
                }
            ]
            """;

        QueryResult result = QueryResult.fromJson(complexJson);
        
        assertTrue(result.isSelectResult());
        
        List<String> labels = result.getSelectLabels();
        assertEquals(5, labels.size());
        
        List<List<Object>> rows = result.getSelectRows();
        assertEquals(2, rows.size());
        
        // Check first row with various data types
        List<Object> firstRow = rows.get(0);
        assertEquals(1, ((Number) firstRow.get(0)).intValue());
        assertEquals("Alice", firstRow.get(1));
        assertEquals(true, firstRow.get(2));
        assertEquals(95.5, ((Number) firstRow.get(3)).doubleValue(), 0.1);
        assertNull(firstRow.get(4));
        
        // Check second row
        List<Object> secondRow = rows.get(1);
        assertEquals("Bob", secondRow.get(1));
        assertEquals(false, secondRow.get(2));
        assertEquals("some data", secondRow.get(4));
    }
}
