package org.gluesql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/**
 * Represents the result of a GlueSQL query execution.
 * 
 * This class provides convenient access to query results without requiring
 * users to manually parse JSON responses.
 */
public class QueryResult {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final List<Map<String, Object>> results;

    private QueryResult(List<Map<String, Object>> results) {
        this.results = results;
    }

    /**
     * Create a QueryResult from a JSON string returned by GlueSQL.
     * 
     * @param json JSON string containing query results
     * @return QueryResult object
     * @throws GlueSQLException if JSON parsing fails
     */
    public static QueryResult fromJson(String json) throws GlueSQLException {
        try {
            JsonNode rootNode = objectMapper.readTree(json);
            List<Map<String, Object>> results = new ArrayList<>();
            
            if (rootNode.isArray()) {
                for (JsonNode resultNode : rootNode) {
                    Map<String, Object> result = objectMapper.convertValue(resultNode, Map.class);
                    results.add(result);
                }
            }
            
            return new QueryResult(results);
        } catch (Exception e) {
            throw new GlueSQLException("Failed to parse query result JSON", e);
        }
    }

    /**
     * Get the number of result sets returned by the query.
     * 
     * @return Number of result sets
     */
    public int getResultCount() {
        return results.size();
    }

    /**
     * Get a specific result set by index.
     * 
     * @param index Index of the result set (0-based)
     * @return Map containing the result data
     * @throws IndexOutOfBoundsException if index is invalid
     */
    public Map<String, Object> getResult(int index) {
        return results.get(index);
    }

    /**
     * Get all result sets.
     * 
     * @return List of all result maps
     */
    public List<Map<String, Object>> getAllResults() {
        return new ArrayList<>(results);
    }

    /**
     * Get the type of the first result (e.g., "Select", "Insert", "Update").
     * 
     * @return Result type string, or null if no results
     */
    public String getFirstResultType() {
        if (results.isEmpty()) {
            return null;
        }
        return (String) results.get(0).get("type");
    }

    /**
     * Check if this result contains SELECT query data.
     * 
     * @return true if the first result is a SELECT result
     */
    public boolean isSelectResult() {
        return "Select".equals(getFirstResultType());
    }

    /**
     * Get the rows from a SELECT query result.
     * 
     * @return List of rows, or empty list if not a SELECT result
     */
    @SuppressWarnings("unchecked")
    public List<List<Object>> getSelectRows() {
        if (!isSelectResult() || results.isEmpty()) {
            return new ArrayList<>();
        }
        
        Object rows = results.get(0).get("rows");
        if (rows instanceof List) {
            return (List<List<Object>>) rows;
        }
        
        return new ArrayList<>();
    }

    /**
     * Get the column labels from a SELECT query result.
     * 
     * @return List of column labels, or empty list if not a SELECT result
     */
    @SuppressWarnings("unchecked")
    public List<String> getSelectLabels() {
        if (!isSelectResult() || results.isEmpty()) {
            return new ArrayList<>();
        }
        
        Object labels = results.get(0).get("labels");
        if (labels instanceof List) {
            return (List<String>) labels;
        }
        
        return new ArrayList<>();
    }

    /**
     * Get the number of affected rows for INSERT/UPDATE/DELETE operations.
     * 
     * @return Number of affected rows, or 0 if not applicable
     */
    public int getAffectedRows() {
        if (results.isEmpty()) {
            return 0;
        }
        
        Map<String, Object> firstResult = results.get(0);
        String type = (String) firstResult.get("type");
        
        if ("Insert".equals(type)) {
            Object insertedRows = firstResult.get("inserted_rows");
            return insertedRows instanceof Number ? ((Number) insertedRows).intValue() : 0;
        } else if ("Update".equals(type)) {
            Object updatedRows = firstResult.get("updated_rows");
            return updatedRows instanceof Number ? ((Number) updatedRows).intValue() : 0;
        } else if ("Delete".equals(type)) {
            Object deletedRows = firstResult.get("deleted_rows");
            return deletedRows instanceof Number ? ((Number) deletedRows).intValue() : 0;
        }
        
        return 0;
    }

    /**
     * Convert the results back to JSON string.
     * 
     * @return JSON representation of the results
     */
    public String toJson() {
        try {
            return objectMapper.writeValueAsString(results);
        } catch (Exception e) {
            return "[]";
        }
    }

    @Override
    public String toString() {
        return "QueryResult{resultCount=" + results.size() + ", type=" + getFirstResultType() + "}";
    }
}
