package examples;

import org.gluesql.*;
import java.util.List;

/**
 * Basic example demonstrating GlueSQL Java bindings usage.
 */
public class BasicExample {
    
    public static void main(String[] args) {
        try {
            // Create an in-memory database
            System.out.println("Creating in-memory GlueSQL database...");
            GlueSQL db = GlueSQL.newMemory();
            
            // Create a table
            System.out.println("Creating users table...");
            db.query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
            
            // Insert some data
            System.out.println("Inserting data...");
            QueryResult insertResult = db.execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)");
            System.out.println("Inserted " + insertResult.getAffectedRows() + " rows");
            
            // Query all users
            System.out.println("\nQuerying all users:");
            QueryResult selectResult = db.execute("SELECT * FROM users ORDER BY age");
            
            if (selectResult.isSelectResult()) {
                List<String> columns = selectResult.getSelectLabels();
                List<List<Object>> rows = selectResult.getSelectRows();
                
                // Print column headers
                System.out.println("Columns: " + String.join(", ", columns));
                
                // Print each row
                for (List<Object> row : rows) {
                    System.out.println("Row: " + row);
                }
            }
            
            // Query with WHERE clause
            System.out.println("\nQuerying users older than 28:");
            QueryResult filteredResult = db.execute("SELECT name, age FROM users WHERE age > 28");
            
            if (filteredResult.isSelectResult()) {
                List<List<Object>> rows = filteredResult.getSelectRows();
                for (List<Object> row : rows) {
                    System.out.println("Name: " + row.get(0) + ", Age: " + row.get(1));
                }
            }
            
            // Count users
            System.out.println("\nCounting users:");
            QueryResult countResult = db.execute("SELECT COUNT(*) as total FROM users");
            if (countResult.isSelectResult()) {
                List<List<Object>> rows = countResult.getSelectRows();
                System.out.println("Total users: " + rows.get(0).get(0));
            }
            
            // Update a user
            System.out.println("\nUpdating Bob's age:");
            QueryResult updateResult = db.execute("UPDATE users SET age = 26 WHERE name = 'Bob'");
            System.out.println("Updated " + updateResult.getAffectedRows() + " rows");
            
            // Delete a user
            System.out.println("\nDeleting Charlie:");
            QueryResult deleteResult = db.execute("DELETE FROM users WHERE name = 'Charlie'");
            System.out.println("Deleted " + deleteResult.getAffectedRows() + " rows");
            
            // Final count
            System.out.println("\nFinal user count:");
            QueryResult finalCountResult = db.execute("SELECT COUNT(*) as total FROM users");
            if (finalCountResult.isSelectResult()) {
                List<List<Object>> rows = finalCountResult.getSelectRows();
                System.out.println("Total users: " + rows.get(0).get(0));
            }
            
            System.out.println("\nExample completed successfully!");
            
        } catch (GlueSQLException e) {
            System.err.println("Database error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
