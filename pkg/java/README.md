# GlueSQL Java Bindings

This package provides Java bindings for [GlueSQL](https://github.com/gluesql/gluesql), an open-source SQL database engine written in Rust.

## Features

- **Multiple Storage Backends**: Support for Memory, Sled, JSON, and SharedMemory storage
- **Full SQL Support**: Execute standard SQL queries with GlueSQL's powerful engine
- **Easy Integration**: Simple Java API with native performance
- **Exception Handling**: Java-friendly error handling with GlueSQLException
- **Result Parsing**: Convenient QueryResult class for handling query results

## Installation

### Prerequisites

- Java 11 or higher
- Rust toolchain (for building from source)

### Using Gradle

Add to your `build.gradle`:

```gradle
dependencies {
    implementation 'org.gluesql:gluesql-java:0.17.0'
}
```

### Building from Source

1. Clone the GlueSQL repository
2. Navigate to the Java bindings directory:
   ```bash
   cd pkg/java
   ```
3. Build the project:
   ```bash
   ./gradlew build
   ```

## Quick Start

### Basic Usage

```java
import org.gluesql.*;

public class Example {
    public static void main(String[] args) {
        try {
            // Create an in-memory database
            GlueSQL db = GlueSQL.newMemory();
            
            // Create a table
            db.query("CREATE TABLE users (id INTEGER, name TEXT)");
            
            // Insert data
            db.query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");
            
            // Query data
            QueryResult result = db.execute("SELECT * FROM users");
            
            if (result.isSelectResult()) {
                System.out.println("Columns: " + result.getSelectLabels());
                System.out.println("Rows: " + result.getSelectRows());
            }
            
        } catch (GlueSQLException e) {
            System.err.println("Database error: " + e.getMessage());
        }
    }
}
```

### Storage Backends

#### Memory Storage
```java
GlueSQL db = GlueSQL.newMemory();
```

#### Sled Storage (Persistent)
```java
GlueSQL db = GlueSQL.newSled("/path/to/database");
```

#### JSON Storage
```java
GlueSQL db = GlueSQL.newJson("/path/to/json/storage");
```

#### Shared Memory Storage
```java
GlueSQL db = GlueSQL.newSharedMemory("my_namespace");
```

### Working with Results

```java
// Execute a query
QueryResult result = db.execute("SELECT COUNT(*) as total FROM users");

// Check result type
if (result.isSelectResult()) {
    List<String> columns = result.getSelectLabels();
    List<List<Object>> rows = result.getSelectRows();
    
    // Access first row, first column
    Object count = rows.get(0).get(0);
    System.out.println("Total users: " + count);
}

// For INSERT/UPDATE/DELETE operations
QueryResult updateResult = db.execute("INSERT INTO users VALUES (3, 'Charlie')");
int affectedRows = updateResult.getAffectedRows();
System.out.println("Inserted " + affectedRows + " rows");
```

### Error Handling

```java
try {
    GlueSQL db = GlueSQL.newSled("/invalid/path");
    db.query("INVALID SQL SYNTAX");
} catch (GlueSQLException e) {
    System.err.println("Error: " + e.getMessage());
    e.printStackTrace();
}
```

## API Reference

### GlueSQL Class

#### Static Factory Methods
- `newMemory()` - Create in-memory database
- `newSled(String path)` - Create Sled-backed database
- `newJson(String path)` - Create JSON-backed database  
- `newSharedMemory(String namespace)` - Create shared memory database

#### Instance Methods
- `query(String sql)` - Execute SQL and return JSON string
- `execute(String sql)` - Execute SQL and return QueryResult object

### QueryResult Class

#### Result Information
- `getResultCount()` - Number of result sets
- `getFirstResultType()` - Type of first result ("Select", "Insert", etc.)
- `isSelectResult()` - Check if result contains SELECT data

#### SELECT Results
- `getSelectLabels()` - Column names
- `getSelectRows()` - Row data
- `getResult(int index)` - Get specific result set

#### Modification Results  
- `getAffectedRows()` - Number of rows affected by INSERT/UPDATE/DELETE

#### Utility Methods
- `toJson()` - Convert result to JSON string
- `getAllResults()` - Get all result sets

### GlueSQLException

Standard Java exception with message and optional cause for error handling.

## Examples

See the `examples/` directory for more comprehensive examples including:
- Basic CRUD operations
- Transaction handling
- Multiple storage backend usage
- Complex query examples

## Building Native Library

The Java bindings use JNI to call into the Rust GlueSQL library. The build process automatically:

1. Compiles the Rust code to a native library
2. Copies the library to Java resources
3. Includes the library in the final JAR

To manually build just the native library:
```bash
cargo build --release
```

## Contributing

Contributions are welcome! Please see the main [GlueSQL repository](https://github.com/gluesql/gluesql) for contribution guidelines.

## License

Apache License 2.0 - see the LICENSE file in the main repository.
