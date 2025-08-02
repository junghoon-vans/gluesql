# GlueSQL Java Bindings

GlueSQL Java Bindings is a Java wrapper for the [GlueSQL](https://github.com/gluesql/gluesql) database engine. It provides an embedded SQL database that works with a selection of storage backends.

Supported storages:

- `MemoryStorage`
- `SledStorage` 
- `JsonStorage`
- `RedbStorage`
- `SharedMemoryStorage`

Learn more at **<https://gluesql.org/docs>**.

## Usage

```java
import org.gluesql.*;

public class Example {
    public static void main(String[] args) {
        try (GlueSQL db = GlueSQL.newMemory()) {
            db.query("""
                CREATE TABLE User (id INTEGER, name TEXT);
                INSERT INTO User VALUES (1, 'Hello'), (2, 'World');
                """);
            
            String result = db.query("SELECT * FROM User;");
            System.out.println(result);
            
        } catch (GlueSQLException e) {
            System.err.println("Database error: " + e.getMessage());
        }
    }
}
```

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](https://raw.githubusercontent.com/gluesql/gluesql/main/LICENSE) file for details.
