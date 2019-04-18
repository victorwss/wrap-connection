# wrap-connection
Easily wrap connections and cursors by using decorators.

## Usage example:

A simple usage is this:

```python
def make_connection():
    import sqlite3
    return sqlite3.connect("test.db")

@transact(make_connection, "cur", "con")
def some_operation(person):
    insert_sql = "INSERT INTO persons (id, name) VALUES (?, ?)"
    cur.execute(insert_sql, (person.id, person.name))
    con.commit()

@transact(make_connection)
def some_other_operation():
    select_sql = "SELECT id, name FROM persons"
    result = []
    cursor.execute(select_sql)
    for r in cursor.fetchall():
        result.append(Person(r[0], r[1]))
    return result
```

Those will delimite a transaction in the `some_operation` and `some_other_operation` functions. The `make_connection` function is used to get the connection for each transaction.