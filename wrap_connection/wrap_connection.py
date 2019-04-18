from functools import wraps
from contextlib import closing
from inject_globals import inject_globals

def transact(db_connect, cursor_var = "cursor", connection_var = "connection"):
    """A decorator that injects a connection and a cursor in the function call global objects scopes delimiting a transaction.
    Warning: There are no support yet for handling nested transactions.

    Keyword arguments:
    db_connect -- Function or lambda that is responsible for creating the actual connection.
    cursor_var -- The name of the injected cursor variable. Defaults to "cursor".
    connection_var -- The name of the injected connection variable. Defaults to "connection".

    Example usage:

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
    """

    def middle(func):
        @wraps(func)
        def inner(*args, **kwargs):
            with closing(db_connect()) as connection, closing(connection.cursor()) as cursor:
                injects = {}
                injects[connection_var] = connection
                injects[cursor_var] = cursor
                return inject_globals(**injects)(func)(*args, **kwargs)
        return inner
    return middle