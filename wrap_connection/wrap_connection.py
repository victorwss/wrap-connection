from functools import wraps
from inject_globals import inject_globals

def transact(db_connect, cursor_var = "cursor", connection_var = "connection", cursor_factory = lambda c : c.cursor()):
    """A decorator that injects a connection and a cursor in the function call global objects scopes delimiting a transaction.
    Warning: There are no support yet for handling nested transactions.

    Keyword arguments:
    db_connect -- Function or lambda that is responsible for creating the actual connection.
    cursor_var -- The name of the injected cursor variable. Defaults to "cursor".
    connection_var -- The name of the injected connection variable. Defaults to "connection".
    cursor_factory -- The function that obtains a cursor from a connection. Defaults to a lambda that calls the parameterless cursor() method on the connection.

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

    class closing:
        def __init__(self, thing):
            self.__thing = thing
        def __enter__(self):
            return self.__thing
        def __exit__(self, *exc_info):
            self.__thing.close()

    def middle(func):
        @wraps(func)
        def inner(*args, **kwargs):
            with closing(db_connect()) as connection, closing(cursor_factory(connection)) as cursor:
                injects = {}
                injects[connection_var] = connection
                injects[cursor_var] = cursor
                return inject_globals(**injects)(func)(*args, **kwargs)
        return inner
    return middle