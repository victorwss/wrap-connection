import os
from wrap_connection import transact

class Person:
    def __init__(self, id, name):
        self.__id = id
        self.__name = name

    @property
    def id(self):
        return self.__id

    @property
    def name(self):
        return self.__name

db1 = "temp_1.db"
db2 = "temp_2.db"

def make_connection_1():
    import sqlite3
    return sqlite3.connect(db1)

def make_connection_2():
    import sqlite3
    return sqlite3.connect(db2)

create_table_sql = """
CREATE TABLE IF NOT EXISTS persons (
    id integer PRIMARY KEY,
    name text NOT NULL
); """

@transact(make_connection_1)
def create_table_1():
    cursor.execute(create_table_sql)
    connection.commit()

@transact(make_connection_2)
def create_table_2():
    cursor.execute(create_table_sql)
    connection.commit()

@transact(make_connection_1, "cur", "con")
def some_operation(person):
    insert_sql = "INSERT INTO persons (id, name) VALUES (?, ?)"
    cur.execute(insert_sql, (person.id, person.name))
    con.commit()

@transact(make_connection_1)
def some_other_operation():
    select_sql = "SELECT id, name FROM persons"
    result = []
    cursor.execute(select_sql)
    for r in cursor.fetchall():
        result.append(Person(r[0], r[1]))
    return result

def test_simple():

    try:
        assert not os.path.exists(db1)
        make_connection_1()
        assert os.path.exists(db1)
        create_table_1()
        some_operation(Person(1, "Joe"))
        some_operation(Person(2, "Zoe"))
        some_operation(Person(3, "Moe"))
        t = some_other_operation()
        assert t[0].id == 1
        assert t[0].name == "Joe"
        assert t[1].id == 2
        assert t[1].name == "Zoe"
        assert t[2].id == 3
        assert t[2].name == "Moe"
    finally:
        if os.path.exists(db1):
            os.remove(db1)
        assert not os.path.exists(db1)

some_counter = 0

def make_cursor(con):
    global some_counter
    some_counter += 1
    return con.cursor()

def make_connection_3():
    import sqlite3
    return {"db_connect": lambda : sqlite3.connect(db1), "cursor_factory": make_cursor}

@transact(**make_connection_3())
def another_op():
    assert some_counter == 6
    cursor.execute(create_table_sql)
    connection.commit()
    insert_sql = "INSERT INTO persons (id, name) VALUES (4, 'Foe')"
    cursor.execute(insert_sql)
    connection.commit()
    select_sql = "SELECT id, name FROM persons"
    result = []
    cursor.execute(select_sql)
    r = cursor.fetchone()
    assert r[0] == 4
    assert r[1] == 'Foe'

def test_custom_cursor():
    try:
        global some_counter
        assert not os.path.exists(db1)
        some_counter = 5
        another_op()
        assert os.path.exists(db1)
    finally:
        if os.path.exists(db1):
            os.remove(db1)
        assert not os.path.exists(db1)