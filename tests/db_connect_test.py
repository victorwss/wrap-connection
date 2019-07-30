import os
import sqlite3
from pytest import raises, mark
from typing import Any, List, TypeVar
from wrap_connection import Cursor, Connection, Database, transact
from threading import Thread
from queue import Queue

_CU = TypeVar("_CU", bound = Cursor, covariant = True)

class Person:
    def __init__(self, id: int, name: str) -> None:
        self.__id = id
        self.__name = name

    @property
    def id(self) -> int:
        return self.__id

    @property
    def name(self) -> str:
        return self.__name

db1 = "temp_1.db"
db2 = "temp_2.db"

def make_connection_2() -> Connection[sqlite3.Cursor]:
    return sqlite3.connect(db2)

create_table_sql = """
CREATE TABLE IF NOT EXISTS persons (
    id integer PRIMARY KEY,
    name text NOT NULL
); """

@transact(make_connection_2)
def create_table_2() -> None:
    Database.cursor.execute(create_table_sql)
    Database.connection.commit()

def test_simple() -> None:
    if os.path.exists(db1): os.remove(db1)

    def make_connection_1() -> Connection[sqlite3.Cursor]:
        return sqlite3.connect(db1)

    @transact(make_connection_1)
    def create_table_1() -> None:
        Database.cursor.execute(create_table_sql)
        Database.connection.commit()

    @transact(make_connection_1)
    def some_operation(person: Person) -> None:
        insert_sql = "INSERT INTO persons (id, name) VALUES (?, ?)"
        Database.cursor.execute(insert_sql, (person.id, person.name))
        Database.connection.commit()

    @transact(make_connection_1)
    def some_other_operation() -> List[Person]:
        select_sql = "SELECT id, name FROM persons"
        result = []
        Database.cursor.execute(select_sql)
        for r in Database.cursor.fetchall():
            result.append(Person(r[0], r[1]))
        return result

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
        if os.path.exists(db1): os.remove(db1)
        assert not os.path.exists(db1)

def test_custom_cursor() -> None:
    if os.path.exists(db1): os.remove(db1)
    some_counter: int = 0

    def make_cursor(con: Connection[sqlite3.Cursor]) -> sqlite3.Cursor:
        nonlocal some_counter
        some_counter += 1
        return con.cursor()

    def make_connection_3() -> Connection[sqlite3.Cursor]:
        return sqlite3.connect(db1)

    @transact(db_connect = make_connection_3, cursor_factory = make_cursor)
    def another_op() -> None:
        assert some_counter == 6
        Database.cursor.execute(create_table_sql)
        Database.connection.commit()
        insert_sql = "INSERT INTO persons (id, name) VALUES (4, 'Foe')"
        Database.cursor.execute(insert_sql)
        Database.connection.commit()
        select_sql = "SELECT id, name FROM persons"
        Database.cursor.execute(select_sql)
        result = Database.cursor.fetchone()
        assert result[0] == 4
        assert result[1] == 'Foe'

    try:
        assert not os.path.exists(db1)
        some_counter = 5
        another_op()
        assert os.path.exists(db1)
    finally:
        if os.path.exists(db1): os.remove(db1)
        assert not os.path.exists(db1)

@mark.timeout(2)
def test_thread_isolation() -> None:
    if os.path.exists(db1): os.remove(db1)

    p: Queue[Any] = Queue()
    q: Queue[Any] = Queue()
    r: Queue[Any] = Queue()
    s: Queue[Any] = Queue()

    t: Queue[Any] = Queue()

    def make_connection_1() -> Connection[sqlite3.Cursor]:
        return sqlite3.connect(db1)

    t1: Thread
    t2: Thread
    t3: Thread

    @transact(make_connection_1)
    def inner3() -> None:
        try:
            t1.join()
            t2.join()
            select_sql = "SELECT id, name FROM persons"
            Database.cursor.execute(select_sql)
            result = Database.cursor.fetchone()
            assert result[0] == 44
            assert result[1] == 'Xorg'
        except BaseException as xxxx:
            t.put(xxxx)
        else:
            t.put(True)

    @transact(make_connection_1)
    def inner1() -> None:
        try:
            Database.cursor.execute(create_table_sql)
            Database.connection.commit()
            insert_sql = "INSERT INTO persons (id, name) VALUES (44, 'Xoom')"
            Database.cursor.execute(insert_sql)
            Database.connection.commit()
            q.put(Database.connection)
        except BaseException as xxxx:
            r.put(xxxx)
            q.put(xxxx)
            return
        try:
            assert p.get() == 123
            select_sql = "SELECT id, name FROM persons"
            Database.cursor.execute(select_sql)
            result = Database.cursor.fetchone()
            assert result[0] == 44
            assert result[1] == 'Xorg'
        except BaseException as xxxx:
            r.put(xxxx)
        else:
            r.put(True)

    @transact(make_connection_1)
    def inner2() -> None:
        try:
            c = q.get()
            assert Database.connection is not c
            select_sql = "SELECT id, name FROM persons"
            Database.cursor.execute(select_sql)
            result = Database.cursor.fetchone()
            assert result[0] == 44
            assert result[1] == 'Xoom'
            update_sql = "UPDATE persons SET name = 'Xorg' WHERE id = 44"
            Database.cursor.execute(update_sql)
            Database.connection.commit()
            p.put(123)
        except BaseException as xxxx:
            s.put(xxxx)
            p.put(555)
        else:
            s.put(True)

    t1 = Thread(target = inner1)
    t2 = Thread(target = inner2)
    t3 = Thread(target = inner3)
    t2.start()
    t1.start()
    t3.start()
    t1.join()
    t2.join()

    assert s.get() is True
    assert r.get() is True
    t3.join()
    assert t.get() is True

    assert os.path.exists(db1)
    if os.path.exists(db1): os.remove(db1)
    assert not os.path.exists(db1)

@mark.timeout(2)
def test_reentrancy() -> None:

    i: int = 0
    x: Any = None
    y: Any = None

    def make_connection_1() -> Connection[sqlite3.Cursor]:
        nonlocal i
        i += 1
        return sqlite3.connect(db1)

    @transact(make_connection_1)
    def t1():
        nonlocal y
        y = Database.connection
        assert x is y

    @transact(make_connection_1)
    def t2():
        nonlocal x
        x = Database.connection
        t1()

    t2()