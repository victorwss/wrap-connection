from functools import wraps
from threading import local
from typing import Any, Callable, cast, Dict, Generic, Iterable, Iterator, List, Optional, Sequence, TypeVar, Text, Tuple, Union
from typing_extensions import Protocol
from mypy_extensions import KwArg, VarArg
import sys

SomeFunction = Callable[[VarArg(), KwArg()], Any]

_F = TypeVar("_F", bound = Callable[..., Any])

class SupportsClose(Protocol):
    def close(self, *args: Any, **kwargs: Any) -> None: ...

_T = TypeVar("_T", bound = SupportsClose)

class Cursor(Iterator[Any], Protocol):
    arraysize: Any
    connection: Any
    description: Any
    lastrowid: Any
    row_factory: Any
    rowcount: Any
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def close(self, *args: Any, **kwargs: Any) -> None: ...
    def execute(self, sql: str, parameters: Iterable[Any] = ()) -> 'Cursor': ...
    def executemany(self, sql: str, seq_of_parameters: Iterable[Iterable[Any]]) -> 'Cursor': ...
    def executescript(self, sql_script: Union[bytes, Text]) -> 'Cursor': ...
    def fetchall(self) -> List[Any]: ...
    def fetchmany(self, size: Optional[int] = None) -> List[Any]: ...
    def fetchone(self) -> Any: ...
    def setinputsizes(self, *args: Any, **kwargs: Any) -> None: ...
    def setoutputsize(self, *args: Any, **kwargs: Any) -> None: ...
    def __iter__(self) -> 'Cursor': ...
    if sys.version_info >= (3, 0):
        def __next__(self) -> Any: ...
    else:
        def next(self) -> Any: ...

_CUR = TypeVar("_CUR", bound = Cursor, covariant = True)
_CUR2 = TypeVar("_CUR2", bound = Cursor)

class Connection(Protocol, Generic[_CUR]):
    def cursor(self, cursorClass: Optional[type] = None) -> _CUR: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...

class ConnectionFactory(Protocol, Generic[_CUR]):
    def __call__(self) -> Connection[_CUR]: ...

class CursorFactory(Protocol, Generic[_CUR2]):
    def __call__(self, __in: Connection[_CUR2]) -> _CUR2: ...

def _call_cursor(c: Connection[_CUR2]) -> _CUR2:
    return c.cursor()

class Connected(Generic[_CUR]):
    def __init__(self, con: Connection[_CUR], cur: _CUR):
        self.__con: Connection[_CUR] = con
        self.__cur: _CUR = cur

    @property
    def connection(self) -> Connection[_CUR]:
        return self.__con

    @property
    def cursor(self) -> _CUR:
        return self.__cur

_initted = False

class DatabaseHolder:
    def __init__(self) -> None:
        if _initted: raise Exception()
        self.__store: local = local()

    def has(self, name: str = "default") -> bool:
        return hasattr(self.__store, name)

    def get(self, name: str = "default") -> Connected[Cursor]:
        if not self.has(name): raise KeyError(name)
        return cast(Connected[Cursor], getattr(self.__store, name))

    @property
    def connection(self) -> Connection[Cursor]:
        return self.get("default").connection

    @property
    def cursor(self) -> Cursor:
        return self.get("default").cursor

    def transact(self, db_connect: ConnectionFactory[_CUR], name: str = "default", cursor_factory: CursorFactory[_CUR] = _call_cursor) -> Callable[[_F], _F]:
        def middle(func: _F) -> _F:
            @wraps(func)
            def inner(*args: Any, **kwargs: Any) -> Any:
                if self.has(name): return func(*args, **kwargs)

                connection: Optional[Connection[_CUR]] = None
                cursor: Optional[_CUR] = None
                try:
                    connection = db_connect()
                    cursor = cursor_factory(connection)
                    setattr(self.__store, name, Connected(connection, cursor))
                    return func(*args, **kwargs)
                finally:
                    if cursor is not None: cursor.close()
                    if connection is not None: connection.close()
                    delattr(self.__store, name)

            return cast(_F, inner)
        return middle

def transact(db_connect: ConnectionFactory[_CUR], name: str = "default", cursor_factory: CursorFactory[_CUR] = _call_cursor) -> Callable[[_F], _F]:
    return Database.transact(db_connect, name, cursor_factory)

Database: DatabaseHolder = DatabaseHolder()
_initted = True