pip install ./ --upgrade
mypy --disallow-untyped-defs --disallow-untyped-calls --disallow-incomplete-defs --check-untyped-defs --disallow-untyped-decorators --strict --show-traceback wrap_connection/wrap_connection.py tests/db_connect_test.py
pytest