import itertools

from django.db import DatabaseError
from sheets_db import cache


class AnyCondition:
    def __init__(self, sql):
        pass

    def check_row(self, table, row):
        return True


class CursorTable:
    name = None
    row = None
    table = None
    condition = None

    def __init__(self, name, condition):
        self.name = name
        self.table = cache.DBCache.tables.get(name.lower(), None)
        if self.table is None:
            raise DatabaseError(f'Table {name} not found')
        self.row = -1
        self.condition = condition

    def __iter__(self):
        return self

    def __next__(self):
        if self.row is None:
            raise StopIteration()
        check_row = self.row + 1
        if len(self.table.data) <= check_row:
            raise StopIteration()
        if self.condition.check_row(self, self.table.data[check_row]):
            self.row = check_row
        else:
            self.row = None
            raise StopIteration()

    @property
    def current_row(self):
        if self.row is None:
            raise DatabaseError('Cursor error')
        return self.table.data[self.row]


class CursorField:
    name = None
    table = None
    number = None

    def __init__(self, name, label, table):
        self.name = name
        self.label = label
        self.table = table
        if name == 'id':
            self.number = -1
            return
        for i, field_name in enumerate(table.table.fields):
            if field_name.lower() == name:
                self.number = i
                break
        if self.number is None:
            raise DatabaseError(
                f'Field {name} not found in table {table.name}')

    def value(self):
        if self.number == -1:  # id field
            return self.table.row
        return self.table.current_row[self.number]


class Cursor:
    fields = None
    tables = None

    def __enter__(self):
        return self

    def __exit__(self, t, value, tb):
        self.close()
        return False

    def close(self):
        pass

    def execute(self, sql, params):
        if sql.startswith('SELECT '):
            result = sql[7:]
            split_result = result.split(' FROM ', 1)
            if len(split_result) != 2:
                raise DatabaseError("SELECT syntax error: no FROM")
            field_names, result = split_result
            field_names = [n.strip() for n in field_names.split(',')]
            split_result = result.split(' WHERE ', 1)
            if len(split_result) == 1:
                sources = split_result[0]
                where_sql = None
            else:
                sources, where_sql = split_result
            sources = sources.split(',')
            return self._execute_select(field_names, sources, where_sql)

    def _execute_select(self, columns, sources, conditions):
        condition = AnyCondition(conditions)
        self.tables = [
            CursorTable(name.lower(), condition) for name in sources]
        self.fields = []
        for column in columns:
            table_name, field_name = column.split('.')
            table_name = table_name.lower()
            table = None
            for i in self.tables:
                if i.name == table_name:
                    table = i
                    break
            if table is None:
                raise DatabaseError(
                    f'Field {column} not matches tables in FROM')
            self.fields.append(CursorField(field_name.lower(), column, table))

    def __next__(self):
        for table in self.tables:
            next(table)
        return tuple(f.value() for f in self.fields)

    def __iter__(self):
        return self

    def fetchone(self):
        try:
            self._next()
        except StopIteration:
            return None
        return self.get_row()

    def fetchmany(self, itersize):
        return list(itertools.islice(self, itersize))

    def fetchall(self):
        return list(self)
