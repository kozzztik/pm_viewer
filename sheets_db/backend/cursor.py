import itertools
import datetime

from django.db import DatabaseError
from django.db import models

from sheets_db.backend import where


class CursorField:
    name = None
    table = None
    number = None

    def __init__(self, cursor, full_name, column):
        self.field = column.output_field
        self.full_name = full_name.lower()
        table_name, self.name = self.full_name.split('.')
        self.table = cursor.tables.get(table_name)
        if self.table is None:
            raise DatabaseError(
                f'Field {column} not matches tables in FROM')
        if self.name == 'id':
            self.number = -1
            return
        for i, field_name in enumerate(self.table.field_names):
            if field_name.lower() == self.name:
                self.number = i
                break
        else:
            raise DatabaseError(
                f'Field {self.name} not found in table {table_name}')

    def __str__(self):
        return f'Cursor field {self.full_name}'

    def value(self, row_number=None):
        if row_number is None:
            row_number = self.table.row
        if self.number == -1:  # id field
            return row_number
        value = self.table.data[row_number][self.number]
        if value and isinstance(self.field, models.DateField):
            if isinstance(value, str):
                value = datetime.datetime.strptime(value, '%d.%m.%Y')
            else:
                value = datetime.datetime(1899, 12, 30) + datetime.timedelta(
                    value)
        return value


class Cursor:
    fields = None
    tables = None
    connection = None

    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, t, value, tb):
        self.close()
        return False

    def close(self):
        pass

    def execute(self, sql, params):
        if sql.action == 'SELECT':
            return self._execute_select(sql)
        raise NotImplementedError('WTF')

    def _execute_select(self, selector):
        self.tables = self.connection.get_tables(selector.tables[0])
        self.fields = []
        for full_name, column in selector.columns:
            self.fields.append(CursorField(self, full_name, column))
        # apply condition context
        condition = where.WhereNode(selector.where, self, selector)
        for table in self.tables.values():
            table.condition = condition

    def __next__(self):
        for table in self.tables.values():
            next(table)
        return tuple(f.value() for f in self.fields)

    def __iter__(self):
        return self

    def fetchone(self):
        try:
            return next(self)
        except StopIteration:
            return None

    def fetchmany(self, itersize):
        return list(itertools.islice(self, itersize))

    def fetchall(self):
        return list(self)
