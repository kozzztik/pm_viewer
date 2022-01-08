import itertools
import datetime

from django.db import DatabaseError
from django.db import models

from sheets_db.backend import where


class CursorTable:
    name = None
    row = None
    table = None
    condition = None

    def __init__(self, connection, name, condition):
        self.name = name
        self.table = connection.tables.get(name.lower(), None)
        if self.table is None:
            raise DatabaseError(f'Table {name} not found')
        self.row = -1
        self.condition = condition
        self.fields = {}

    def __iter__(self):
        return self

    def __next__(self):
        if self.row is None:
            raise StopIteration()
        check_row = self.row + 1
        while check_row < len(self.table.data):
            if self.condition.check_row(self, check_row):
                self.row = check_row
                return
            check_row += 1
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

    def __init__(self, column, name, label, table):
        self.field = column.output_field
        self.name = name
        self.label = label
        self.table = table
        if name == 'id':
            self.number = -1
            return
        for i, field_name in enumerate(table.table.fields):
            if field_name.lower() == name:
                self.number = i
                table.fields[i] = self
                break
        if self.number is None:
            raise DatabaseError(
                f'Field {name} not found in table {table.name}')

    def value(self, row_number=None):
        if row_number is None:
            row_number = self.table.row
        if self.number == -1:  # id field
            return self.table.row
        value = self.table.table.data[row_number][self.number]
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
        condition = where.WhereNode(selector.where, self.connection, selector)
        self.tables = [
            CursorTable(self.connection, name.lower(), condition)
            for name in selector.tables[0]]
        self.fields = []
        for column in selector.columns:
            table_name, field_name = column[0].split('.')
            table_name = table_name.lower()
            table = None
            for i in self.tables:
                if i.name == table_name:
                    table = i
                    break
            if table is None:
                raise DatabaseError(
                    f'Field {column} not matches tables in FROM')
            self.fields.append(CursorField(
                column[1], field_name.lower(), column, table))

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
