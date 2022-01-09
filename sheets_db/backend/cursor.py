import itertools
import datetime

from django.db import DatabaseError
from django.db import models
from django.db.models.sql import datastructures

from sheets_db.backend import expressions


class BaseField:
    column = None
    alias = None
    cursor = None

    def __init__(self, cursor, alias, column):
        self.column = column
        self.alias = alias.lower()
        self.cursor = cursor

    @property
    def value(self):
        raise NotImplemented()

    def __str__(self):
        return f'Cursor field {self.alias}'


class CursorField(BaseField):
    table = None
    number = None

    def __init__(self, cursor, alias, column):
        super(CursorField, self).__init__(cursor, alias, column)
        table_name, self.name = self.alias.split('.')
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

    @property
    def value(self):
        if self.number == -1:  # id field
            return self.table.row_id
        value = self.table.current_row[self.number]
        if value and self.column and isinstance(
                self.column.output_field, models.DateField):
            if isinstance(value, str):
                value = datetime.datetime.strptime(value, '%d.%m.%Y')
            else:
                value = datetime.datetime(1899, 12, 30) + datetime.timedelta(
                    value)
        return value


class EvaluatedField(BaseField):
    expression = None

    def __init__(self, cursor, alias, column):
        super(EvaluatedField, self).__init__(cursor, alias, column)
        self.expression = expressions.BaseNode.build_node(column, cursor)

    @property
    def value(self):
        return self.expression.evaluate()


class JoinCondition(expressions.BaseNode):
    def __init__(self, node, cursor):
        super(JoinCondition, self).__init__(node, cursor)
        self.table = cursor.tables[node.table_alias.lower()]
        self.parent_table = cursor.tables[node.parent_alias.lower()]
        self.columns = []
        for parent_column, table_column in node.join_cols:
            parent_field = cursor.get_or_create_field(
                '.'.join([node.parent_alias, parent_column]))
            table_field = cursor.get_or_create_field(
                '.'.join([node.table_alias, table_column]))
            self.columns.append((parent_field, table_field))

    def evaluate(self):
        return all([f1.value == f2.value for f1, f2 in self.columns])

    def __iter__(self):
        self.table.flush()
        return self

    def __next__(self):
        while True:
            result = next(self.table)
            if self.evaluate():
                return result


class Cursor:
    fields = None
    tables = None
    connection = None
    selector = None
    fields_map = {}
    condition = None
    _base_table = None
    joins = None

    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, t, value, tb):
        self.close()
        return False

    def close(self):
        self.tables = None
        self.fields = None
        self.fields_map = None
        self.condition = None
        self.selector = None

    def execute(self, sql, params):
        if sql.action == 'SELECT':
            return self._execute_select(sql)
        raise NotImplementedError('WTF')

    def get_or_create_field(self, alias):
        alias = alias.lower()
        if alias in self.fields_map:
            return self.fields_map[alias]
        return CursorField(self, alias, None)

    def _execute_select(self, selector):
        self.selector = selector
        self.tables = self.connection.get_tables(selector.tables.keys())
        self.fields = []
        for full_name, column in selector.columns:
            if isinstance(full_name, str):
                field = CursorField(self, full_name, column)
            else:
                field = EvaluatedField(self, full_name[1], column)
            self.fields.append(field)
            self.fields_map[field.alias] = field
        self.condition = expressions.WhereNode(selector.where, self)
        self.joins = {}
        for alias, table in selector.tables.items():
            if isinstance(table, datastructures.BaseTable):
                if self._base_table is not None:
                    raise DatabaseError('Two base tables not supported')
                self._base_table = self.tables[alias.lower()]
            elif isinstance(table, datastructures.Join):
                alias = alias.lower()
                self.joins[alias] = JoinCondition(table, self)
        if self._base_table is None:
            raise DatabaseError('Base table not found')
        for table in self.tables.values():
            if table != self._base_table:
                table.cached = True

    def __next__(self):
        while True:
            # here should be kind of strategy for joins, how to iterate
            # multiple tables, but it is not implemented yet
            next(self._base_table)
            if self.condition.evaluate():
                break
        return tuple(f.value for f in self.fields)

    def __iter__(self):
        return self

    def fetchone(self):
        if self.selector.order_by:
            result = self.fetchall()
            return result[0] if result else None
        try:
            return next(self)
        except StopIteration:
            return None

    def fetchmany(self, itersize):
        if self.selector.order_by:
            return self.fetchall()
        return list(itertools.islice(self, itersize))

    def fetchall(self):
        return self._apply_order(list(self))

    def _apply_order(self, result):
        # apply ordering in reverse - so first items will have more effect,
        # as they applied last
        for ordering, _ in reversed(self.selector.order_by):
            compiler = self.selector.compiler
            field_alias = ordering.expression.as_sql(
                compiler, compiler.connection)[0].lower()
            field = self.fields_map.get(field_alias)
            try:
                field_num = self.fields.index(field)
            except ValueError:
                raise DatabaseError(
                    f'Ordering field {field_alias} not found in query')
            result.sort(
                # tricky way to avoid comparing None and int
                # https://scipython.com/book2/chapter-4-the-core-python-language-ii/questions/sorting-a-list-containing-none/
                key=lambda x: (x[field_num] is not None, x[field_num]),
                reverse=ordering.descending)
        return result
