import operator
import datetime

from django.db.models.sql import where
from django.db import DatabaseError
from django.db.models.functions import datetime as django_datetime


class BaseNode:
    def __init__(self, node, connection, selector):
        self.node = node
        self.selector = selector
        self.connection = connection

    def check_row(self, table, row_number):
        raise NotImplementedError()


class WhereNode(BaseNode):
    children = None

    def __init__(self, node, connection, selector):
        super(WhereNode, self).__init__(node, connection, selector)
        self.children = []
        for child in node.children:
            if child.lookup_name not in lookup_names:
                raise NotImplementedError(child)
            self.children.append(
                lookup_names[child.lookup_name](child, connection, selector))

    def check_row(self, table, row_number):
        if self.node.connector == where.AND:
            for child in self.children:
                result = child.check_row(table, row_number)
                if self.node.negated:
                    result = not result
                if not result:
                    return False
            return True
        for child in self.children:
            result = child.check_row(table, row_number)
            if self.node.negated:
                result = not result
            if result:
                return True
        return False


class FieldExtractor:
    def __init__(self, lookup_name, field_name):
        self.field_name = field_name
        self.lookup_name = lookup_name

    def extract(self, value):
        raise NotImplementedError()


class YearExtractor(FieldExtractor):
    param = 'year'

    def extract(self, value):
        if value is None:
            return None
        if not isinstance(value, datetime.datetime):
            raise DatabaseError(
                f'Field {self.field_name} expected to be datetime')
        return getattr(value, self.param)


class MonthExtractor(YearExtractor):
    param = 'month'


class DayExtractor(YearExtractor):
    param = 'day'


def extractor_wrapper(cls):
    def as_sql(node, compiler, connection):
        sql, params = node.as_sql(compiler, connection)
        return sql, [cls(node.lookup_name, sql)]
    return as_sql


django_datetime.ExtractYear.as_sheets_db = extractor_wrapper(YearExtractor)
django_datetime.ExtractIsoYear.as_sheets_db = extractor_wrapper(YearExtractor)
django_datetime.ExtractMonth.as_sheets_db = extractor_wrapper(MonthExtractor)
django_datetime.ExtractDay.as_sheets_db = extractor_wrapper(DayExtractor)


class CompareNode(BaseNode):
    extractor = None

    def __init__(self, node, connection, selector):
        super(CompareNode, self).__init__(node, connection, selector)
        db = selector.compiler.connection
        field_name, params = node.process_lhs(selector.compiler, db)
        if params and isinstance(params[0], FieldExtractor):
            self.extractor = params[0]
        _, self.compare_value = node.process_rhs(selector.compiler, db)
        self.lookup_name = node.lookup_name
        self.table_name, field_name = field_name.lower().split('.', 1)
        table = connection.tables.get(self.table_name)
        if table is None:
            raise DatabaseError(
                f'Table {table} not found for field {field_name}')
        self.field_number = None
        for i, name in enumerate(table.fields):
            if name.lower() == field_name:
                self.field_number = i
                break
        if self.field_number is None:
            raise DatabaseError(
                f'{field_name} not found in table {self.table_name}')

    def compare(self, db_value, compare_value):
        raise NotImplementedError()

    def check_row(self, table, row_number):
        if table.name != self.table_name:
            return True
        value = table.fields[self.field_number].value(row_number)
        if self.extractor:
            value = self.extractor.extract(value)
        return self.compare(value, self.compare_value[0])


def simple_compare(func):
    class SimpleCompare(CompareNode):
        def compare(self, db_value, compare_value):
            if db_value is None:
                return False
            return func(db_value, compare_value)

        def __str__(self):
            return f'SimpleCompare {self.lookup_name}'
    return SimpleCompare


class IExact(CompareNode):
    def compare(self, db_value, compare_value):
        return str(db_value).lower() == str(compare_value).lower()


class CompareIn(CompareNode):
    def compare(self, db_value, compare_value):
        return db_value in self.compare_value


class CompareRange(CompareNode):
    def compare(self, db_value, compare_value):
        if db_value is None:
            return False
        return self.compare_value[0] <= db_value <= self.compare_value[1]


lookup_names = {
    'iexact': IExact,
    'exact': simple_compare(operator.eq),
    'gt': simple_compare(operator.gt),
    'gte': simple_compare(operator.ge),
    'lt': simple_compare(operator.lt),
    'lte': simple_compare(operator.le),
    'startswith': simple_compare(lambda x, y: str(x).startswith(str(y)[:-1])),
    'istartswith': simple_compare(
        lambda x, y: str(x).lower().startswith(str(y[:-1]).lower())),
    'endswith': simple_compare(lambda x, y: str(x).endswith(str(y)[1:])),
    'iendswith': simple_compare(
        lambda x, y: str(x).lower().endswith(str(y)[1:].lower())),
    'contains': simple_compare(lambda x, y: str(y)[1:-1] in str(x)),
    'icontains': simple_compare(
        lambda x, y: str(y)[1:-1].lower() in str(x).lower()),
    'in': CompareIn,
    'range': CompareRange,
}