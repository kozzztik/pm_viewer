from django import db
from django.db.models.sql import where
from django.db.models import lookups
from django.db.models import expressions
from django.db.models import aggregates
from django.db.models.fields import related_lookups
from django.db.models.functions import datetime as dj_datetime


class BaseNode:
    def __init__(self, node, cursor):
        self.node = node
        self.cursor = cursor

    def evaluate(self):
        raise NotImplementedError()

    def get_child(self, node):
        return self.build_node(node, self.cursor)

    @classmethod
    def build_node(cls, node, cursor):
        if isinstance(node, (str, int, bool, list, tuple)):
            node_cls = SimpleValueNode
        else:
            node_cls = expressions_map.get(node.__class__)
        if not node_cls:
            raise NotImplementedError(
                f'Expression {node.__class__} not implemented')
        return node_cls(node, cursor)


class WhereNode(BaseNode):
    children = None

    def __init__(self, node, cursor):
        super(WhereNode, self).__init__(node, cursor)
        self.children = []
        for child in node.children:
            self.children.append(self.get_child(child))

    def evaluate(self):
        results = [child.evaluate() for child in self.children]
        if self.node.connector == where.AND:
            result = all(results)
        else:
            result = any(results)
        if self.node.negated:
            result = not result
        return result


simple_operations = {
    'exact': lambda x, y: x == y,
    'lt': lambda x, y: x < y,
    'lte': lambda x, y: x <= y,
    'gt': lambda x, y: x > y,
    'gte': lambda x, y: x >= y,
    '+': lambda x, y: x + y,
    '-': lambda x, y: x - y,
    '*': lambda x, y: x * y,
    '/': lambda x, y: x / y,
    '^': lambda x, y: x ** y,
    '%': lambda x, y: x % y,
    '&': lambda x, y: x & y,
    '|': lambda x, y: x | y,
    '<<': lambda x, y: x << y,
    '>>': lambda x, y: x >> y,
    '#': lambda x, y: x ^ y,
    'iexact': lambda x, y: x.lower() == y.lower(),
    'in': lambda x, y: x in y,
    'contains': lambda x, y: y in x,
    'icontains': lambda x, y: y.lower() in x.lower(),
    'startswith': lambda x, y: x.startswith(y),
    'istartswith': lambda x, y: x.lower().startswith(y.lower()),
    'endswith': lambda x, y: x.endswith(y),
    'iendswith': lambda x, y: x.lower().endswith(y.lower()),
    'range': lambda x, y: y[0] <= x <= y[1],
    'isnull': lambda x, y: (x is None) == y,
}


class SimpleOperationNode(BaseNode):
    operation = None

    def __init__(self, node, cursor):
        super(SimpleOperationNode, self).__init__(node, cursor)
        self.lhs = self.get_child(node.lhs)
        self.rhs = self.get_child(node.rhs)

    def get_operation(self):
        return self.operation or simple_operations.get(
            self.node.lookup_name)

    def evaluate(self):
        operation = self.get_operation()
        if not operation:
            raise NotImplementedError(f'Operation {self.node} not implemented')
        lhs, rhs = self.lhs.evaluate(), self.rhs.evaluate()
        if lhs is None or rhs is None:
            return None
        return operation(lhs, rhs)


class ColumnNode(BaseNode):
    def __init__(self, node, cursor):
        super(ColumnNode, self).__init__(node, cursor)
        alias, column = node.alias, node.target.column
        identifiers = (alias, column) if alias else (column,)
        self.field = cursor.get_or_create_field('.'.join(identifiers))

    def evaluate(self):
        return self.field.value


class ValueNode(BaseNode):
    def evaluate(self):
        return self.node.value


class SimpleValueNode(BaseNode):
    def evaluate(self):
        return self.node


class CombinedExpression(SimpleOperationNode):
    def get_operation(self):
        return simple_operations.get(self.node.connector)


class BaseExtractDate(BaseNode):
    param = None

    def __init__(self, node, cursor):
        super(BaseExtractDate, self).__init__(node, cursor)
        self.column = self.get_child(node.lhs)

    def evaluate(self):
        value = self.column.evaluate()
        if value is None:
            return None
        return getattr(value, self.param)

    @classmethod
    def extract(cls, extract_param):
        class ParamExtractor(cls):
            param = extract_param
        return ParamExtractor


class CountAggregation(BaseNode):
    def __init__(self, node, cursor):
        super(CountAggregation, self).__init__(node, cursor)
        if len(node.source_expressions) != 1:
            raise db.DatabaseError('Only one expression aggregates supported')
        exp = node.source_expressions[0]
        self.column = self.get_child(exp)
        self.field = self.column.field

    def evaluate(self):
        counter = 0
        values = set()
        for _ in self.cursor.joins[self.field.table.name]:
            value = self.field.value
            if self.node.distinct:
                values.add(value)
            elif value is not None:
                counter += 1
        return len(values) if self.node.distinct else counter


class AvgAggregation(CountAggregation):
    def evaluate(self):
        counter = 0
        total_sum = 0
        for _ in self.cursor.joins[self.field.table.name]:
            value = self.field.value
            if value is not None:
                counter += 1
                total_sum += value
        return total_sum / counter if counter else None


class SumAggregation(CountAggregation):
    def evaluate(self):
        total_sum = None
        for _ in self.cursor.joins[self.field.table.name]:
            value = self.field.value
            if value is not None:
                total_sum = (total_sum or 0) + value
        return total_sum


class MaxAggregation(CountAggregation):
    def evaluate(self):
        result = None
        for _ in self.cursor.joins[self.field.table.name]:
            value = self.field.value
            if value is not None:
                if result is None:
                    result = value
                else:
                    result = max(result, value)
        return result


class MinAggregation(CountAggregation):
    def evaluate(self):
        result = None
        for _ in self.cursor.joins[self.field.table.name]:
            value = self.field.value
            if value is not None:
                if result is None:
                    result = value
                else:
                    result = min(result, value)
        return result


expressions_map = {
    lookups.Exact: SimpleOperationNode,
    lookups.IExact: SimpleOperationNode,
    lookups.GreaterThan: SimpleOperationNode,
    lookups.GreaterThanOrEqual: SimpleOperationNode,
    lookups.LessThan: SimpleOperationNode,
    lookups.LessThanOrEqual: SimpleOperationNode,
    lookups.IntegerGreaterThanOrEqual: SimpleOperationNode,
    lookups.IntegerLessThan: SimpleOperationNode,
    lookups.In: SimpleOperationNode,
    lookups.Contains: SimpleOperationNode,
    lookups.IContains: SimpleOperationNode,
    lookups.StartsWith: SimpleOperationNode,
    lookups.IStartsWith: SimpleOperationNode,
    lookups.EndsWith: SimpleOperationNode,
    lookups.IEndsWith: SimpleOperationNode,
    lookups.Range: SimpleOperationNode,
    lookups.IsNull: SimpleOperationNode,
    lookups.Regex: None,
    lookups.IRegex: None,
    expressions.Col: ColumnNode,
    expressions.Value: ValueNode,
    expressions.CombinedExpression: CombinedExpression,
    lookups.YearExact: SimpleOperationNode,
    lookups.YearGt: SimpleOperationNode,
    lookups.YearGte: SimpleOperationNode,
    lookups.YearLt: SimpleOperationNode,
    lookups.YearLte: SimpleOperationNode,
    dj_datetime.ExtractYear: BaseExtractDate.extract('year'),
    dj_datetime.ExtractMonth: BaseExtractDate.extract('month'),
    dj_datetime.ExtractWeekDay: None,
    dj_datetime.ExtractIsoWeekDay: None,
    dj_datetime.ExtractWeek: None,
    dj_datetime.ExtractIsoYear: None,
    dj_datetime.ExtractQuarter: None,
    dj_datetime.ExtractDay: BaseExtractDate.extract('day'),
    dj_datetime.ExtractHour: BaseExtractDate.extract('hour'),
    dj_datetime.ExtractMinute: BaseExtractDate.extract('minute'),
    dj_datetime.ExtractSecond: BaseExtractDate.extract('second'),
    aggregates.Count: CountAggregation,
    aggregates.Avg: AvgAggregation,
    aggregates.Sum: SumAggregation,
    aggregates.Max: MaxAggregation,
    aggregates.Min: MinAggregation,
    aggregates.StdDev: None,
    aggregates.Variance: None,
    related_lookups.RelatedIn: SimpleOperationNode,
}
