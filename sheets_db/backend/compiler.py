from django.core.exceptions import FieldError
from django.db.models.expressions import Col
from django.db.models.sql import compiler
from django.db import NotSupportedError
from django.db.transaction import TransactionManagementError


class Selector:
    action = None
    compiler = None
    where = None
    tables = None
    columns = None
    extra_select = None
    order_by = None
    group_by = None
    with_limit_offset = None
    combinator = None
    distinct_fields = None
    having = None
    for_update = None
    explain_info = None

    def __init__(self, action, compiler):
        self.action = action
        self.compiler = compiler


class SQLCompiler(compiler.SQLCompiler):
    def as_sql(self, with_limits=True, with_col_aliases=False):
        """
        Create the SQL for this query. Return the SQL string and list of
        parameters.

        If 'with_limits' is False, any limit/offset information is not included
        in the query.
        """
        selector = Selector('SELECT', self)
        refcounts_before = self.query.alias_refcount.copy()
        try:
            extra_select, order_by, group_by = self.pre_sql_setup()
            selector.extra_select = extra_select
            selector.order_by = order_by
            selector.group_by = group_by
            # Is a LIMIT/OFFSET clause needed?
            selector.with_limit_offset = with_limits and (
                    self.query.high_mark is not None or self.query.low_mark)
            combinator = self.query.combinator
            features = self.connection.features
            if combinator:
                if not getattr(features, 'supports_select_{}'.format(combinator)):
                    raise NotSupportedError('{} is not supported on this database backend.'.format(combinator))
                selector.combinator = combinator
            else:
                distinct_fields, distinct_params = self.get_distinct()
                selector.distinct_fields = distinct_params
                # This must come after 'select', 'ordering', and 'distinct'
                # (see docstring of get_from_clause() for details).
                selector.tables = self.get_from_clause()
                selector.where = self.where
                selector.having = self.compile(self.having) if self.having is not None else ("", [])

                out_cols = []
                col_idx = 1
                for col, (s_sql, s_params), alias in self.select + extra_select:
                    if alias:
                        s_sql = (s_sql, alias)
                    elif with_col_aliases:
                        s_sql = (s_sql, 'col%d' % col_idx)
                        col_idx += 1
                    out_cols.append((s_sql, col))
                selector.columns = out_cols

                if self.query.select_for_update and \
                        features.has_select_for_update:
                    if self.connection.get_autocommit():
                        raise TransactionManagementError(
                            'select_for_update cannot be used outside of a transaction.')

                    if selector.with_limit_offset and not features.supports_select_for_update_with_limit:
                        raise NotSupportedError(
                            'LIMIT/OFFSET is not supported with '
                            'select_for_update on this database backend.'
                        )
                    nowait = self.query.select_for_update_nowait
                    skip_locked = self.query.select_for_update_skip_locked
                    of = self.query.select_for_update_of
                    no_key = self.query.select_for_no_key_update
                    # If it's a NOWAIT/SKIP LOCKED/OF/NO KEY query but the
                    # backend doesn't support it, raise NotSupportedError to
                    # prevent a possible deadlock.
                    if nowait and not features.has_select_for_update_nowait:
                        raise NotSupportedError('NOWAIT is not supported on this database backend.')
                    elif skip_locked and not features.has_select_for_update_skip_locked:
                        raise NotSupportedError('SKIP LOCKED is not supported on this database backend.')
                    elif of and not features.has_select_for_update_of:
                        raise NotSupportedError('FOR UPDATE OF is not supported on this database backend.')
                    elif no_key and not features.has_select_for_no_key_update:
                        raise NotSupportedError(
                            'FOR NO KEY UPDATE is not supported on this '
                            'database backend.'
                        )
                    for_update_part = self.connection.ops.for_update_sql(
                        nowait=nowait,
                        skip_locked=skip_locked,
                        of=self.get_select_for_update_of_arguments(),
                        no_key=no_key,
                    )

                    if for_update_part and features.for_update_after_from:
                        selector.for_update = (
                            nowait, skip_locked,
                            self.get_select_for_update_of_arguments(), no_key)

                if group_by:
                    if distinct_fields:
                        raise NotImplementedError('annotate() + distinct(fields) is not implemented.')
                    selector.order_by = selector.order_by or \
                                        self.connection.ops.force_no_ordering()
                    if self._meta_ordering:
                        selector.order_by = None

            selector.explain_info = self.query.explain_info
            # if self.query.explain_info:
            #     result.insert(0, self.connection.ops.explain_query_prefix(
            #         self.query.explain_info.format,
            #         **self.query.explain_info.options
            #     ))

            # if order_by:
            #     ordering = []
            #     for _, (o_sql, o_params, _) in order_by:
            #         ordering.append(o_sql)
            #         params.extend(o_params)
            #     result.append('ORDER BY %s' % ', '.join(ordering))

            # if with_limit_offset:
            #     result.append(self.connection.ops.limit_offset_sql(self.query.low_mark, self.query.high_mark))
            #
            # if for_update_part and not self.connection.features.for_update_after_from:
            #     result.append(for_update_part)

            if self.query.subquery and extra_select:
                raise NotImplementedError('no subqueries yet')
                # If the query is used as a subquery, the extra selects would
                # result in more columns than the left-hand side expression is
                # expecting. This can happen when a subquery uses a combination
                # of order_by() and distinct(), forcing the ordering expressions
                # to be selected as well. Wrap the query in another subquery
                # to exclude extraneous selects.
                sub_selects = []
                sub_params = []
                for index, (select, _, alias) in enumerate(self.select, start=1):
                    if not alias and with_col_aliases:
                        alias = 'col%d' % index
                    if alias:
                        sub_selects.append("%s.%s" % (
                            self.connection.ops.quote_name('subquery'),
                            self.connection.ops.quote_name(alias),
                        ))
                    else:
                        select_clone = select.relabeled_clone({select.alias: 'subquery'})
                        subselect, subparams = select_clone.as_sql(self, self.connection)
                        sub_selects.append(subselect)
                        sub_params.extend(subparams)
                return 'SELECT %s FROM (%s) subquery' % (
                    ', '.join(sub_selects),
                    ' '.join(result),
                ), tuple(sub_params + params)

            return selector, []
        finally:
            # Finally do cleanup - get rid of the joins we created above.
            self.query.reset_refcounts(refcounts_before)

    def get_from_clause(self):
        return self.query.alias_map


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    pass


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    def as_sql(self):
        # Prefer the non-standard DELETE FROM syntax over the SQL generated by
        # the SQLDeleteCompiler's default implementation when multiple tables
        # are involved since MySQL/MariaDB will generate a more efficient query
        # plan than when using a subquery.
        where, having = self.query.where.split_having()
        if self.single_alias or having:
            # DELETE FROM cannot be used when filtering against aggregates
            # since it doesn't allow for GROUP BY and HAVING clauses.
            return super().as_sql()
        result = [
            'DELETE %s FROM' % self.quote_name_unless_alias(
                self.query.get_initial_alias()
            )
        ]
        from_sql, from_params = self.get_from_clause()
        result.extend(from_sql)
        where_sql, where_params = self.compile(where)
        if where_sql:
            result.append('WHERE %s' % where_sql)
        return ' '.join(result), tuple(from_params) + tuple(where_params)


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    def as_sql(self):
        update_query, update_params = super().as_sql()
        # MySQL and MariaDB support UPDATE ... ORDER BY syntax.
        if self.query.order_by:
            order_by_sql = []
            order_by_params = []
            db_table = self.query.get_meta().db_table
            try:
                for resolved, (sql, params, _) in self.get_order_by():
                    if (
                        isinstance(resolved.expression, Col) and
                        resolved.expression.alias != db_table
                    ):
                        # Ignore ordering if it contains joined fields, because
                        # they cannot be used in the ORDER BY clause.
                        raise FieldError
                    order_by_sql.append(sql)
                    order_by_params.extend(params)
                update_query += ' ORDER BY ' + ', '.join(order_by_sql)
                update_params += tuple(order_by_params)
            except FieldError:
                # Ignore ordering if it contains annotations, because they're
                # removed in .update() and cannot be resolved.
                pass
        return update_query, update_params


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
