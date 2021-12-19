"""
Dummy database backend for Django.

Django uses this if the database ENGINE setting is empty (None or empty string).

Each of these API functions, except connection.close(), raise
ImproperlyConfigured.
"""

from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.client import BaseDatabaseClient
from django.db.backends.base.creation import BaseDatabaseCreation
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.dummy.features import DummyDatabaseFeatures
from django.db.backends.base.introspection import (
    BaseDatabaseIntrospection, FieldInfo, TableInfo,
)
from sheets_db import cache
from sheets_db.backend import cursor


def complain(*args, **kwargs):
    raise NotImplemented("Feature not implemented yet")


def ignore(*args, **kwargs):
    pass


class DatabaseOperations(BaseDatabaseOperations):
    def quote_name(self, name):
        return name


class DatabaseClient(BaseDatabaseClient):
    def runshell(self, parameters):
        raise RuntimeError('Google sheets DB doesnt have a DB client.')


class DatabaseCreation(BaseDatabaseCreation):
    create_test_db = ignore
    destroy_test_db = ignore


class DatabaseIntrospection(BaseDatabaseIntrospection):
    def get_table_list(self, cursor):
        """
        Return an unsorted list of TableInfo named tuples of all tables and
        views that exist in the database.
        """
        return [
            TableInfo(table.properties['title'], table.properties['sheetType'])
            for table in cache.DBCache.tables.values()
        ]

    def get_table_description(self, cursor, table_name):
        """
        Return a description of the table with the DB-API cursor.description
        interface.
        """
        table_info = cache.DBCache.tables[table_name]
        'name type_code display_size internal_size precision scale null_ok '
        'default collation'
        return [
            FieldInfo(
                field, "STRING", None, None, None, None, True, None, None,
            )
            for field in table_info.fields
        ]

    get_indexes = complain

    def get_sequences(self, cursor, table_name, table_fields=()):
        """
        Return a list of introspected sequences for table_name. Each sequence
        is a dict: {'table': <table_name>, 'column': <column_name>}. An optional
        'name' key can be added if the backend supports named sequences.
        """
        raise NotImplementedError('subclasses of BaseDatabaseIntrospection may require a get_sequences() method')

    def get_relations(self, cursor, table_name):
        """
        Return a dictionary of
        {field_name: (field_name_other_table, other_table)} representing all
        relationships to the given table.
        """
        raise NotImplementedError(
            'subclasses of BaseDatabaseIntrospection may require a '
            'get_relations() method.'
        )

    def get_key_columns(self, cursor, table_name):
        """
        Backends can override this to return a list of:
            (column_name, referenced_table_name, referenced_column_name)
        for all key columns in given table.
        """
        raise NotImplementedError('subclasses of BaseDatabaseIntrospection may require a get_key_columns() method')

    def get_primary_key_column(self, cursor, table_name):
        """
        Return the name of the primary key column for the given table.
        """
        for constraint in self.get_constraints(cursor, table_name).values():
            if constraint['primary_key']:
                return constraint['columns'][0]
        return None

    def get_constraints(self, cursor, table_name):
        """
        Retrieve any constraints or keys (unique, pk, fk, check, index)
        across one or more columns.

        Return a dict mapping constraint names to their attributes,
        where attributes is a dict with keys:
         * columns: List of columns this covers
         * primary_key: True if primary key, False otherwise
         * unique: True if this is a unique constraint, False otherwise
         * foreign_key: (table, column) of target, or None
         * check: True if check constraint, False otherwise
         * index: True if index, False otherwise.
         * orders: The order (ASC/DESC) defined for the columns of indexes
         * type: The type of the index (btree, hash, etc.)

        Some backends may return special constraint names that don't exist
        if they don't name constraints of a certain type (e.g. SQLite)
        """
        return {}


class DatabaseWrapper(BaseDatabaseWrapper):
    operators = {}
    # Override the base class implementations with null
    # implementations. Anything that tries to actually
    # do something raises complain; anything that tries
    # to rollback or undo something raises ignore.

    def _cursor(self):
        return cursor.Cursor()

    ensure_connection = complain
    _commit = complain
    _rollback = ignore
    _close = ignore
    _savepoint = ignore
    _savepoint_commit = complain
    _savepoint_rollback = ignore
    _set_autocommit = complain
    # Classes instantiated in __init__().
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DummyDatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    def is_usable(self):
        return True
