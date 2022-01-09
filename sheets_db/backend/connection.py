import logging
import json
import os

from google.oauth2.credentials import Credentials
from google.auth import exceptions
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from django.core.cache import cache
from django import db

from sheets_db.backend import cursor

logger = logging.getLogger('sheets_db')

CACHE_KEY_PREFIX = 'sheets_db_'
TABLE_NAMES_SUFFIX = '_tables'
TABLE_SUFFIX = '_table_'


class Connection:
    settings = None
    credentials = None
    cache_key = None

    def __init__(self, settings):
        self.settings = settings
        self.name = self.settings['NAME']
        self.cache_key = CACHE_KEY_PREFIX + self.name
        self.alias = self.settings['ALIAS']
        self.user_secret_file = self.settings['USER_SECRET']
        self.configured = os.path.exists(self.user_secret_file)
        self.cache_ttl = self.settings['CACHE_TTL']

    def refresh_credentials(self):
        if self.credentials.expired:
            try:
                self.credentials.refresh(Request())
            except exceptions.RefreshError as e:
                logger.warning(e)
                self.credentials = None
                self.configured = False

    def connect(self):
        if not self.configured:
            logger.warning(
                f"Sheets DB {self.alias} not configured.")
            return
        logger.warning("Load db credentials")
        self.credentials = Credentials.from_authorized_user_file(
            self.user_secret_file)
        self.refresh_credentials()
        if not self.configured:
            return

    def cursor(self):
        return cursor.Cursor(self)

    def configure(self, credentials):
        self.credentials = credentials
        self.configured = True
        try:
            self.get_tables()
        except:
            self.credentials = False
            self.configured = False
        logger.warning("DB data initially loaded")
        with open(self.user_secret_file, 'tw') as token:
            token.write(credentials.to_json())

    def _get_table_map(self):
        table_map = cache.get(self.cache_key + TABLE_NAMES_SUFFIX)
        if table_map is not None:
            table_map = json.loads(table_map)
        return table_map

    def get_table_names(self):
        table_map = self._get_table_map()
        if table_map is None:
            logger.info('Table map cache miss')
        else:
            return table_map.values()
        return self.get_tables().keys()

    def get_tables(self, table_names=None):
        table_map = self._get_table_map()
        table_names = set(name.lower() for name in table_names or [])
        if table_map:
            results = {}
            for table_id, table_name in table_map.items():
                if not table_names or table_name in table_names:
                    data = cache.get(
                        self.cache_key + TABLE_SUFFIX + str(table_id))
                    if not data:
                        logger.info(
                            f'Table {table_name}({table_id}) cache miss')
                        break
                    results[table_name] = Table(json.loads(data))
            else:
                for name in table_names:
                    if name not in results:
                        raise db.DatabaseError(f'{name} table not found in DB')
                return results
        # if table map cache miss or any table cache
        if not self.configured:
            return []
        self.refresh_credentials()
        logger.warning("Requesting google for DB data")
        with build(
                'sheets', 'v4', credentials=self.credentials) as service:
            data = service.spreadsheets().get(
                spreadsheetId=self.name, includeGridData=True
            ).execute()
        table_map = {}
        results = {}
        for table_data in data['sheets']:
            table = Table(table_data)
            table_map[table.sheet_id] = table.name
            cache.set(
                self.cache_key + TABLE_SUFFIX + str(table.sheet_id),
                json.dumps(table_data), self.cache_ttl)
            if not table_names or table.name in table_names:
                results[table.name] = table
        cache.set(
            self.cache_key + TABLE_NAMES_SUFFIX,
            json.dumps(table_map), self.cache_ttl)
        for name in table_names:
            if name not in results:
                raise db.DatabaseError(f'{name} table not found in DB')
        logger.warning("Database cache updated")
        return results


class Table:
    properties = None
    data = None
    field_names = None
    extra = None
    row_id = -1
    row_data = None
    _cached = False
    _cache = None

    def __init__(self, data):
        self.properties = data['properties']
        self.sheet_id = data['properties']['sheetId']
        self.name = data['properties']['title'].lower()
        self.data = data['data'][0]['rowData']
        self._init_fields()

    @property
    def cached(self):
        return self._cached

    @cached.setter
    def cached(self, value):
        if self.row_id != -1:
            raise ValueError('Cached can be set only on start')
        self._cached = value
        if self._cached and self._cache is None:
            self._cache = []

    def flush(self):
        if not self._cached:
            raise ValueError('Can flush only cached tables')
        self.row_id = -1

    def _init_fields(self):
        self.field_names = []
        if not self.data:
            return
        for entry in self.data[0]['values']:
            self.field_names.append(entry.get('formattedValue', None))

    def _get_field_value(self, data):
        if data is None:
            return None
        if len(data) == 1:
            return list(data.values())[0]
        raise NotImplementedError('unknown data format')

    def _read_row(self):
        if self.row_id == -1:
            # remove first row reserved for field names. It is kept there to
            # not break caching
            if self._cached:
                self.row_id += 1
            else:
                self.data.pop(0)
        self.row_id += 1
        row = self.data.pop(0)
        self.row_data = [
            self._get_field_value(value.get('effectiveValue', None))
            for value in row['values']]
        if self._cached:
            self._cache.append(self.row_data)

    def __iter__(self):
        return self

    def __next__(self):
        if self.row_id is None:
            raise StopIteration()
        if self._cached and self.row_id + 1 < len(self._cache):
            self.row_id += 1
            self.row_data = self._cache[self.row_id]
            return self.row_id
        if not self.data:
            self.row_id = None
            self.row_data = None
            raise StopIteration()
        self._read_row()
        return self.row_id

    @property
    def current_row(self):
        if self.row_data is None:
            raise db.DatabaseError('Cursor error')
        return self.row_data

    def __str__(self):
        return f'Table {self.name}({self.sheet_id})[{self.row_id}]'

    def __repr__(self):
        return str(self)
