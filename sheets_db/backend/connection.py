import datetime
import logging
import json
import os

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from django.core.cache import cache

from sheets_db.backend import cursor

logger = logging.getLogger('sheets_db')

CACHE_KEY_PREFIX = 'sheets_db_'


class Connection:
    settings = None
    cache = None
    last_updated = None
    credentials = None
    tables = None
    cache_key = None

    def __init__(self, settings):
        self.settings = settings
        self.tables = {}
        self.name = self.settings['NAME']
        self.cache_key = CACHE_KEY_PREFIX + self.name
        self.alias = self.settings['ALIAS']
        self.user_secret_file = self.settings['USER_SECRET']
        self.configured = os.path.exists(self.user_secret_file)
        self.cache_ttl = self.settings['CACHE_TTL']

    def connect(self):
        if not self.configured:
            logger.warning(
                f"Sheets DB {self.alias} not configured.")
            return
        logger.warning("Load db credentials")
        self.credentials = Credentials.from_authorized_user_file(
            self.user_secret_file)
        if self.credentials.expired:
            self.credentials.refresh(Request())
        logger.warning(f"Initializing Sheets DB {self.alias}")
        data = cache.get(self.cache_key, None)
        if data:
            self._update_cache(json.loads(data))
            logger.warning("DB loaded from cache")
        else:
            self.load_db()

    def cursor(self):
        return cursor.Cursor(self)

    def _update_cache(self, data):
        self.cache = data
        self.tables = {
            table['properties']['title'].lower(): Table(table)
            for table in data['sheets']}
        self.last_updated = datetime.datetime.now()

    def load_db(self):
        logger.warning("Requesting google for DB data")
        with build(
                'sheets', 'v4', credentials=self.credentials) as service:
            result = service.spreadsheets().get(
                spreadsheetId=self.name, includeGridData=True
            ).execute()
        self._update_cache(result)
        cache.set(self.cache_key, json.dumps(result), self.cache_ttl)
        logger.warning("DB loaded from google")

    def configure(self, credentials):
        self.credentials = credentials
        self.load_db()
        logger.warning("DB data initially loaded")
        with open(self.user_secret_file, 'tw') as token:
            token.write(credentials.to_json())
        self.configured = True


class Table:
    properties = None
    _raw = None
    data = None
    fields = None
    extra = None

    def __init__(self, data):
        self._raw = data
        self.properties = data['properties']
        table_data = data['data'][0]['rowData']
        self.data = []
        for row in table_data:
            if self.fields is None:
                self.fields = []
                self._init_fields(row['values'])
            elif row:
                self.data.append(self._read_row(row['values']))

    def _init_fields(self, row):
        for entry in row:
            self.fields.append(entry.get('formattedValue', None))

    def _read_row(self, row):
        return [value.get('formattedValue', None) for value in row]
