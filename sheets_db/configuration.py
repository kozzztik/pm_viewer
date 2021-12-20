import google_auth_oauthlib.flow

from django.db import DEFAULT_DB_ALIAS, connections
from django.core import exceptions

from sheets_db.backend import base


GOOGLE_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


def _get_flow(alias):
    db_backend = connections[alias]
    if not isinstance(db_backend, base.DatabaseWrapper):
        raise exceptions.ImproperlyConfigured(
            'Configured database is not Google Sheets backend')
    return google_auth_oauthlib.flow.Flow.from_client_secrets_file(
        db_backend.settings_dict['APP_SECRET'], scopes=GOOGLE_SCOPES)


def get_db_configuration_url(callback_uri, alias=DEFAULT_DB_ALIAS):
    flow = _get_flow(alias)
    flow.redirect_uri = callback_uri
    url, state = flow.authorization_url(access_type='offline')
    return url


def is_db_configured(alias=DEFAULT_DB_ALIAS):
    db_backend = connections[alias]
    db_backend.ensure_connection()
    return db_backend.connection.configured


def configure_db(request, alias=DEFAULT_DB_ALIAS, callback_uri=None):
    user_code = request.GET['code']
    flow = _get_flow(alias)
    callback_uri = callback_uri or \
        f'{request.scheme}://{request.headers["HOST"]}{request.path}'
    flow.redirect_uri = callback_uri
    flow.fetch_token(code=user_code)
    db_backend = connections[alias]
    db_backend.ensure_connection()
    db_backend.connection.configure(flow.credentials)
