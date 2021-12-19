import logging
import json

import google_auth_oauthlib.flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

from django.conf import settings
from django.core.cache import cache as django_cache
from sheets_db import cache


logger = logging.getLogger('sheets_db')

GOOGLE_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
    settings.SECRETS_JSON, scopes=GOOGLE_SCOPES)
flow.redirect_uri = \
    'http://' + settings.ALLOWED_HOSTS[0] + ':' + str(settings.APP_PORT) + \
    '/oauth_callback/'
DJANGO_CACHE_KEY = 'sheets_db'
DJANGO_CACHE_TTL = 60 * 60  # in seconds


def initial_configure_db(code, sheet_id):
    flow.fetch_token(code=code)
    cache.DBCache.credentials = flow.credentials
    logger.warning("Try to initially load DB data")
    update_db(sheet_id)
    logger.warning("DB data initially loaded")
    with open(settings.BASE_DIR / 'project_settings.py', 'tw') as f:
        f.write(f'PROJECT_SHEET_ID="{sheet_id}"\n')
        f.write(f'PROJECT_USER_ID="{flow.credentials.client_id}"\n')
    with open(settings.BASE_DIR / 'token.json', 'tw') as token:
        token.write(flow.credentials.to_json())
    settings.PROJECT_SHEET_ID = sheet_id
    settings.PROJECT_CONFIGURED = True


def update_db(sheet_id):
    logger.warning("Requesting google for DB data")
    with build(
            'sheets', 'v4', credentials=cache.DBCache.credentials) as service:
        result = service.spreadsheets().get(
            spreadsheetId=sheet_id, includeGridData=True).execute()
    cache.DBCache.update(result)
    django_cache.set(DJANGO_CACHE_KEY, json.dumps(result), DJANGO_CACHE_TTL)
    logger.warning("DB loaded from google")


def configure_db():
    logger.warning("Load db credentials")
    credentials = Credentials.from_authorized_user_file(
        settings.BASE_DIR / 'token.json')
    if credentials.expired:
        credentials.refresh(Request())
    cache.DBCache.credentials = credentials
    logger.warning("Initializing Sheets DB")
    data = django_cache.get(DJANGO_CACHE_KEY, None)
    if data:
        cache.DBCache.update(json.loads(data))
        logger.warning("DB loaded from cache")
    else:
        update_db(settings.PROJECT_SHEET_ID)


if settings.PROJECT_CONFIGURED and cache.DBCache.cache is None:
    configure_db()
