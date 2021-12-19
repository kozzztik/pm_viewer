import datetime


class DBCache:
    cache = None
    last_updated = None
    credentials = None


def update_cache(data):
    DBCache.cache = data
    DBCache.last_updated = datetime.datetime.now()
