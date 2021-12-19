import datetime


class DBCache:
    cache = None
    last_updated = None
    credentials = None
    tables = {}

    @classmethod
    def update(cls, data):
        cls.cache = data
        cls.tables = {
            table['properties']['title'].lower(): Table(table)
            for table in data['sheets']}
        cls.last_updated = datetime.datetime.now()


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
