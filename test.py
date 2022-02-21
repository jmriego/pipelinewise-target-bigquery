from datetime import datetime
import json

from google.cloud.bigquery import SchemaField

from target_bigquery.db_sync import BigQueryJSONEncoder


schema = {
    'c_array_objects_no_props': SchemaField('c_array_objects_no_props', 'STRING', 'REPEATED'),
    'camelcasecolumn': SchemaField('camelcasecolumn', 'STRING', 'NULLABLE'),
    'datetime': SchemaField('datetime', 'STRING', 'NULLABLE'),
}
r = {
    'c_array_objects_no_props': [{'nested': 1}, {'nested': 2}],
    'camelcasecolumn': 'Dummy row 1',
    'datetime': datetime.now()
}

dump = json.dumps(r, cls=BigQueryJSONEncoder, schema=schema, data_flattening_max_level=0)
print(dump)
print(json.loads(dump))
