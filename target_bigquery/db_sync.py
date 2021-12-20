import json
import sys
import singer
import collections
import inflection
import re
import itertools
import time
import datetime
from decimal import Decimal, getcontext

from google.cloud import bigquery
from google.cloud.bigquery.job import SourceFormat
from google.cloud.bigquery import Dataset, WriteDisposition
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery.table import Table
from google.api_core import exceptions

logger = singer.get_logger()

PRECISION = 38
SCALE = 9
getcontext().prec = PRECISION
# Limit decimals to the same precision and scale as BigQuery accepts
ALLOWED_DECIMALS = Decimal(10) ** Decimal(-SCALE)
MAX_NUM = (Decimal(10) ** Decimal(PRECISION-SCALE)) - ALLOWED_DECIMALS

def validate_config(config):
    errors = []
    required_config_keys = [
        'project_id'
    ]

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append("Required key is missing from config: [{}]".format(k))

    # Check target schema config
    config_default_target_schema = config.get('default_target_schema', None)
    config_schema_mapping = config.get('schema_mapping', None)
    if not config_default_target_schema and not config_schema_mapping:
        errors.append("Neither 'default_target_schema' (string) nor 'schema_mapping' (object) keys set in config.")

    return errors


# pylint: disable=no-else-return,too-many-return-statements
def bigquery_type(property_type, property_format):
    # Every date-time JSON value is currently mapped to TIMESTAMP WITHOUT TIME ZONE
    #
    # TODO: Detect if timezone postfix exists in the JSON and find if DATETIME or
    # TIMESTAMP which includes time zone is the better column type
    if property_format == 'date-time':
        return 'timestamp'
    elif property_format == 'time':
        return 'time'
    elif 'number' in property_type:
        return 'numeric'
    elif 'integer' in property_type and 'string' in property_type:
        return 'string'
    elif 'integer' in property_type:
        return 'integer'
    elif 'boolean' in property_type:
        return 'boolean'
    elif 'object' in property_type:
        return 'record'
    else:
        return 'string'


def handle_record_type(safe_name, schema_property, mode="NULLABLE"):
    fields = [column_type(col, t) for col, t in schema_property.get('properties', {}).items()]
    if fields:
        return SchemaField(safe_name, 'RECORD', mode, fields=fields)
    else:
        return SchemaField(safe_name, 'string', mode)


def column_type(name, schema_property):
    safe_name = safe_column_name(name, quotes=False)
    property_type = schema_property['type']
    property_format = schema_property.get('format', None)

    if 'array' in property_type:
        try:
            items_schema = schema_property['items']
            items_type = bigquery_type(
                             items_schema['type'],
                             items_schema.get('format', None))
        except KeyError:
            return SchemaField(safe_name, 'string', 'NULLABLE')
        else:
            if items_type == "record":
                return handle_record_type(safe_name, items_schema, "REPEATED")
            return SchemaField(safe_name, items_type, 'REPEATED')

    elif 'object' in property_type:
        return handle_record_type(safe_name, schema_property)

    else:
        result_type = bigquery_type(property_type, property_format)
        return SchemaField(safe_name, result_type, 'NULLABLE')


def column_type_avro(name, schema_property):
    property_type = schema_property['type']
    property_format = schema_property.get('format', None)
    result = {"name": safe_column_name(name, quotes=False)}

    if 'array' in property_type:
        try:
            items_type = column_type_avro(name, schema_property['items'])
            result_type = {
                'type': 'array',
                'items': items_type['type']}
        except KeyError:
            result_type = 'string'
    elif 'object' in property_type:
        items_types = [
            column_type_avro(col, schema_property)
            for col, schema_property in schema_property.get('properties', {}).items()]

        if items_types:
            result_type = {
                'type': 'record',
                'name': name + '_properties',
                'fields': items_types}
        else:
            result_type = 'string'

    elif property_format == 'date-time':
        result_type = {
            'type': 'long',
            'logicalType': 'timestamp-millis'}
    elif property_format == 'time':
        result_type = {
            'type': 'int',
            'logicalType': 'time-millis'}
    elif 'number' in property_type:
        result_type = {
            'type': 'bytes',
            'logicalType': 'decimal',
            'scale': 9,
            'precision': 38}
    elif 'integer' in property_type and 'string' in property_type:
        result_type = 'string'
    elif 'integer' in property_type:
        result_type = 'long'
    elif 'boolean' in property_type:
        result_type = 'boolean'
    else:
        result_type = 'string'

    result['type'] = ['null', result_type]
    return result


def safe_column_name(name, quotes=False):
    name = name.replace('`', '')
    pattern = '[^a-zA-Z0-9_]'
    name = re.sub(pattern, '_', name)
    if quotes:
        return '`{}`'.format(name).lower()
    else:
        return '{}'.format(name).lower()


def is_unstructured_object(props):
    """Check if property is object and it has no properties."""
    return 'object' in props['type'] and not props.get('properties')


def flatten_key(k, parent_key, sep):
    full_key = parent_key + [k]
    inflected_key = full_key.copy()
    reducer_index = 0
    while len(sep.join(inflected_key)) >= 255 and reducer_index < len(inflected_key):
        reduced_key = re.sub(r'[a-z]', '', inflection.camelize(inflected_key[reducer_index]))
        inflected_key[reducer_index] = \
            (reduced_key if len(reduced_key) > 1 else inflected_key[reducer_index][0:3]).lower()
        reducer_index += 1

    return sep.join(inflected_key)


def flatten_schema(d, parent_key=[], sep='__', level=0, max_level=0):
    items = []

    if 'properties' not in d:
        return {}

    for k, v in d['properties'].items():
        k = safe_column_name(k, quotes=False)
        new_key = flatten_key(k, parent_key, sep)
        if 'type' in v.keys():
            if 'object' in v['type'] and 'properties' in v and level < max_level:
                items.extend(flatten_schema(v, parent_key + [k], sep=sep, level=level+1, max_level=max_level).items())
            else:
                items.append((new_key, v))
        else:
            if len(v.values()) > 0:
                if list(v.values())[0][0]['type'] == 'string':
                    list(v.values())[0][0]['type'] = ['null', 'string']
                    items.append((new_key, list(v.values())[0][0]))
                elif list(v.values())[0][0]['type'] == 'array':
                    list(v.values())[0][0]['type'] = ['null', 'array']
                    items.append((new_key, list(v.values())[0][0]))
                elif list(v.values())[0][0]['type'] == 'object':
                    list(v.values())[0][0]['type'] = ['null', 'object']
                    items.append((new_key, list(v.values())[0][0]))

    key_func = lambda item: item[0]
    sorted_items = sorted(items, key=key_func)
    for k, g in itertools.groupby(sorted_items, key=key_func):
        if len(list(g)) > 1:
            raise ValueError('Duplicate column name produced in schema: {}'.format(k))

    return dict(sorted_items)


def flatten_record(d, parent_key=[], sep='__', level=0, max_level=0):
    items = []
    for k, v in d.items():
        k = safe_column_name(k, quotes=False)
        new_key = flatten_key(k, parent_key, sep)
        if isinstance(v, collections.MutableMapping) and level < max_level:
            items.extend(flatten_record(v, parent_key + [k], sep=sep, level=level+1, max_level=max_level).items())
        else:
            items.append((new_key, v if type(v) is list or type(v) is dict else v))
    return dict(items)


def primary_column_names(stream_schema_message):
    return [safe_column_name(p) for p in stream_schema_message['key_properties']]

def stream_name_to_dict(stream_name, separator='-'):
    catalog_name = None
    schema_name = None
    table_name = stream_name

    # Schema and table name can be derived from stream if it's in <schema_name>-<table_name> format
    s = stream_name.split(separator)
    if len(s) == 2:
        schema_name = s[0]
        table_name = s[1]
    if len(s) > 2:
        catalog_name = s[0]
        schema_name = s[1]
        table_name = '_'.join(s[2:])

    return {
        'catalog_name': catalog_name,
        'schema_name': schema_name,
        'table_name': table_name
    }

# pylint: disable=too-many-public-methods,too-many-instance-attributes
class DbSync:
    def __init__(self, connection_config, stream_schema_message=None):
        """
            connection_config:      BigQuery connection details

            stream_schema_message:  An instance of the DbSync class is typically used to load
                                    data only from a certain singer tap stream.

                                    The stream_schema_message holds the destination schema
                                    name and the JSON schema that will be used to
                                    validate every RECORDS messages that comes from the stream.
                                    Schema validation happening before creating JSON and before
                                    uploading data into BigQuery.

                                    If stream_schema_message is not defined that we can use
                                    the DbSync instance as a generic purpose connection to
                                    BigQuery and can run individual queries. For example
                                    collecting catalog informations from BigQuery for caching
                                    purposes.
        """
        self.connection_config = connection_config
        self.stream_schema_message = stream_schema_message

        # Validate connection configuration
        config_errors = validate_config(connection_config)

        # Exit if config has errors
        if len(config_errors) > 0:
            logger.error("Invalid configuration:\n   * {}".format('\n   * '.join(config_errors)))
            sys.exit(1)

        self.schema_name = None
        self.grantees = None

        # Init stream schema
        if self.stream_schema_message is not None:
            #  Define target schema name.
            #  --------------------------
            #  Target schema name can be defined in multiple ways:
            #
            #   1: 'default_target_schema' key  : Target schema is the same for every incoming stream if
            #                                     not specified explicitly for a given stream in
            #                                     the `schema_mapping` object
            #   2: 'schema_mapping' key         : Target schema defined explicitly for a given stream.
            #                                     Example config.json:
            #                                           "schema_mapping": {
            #                                               "my_tap_stream_id": {
            #                                                   "target_schema": "my_bigquery_schema",
            #                                                   "target_schema_select_permissions": [ "role_with_select_privs" ]
            #                                               }
            #                                           }
            config_default_target_schema = self.connection_config.get('default_target_schema', '').strip()
            config_schema_mapping = self.connection_config.get('schema_mapping', {})

            stream_name = stream_schema_message['stream']
            stream_schema_name = stream_name_to_dict(stream_name)['schema_name']
            if config_schema_mapping and stream_schema_name in config_schema_mapping:
                self.schema_name = config_schema_mapping[stream_schema_name].get('target_schema')
            elif config_default_target_schema:
                self.schema_name = config_default_target_schema

            if not self.schema_name:
                raise Exception("Target schema name not defined in config. Neither 'default_target_schema' (string) nor 'schema_mapping' (object) defines target schema for {} stream.".format(stream_name))

            #  Define grantees
            #  ---------------
            #  Grantees can be defined in multiple ways:
            #
            #   1: 'default_target_schema_select_permissions' key  : USAGE and SELECT privileges will be granted on every table to a given role
            #                                                       for every incoming stream if not specified explicitly
            #                                                       in the `schema_mapping` object
            #   2: 'target_schema_select_permissions' key          : Roles to grant USAGE and SELECT privileges defined explicitly
            #                                                       for a given stream.
            #                                                       Example config.json:
            #                                                           "schema_mapping": {
            #                                                               "my_tap_stream_id": {
            #                                                                   "target_schema": "my_bigquery_schema",
            #                                                                   "target_schema_select_permissions": [ "role_with_select_privs" ]
            #                                                               }
            #                                                           }
            self.grantees = self.connection_config.get('default_target_schema_select_permissions')
            if config_schema_mapping and stream_schema_name in config_schema_mapping:
                self.grantees = config_schema_mapping[stream_schema_name].get('target_schema_select_permissions', self.grantees)

            self.data_flattening_max_level = self.connection_config.get('data_flattening_max_level', 0)
            self.flatten_schema = flatten_schema(stream_schema_message['schema'], max_level=self.data_flattening_max_level)
            self.renamed_columns = {}


    def open_connection(self):
        project_id = self.connection_config['project_id']
        location = self.connection_config.get('location', None)
        return bigquery.Client(project=project_id, location=location)

    def query(self, query, params=[]):
        def to_query_parameter(value):
            if isinstance(value, int):
                value_type = "INT64"
            elif isinstance(value, float):
                value_type = "NUMERIC"
            elif isinstance(value, bool):
                value_type = "BOOL"
            else:
                value_type = "STRING"
            return bigquery.ScalarQueryParameter(None, value_type, value)

        job_config = bigquery.QueryJobConfig()
        query_params = [to_query_parameter(p) for p in params]
        job_config.query_parameters = query_params

        queries = []
        if type(query) is list:
            queries.extend(query)
        else:
            queries = [query]

        client = self.open_connection()
        logger.info("TARGET_BIGQUERY - Running query: {}".format(query))
        query_job = client.query(';\n'.join(queries), job_config=job_config)
        query_job.result()

        return query_job

    def table_name(self, stream_name, is_temporary=False, without_schema=False):
        stream_dict = stream_name_to_dict(stream_name)
        pattern = '[^a-zA-Z0-9]'
        table_name = re.sub(pattern, '_', stream_dict['table_name']).lower()

        if is_temporary:
            table_name =  '{}_temp'.format(table_name)

        if without_schema:
            return '{}'.format(table_name)
        else:
            return '{}.{}'.format(self.schema_name, table_name)

    def record_primary_key_string(self, record):
        if len(self.stream_schema_message['key_properties']) == 0:
            return None
        flatten = flatten_record(record, max_level=self.data_flattening_max_level)
        try:
            key_props = [str(flatten[p.lower()]) for p in self.stream_schema_message['key_properties']]
        except Exception as exc:
            logger.info("Cannot find {} primary key(s) in record: {}".format(self.stream_schema_message['key_properties'], flatten))
            raise exc
        return ','.join(key_props)

    def avro_schema(self):
        project_id = self.connection_config['project_id']
        pattern = r"[^A-Za-z0-9_]"
        clean_project_id = re.sub(pattern, '', project_id)
        schema = {
             "type": "record",
             "namespace": "{}.{}.pipelinewise.avro".format(
                 clean_project_id,
                 self.schema_name,
                 ),
             "name": self.stream_schema_message['stream'],
             "fields": [column_type_avro(name, c) for name, c in self.flatten_schema.items()]} 

        if re.search(pattern, schema['name']):
            schema["alias"] = schema['name']
            schema["name"] = re.sub(pattern, "_", schema['name'])

        return schema

    # TODO: write tests for the json.dumps lines below and verify nesting
    # TODO: improve performance
    def records_to_avro(self, records):
        for record in records:
            flatten = flatten_record(record, max_level=self.data_flattening_max_level)
            result = {}
            for name, props in self.flatten_schema.items():
                if name in flatten:
                    if is_unstructured_object(props):
                        result[name] = json.dumps(flatten[name])
                    # dump to string if array without items or recursive
                    elif ('array' in props['type'] and
                          (not 'items' in props
                           or '$ref' in props['items'])):
                        result[name] = json.dumps(flatten[name])
                    # dump array elements to strings
                    elif (
                        'array' in props['type'] and
                        is_unstructured_object(props.get('items', {}))
                    ):
                        result[name] = [json.dumps(value) for value in flatten[name]]
                    elif 'number' in props['type']:
                        if flatten[name] is None:
                            result[name] = None
                        else:
                            n = Decimal(flatten[name])
                            # limit n to the range -MAX_NUM to MAX_NUM
                            result[name] = MAX_NUM if n > MAX_NUM else -MAX_NUM if n < -MAX_NUM else n.quantize(ALLOWED_DECIMALS)
                    else:
                        result[name] = flatten[name] if name in flatten else ''
                else:
                    result[name] = None
            yield result

    def load_avro(self, f, count):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        logger.info("Loading {} rows into '{}'".format(count, self.table_name(stream, False)))

        client = self.open_connection()
        # TODO: make temp table creation and DML atomic with merge
        temp_table = self.table_name(stream_schema_message['stream'], is_temporary=True, without_schema=True)

        logger.info("INSERTING INTO {} ({})".format(
            temp_table,
            ', '.join(self.column_names())
        ))

        dataset_id = self.connection_config.get('temp_schema', self.schema_name).strip()
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(temp_table)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.AVRO
        job_config.use_avro_logical_types = True
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job = client.load_table_from_file(f, table_ref, job_config=job_config)
        job.result()

        if len(self.stream_schema_message['key_properties']) > 0:
            query = self.update_from_temp_table(temp_table)
        else:
            query = self.insert_from_temp_table(temp_table)
        drop_temp_query = self.drop_temp_table(temp_table)

        results = self.query([query, drop_temp_query])
        logger.info('LOADED {} rows'.format(results.num_dml_affected_rows))

    def drop_temp_table(self, temp_table):
        temp_schema = self.connection_config.get('temp_schema', self.schema_name)

        return "DROP TABLE IF EXISTS {}.{}".format(
            temp_schema,
            temp_table
        )

    def insert_from_temp_table(self, temp_table):
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        temp_schema = self.connection_config.get('temp_schema', self.schema_name)
        table = self.table_name(stream_schema_message['stream'])

        return """INSERT INTO {} ({})
                (SELECT s.* FROM {}.{} s)
                """.format(
            table,
            ', '.join(columns),
            temp_schema,
            temp_table
        )

    def update_from_temp_table(self, temp_table):
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        table = self.table_name(stream_schema_message['stream'])
        table_without_schema = self.table_name(stream_schema_message['stream'], without_schema=True)
        temp_schema = self.connection_config.get('temp_schema', self.schema_name)

        result = """MERGE `{table}` t
        USING `{temp_schema}`.`{temp_table}` s
        ON {primary_key_condition}
        WHEN MATCHED THEN
            UPDATE SET {set_values}
        WHEN NOT MATCHED THEN
            INSERT ({renamed_cols}) VALUES ({cols})
        """.format(
            table=table,
            temp_schema=temp_schema,
            temp_table=temp_table,
            primary_key_condition=self.primary_key_condition(),
            set_values=', '.join(
                '{}=s.{}'.format(
                    safe_column_name(self.renamed_columns.get(c, c), quotes=True),
                    safe_column_name(c, quotes=True))
                for c in columns),
            renamed_cols=', '.join(
                safe_column_name(self.renamed_columns.get(c, c), quotes=True)
                for c in columns),
            cols=', '.join(safe_column_name(c,quotes=True) for c in self.column_names()))
        return result

    def primary_key_condition(self):
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(
            ['s.{} = t.{}'
                 .format(
                     safe_column_name(self.renamed_columns.get(c, c), quotes=True),
                     safe_column_name(c, quotes=True))
             for c in names])

    def primary_key_null_condition(self, right_table):
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(
            ['{}.{} is null'
                 .format(
                     right_table,
                     safe_column_name(c, quotes=True))
             for c in names])

    def column_names(self):
        return [safe_column_name(name) for name in self.flatten_schema]

    def create_table(self, is_temporary=False):
        stream_schema_message = self.stream_schema_message

        client = self.open_connection()
        project_id = self.connection_config['project_id']
        dataset_id = self.schema_name
        table_name =  self.table_name(stream_schema_message['stream'], is_temporary, without_schema=True)

        schema = [
            column_type(
                name,
                schema
            )
            for (name, schema) in self.flatten_schema.items()
        ]

        table = Table(
                    '{}.{}.{}'.format(project_id, dataset_id, table_name),
                    schema)
        if is_temporary:
            table.expires = datetime.datetime.now() + datetime.timedelta(days=1)

        client.create_table(table, schema)

    def grant_usage_on_schema(self, schema_name, grantee):
        query = "GRANT USAGE ON SCHEMA {} TO GROUP {}".format(schema_name, grantee)
        logger.info("Granting USAGE privilegue on '{}' schema to '{}'... {}".format(schema_name, grantee, query))
        self.query(query)

    def grant_select_on_all_tables_in_schema(self, schema_name, grantee):
        query = "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO GROUP {}".format(schema_name, grantee)
        logger.info("Granting SELECT ON ALL TABLES privilegue on '{}' schema to '{}'... {}".format(schema_name, grantee, query))
        self.query(query)

    @classmethod
    def grant_privilege(self, schema, grantees, grant_method):
        if isinstance(grantees, list):
            for grantee in grantees:
                grant_method(schema, grantee)
        elif isinstance(grantees, str):
            grant_method(schema, grantees)

    def delete_rows(self, stream):
        table = self.table_name(stream, False)
        query = "DELETE FROM {} WHERE _sdc_deleted_at IS NOT NULL".format(table)
        logger.info("Deleting rows from '{}' table... {}".format(table, query))
        logger.info("DELETE {}".format(self.query(query).result().total_rows))

    def create_schema_if_not_exists(self, table_columns_cache=None):
        schema_name = self.schema_name
        temp_schema = self.connection_config.get('temp_schema', self.schema_name)
        schema_rows = 0

        for schema in set([schema_name, temp_schema]):
            # table_columns_cache is an optional pre-collected list of available objects in postgres
            if table_columns_cache:
                schema_rows = list(filter(lambda x: x['TABLE_SCHEMA'] == schema, table_columns_cache))
            # Query realtime if not pre-collected
            else:
                schema_rows = self.query(
                    'SELECT LOWER(schema_name) schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE LOWER(schema_name) = ?',
                    (schema.lower(),)
                )

            if schema_rows.result().total_rows == 0:
                logger.info("Schema '{}' does not exist. Creating...".format(schema))
                client = self.open_connection()
                dataset = client.create_dataset(schema)

                self.grant_privilege(schema, self.grantees, self.grant_usage_on_schema)

    def get_tables(self):
        return self.query(
            'SELECT table_name FROM {schema}.INFORMATION_SCHEMA.TABLES'
            .format(schema=self.schema_name)
        )

    # pylint: disable=no-self-use
    def alias_field(self, field, alias):
        api_repr = field.to_api_repr()
        api_repr['name'] = alias
        return SchemaField.from_api_repr(api_repr)

    def get_table_columns(self, table_name):
        client = self.open_connection()
        dataset_ref = client.dataset(self.schema_name)

        project_id = self.connection_config['project_id']
        dataset_id = self.schema_name
        table_name = self.table_name(table_name, without_schema=True)

        table_ref = client.dataset(dataset_id).table(table_name)
        table = client.get_table(table_ref)  # API request

        return {field.name: field for field in table.schema}

    def update_columns(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, without_schema=True)
        columns = self.get_table_columns(table_name)

        columns_to_add = [
            column_type(name, properties_schema)
            for (name, properties_schema) in self.flatten_schema.items()
            if safe_column_name(name, quotes=False) not in columns
        ]

        for field in columns_to_add:
            self.add_column(field, stream)

        columns_to_replace = [
            column_type(name, properties_schema)
            for (name, properties_schema) in self.flatten_schema.items()
            if name.lower() in columns and
               columns[name.lower()] != column_type(name, properties_schema)
        ]

        for field in columns_to_replace:
            self.version_column(field, stream)


    def version_column(self, field, stream):
        column = safe_column_name(field.name, quotes=False)
        col_type_suffixes = {
            'timestamp': 'ti',
            'time': 'tm',
            'numeric': 'de',
            'string': 'st',
            'int64': 'it',
            'integer': 'it',
            'bool': 'bo',
            'boolean': 'bo',
            'array': 'arr',
            'struct': 'sct'}

        if field.field_type == 'REPEATED':
            field_with_type_suffix = '{}__{}{}'.format(column, col_type_suffixes['array'], time.strftime("%Y%m%d_%H%M"))
        elif field.field_type == 'RECORD':
            field_with_type_suffix = '{}__{}{}'.format(column, col_type_suffixes['struct'], time.strftime("%Y%m%d_%H%M"))
        else:
            field_with_type_suffix = '{}__{}'.format(column, col_type_suffixes[field.field_type])

        field_without_dt_suffix = re.sub(r"[0-9]{8}_[0-9]{4}", "", field_with_type_suffix)

        table_name = self.table_name(stream, without_schema=True)
        table_columns = self.get_table_columns(table_name)

        # check if we already have this column in the table with a name like column_name__type_suffix
        for col, schemafield in table_columns.items():
            # this is a existing table column without the date suffix that gets added to arrays and structs
            col_without_dt_suffix = re.sub(r"[0-9]{8}_[0-9]{4}", "", col)
                
            if (col_without_dt_suffix in [column, field_without_dt_suffix] and
                self.alias_field(field, '') == self.alias_field(schemafield, '')):
                # example: the column named ID in the stage table exists as ID__int in the final table
                self.renamed_columns[column] = col

        # if we didnt find a existing suitable column, create it
        if not column in self.renamed_columns:
            logger.info('Versioning column: {}'.format(field_with_type_suffix))
            self.add_column(self.alias_field(field, field_with_type_suffix), stream)
            self.renamed_columns[column] = field_with_type_suffix

    def add_column(self, field, stream):
        client = self.open_connection()
        dataset_ref = client.dataset(self.schema_name)

        project_id = self.connection_config['project_id']
        dataset_id = self.schema_name
        table_name = self.table_name(stream, without_schema=True)

        table_ref = client.dataset(dataset_id).table(table_name)
        table = client.get_table(table_ref)  # API request

        schema = table.schema[:]
        schema.append(field) 
        table.schema = schema

        logger.info('Adding column: {}'.format(field.name))
        client.update_table(table, ['schema'])  # API request

    def sync_table(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, without_schema=True)
        table_name_with_schema = self.table_name(stream)
        found_tables = [table for table in (self.get_tables()) if table['table_name'].lower() == table_name]
        if len(found_tables) == 0:
            logger.info("Table '{}' does not exist. Creating...".format(table_name_with_schema))
            self.create_table()

            self.grant_privilege(self.schema_name, self.grantees, self.grant_select_on_all_tables_in_schema)
        else:
            logger.info("Table '{}' exists".format(table_name_with_schema))
            self.update_columns()
