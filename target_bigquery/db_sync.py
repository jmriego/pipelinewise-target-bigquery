import json
import sys
import singer
import re
import itertools
import time
import datetime
from decimal import Decimal, getcontext
from typing import List, Mapping, MutableMapping, Tuple

from google.cloud import bigquery

from google.cloud.bigquery import SchemaField
from google.cloud import storage
from google.cloud.exceptions import Conflict


logger = singer.get_logger()

PRECISION = 38
SCALE = 9
getcontext().prec = PRECISION
# Limit decimals to the same precision and scale as BigQuery accepts
ALLOWED_DECIMALS = Decimal(10) ** Decimal(-SCALE)
MAX_NUM = (Decimal(10) ** Decimal(PRECISION-SCALE)) - ALLOWED_DECIMALS


class BigQueryJSONEncoder(json.JSONEncoder):

    def __init__(self, schema: Mapping[str, bigquery.SchemaField], data_flattening_max_level: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = schema
        self.data_flattening_max_level = data_flattening_max_level

    def default(self, o):
        if isinstance(o, (datetime.date, datetime.datetime, datetime.time)):
            return o.isoformat()
        if isinstance(o, Decimal):
            return str(o)
        return json.JSONEncoder.default(self, o)

    def _bq_format(self, o, field_type):
        if field_type == 'string' and not isinstance(o, str):
            return json.JSONEncoder.encode(self, o)
        if field_type == 'numeric':
            n = Decimal(o)
            return MAX_NUM if n > MAX_NUM else -MAX_NUM if n < -MAX_NUM else n.quantize(ALLOWED_DECIMALS)
        return o

    def encode(self, o):
        if isinstance(o, Mapping):
            # Preprocess record to make sure it is compatible with the BQ schema.
            for k, v in o.items():
                field_type = self.schema[k].field_type.lower()
                field_mode = self.schema[k].mode.lower()
                if field_mode == 'repeated':
                    o[k] = [self._bq_format(each, field_type) for each in v]
                else:
                    o[k] = self._bq_format(v, field_type)

        return json.JSONEncoder.encode(self, o)


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


def camelize(string):
    return re.sub(r"(?:^|_)(.)", lambda m: m.group(1).upper(), string)


def flatten_key(k, parent_key, sep):
    full_key = parent_key + [k]
    inflected_key = full_key.copy()
    reducer_index = 0
    while len(sep.join(inflected_key)) >= 255 and reducer_index < len(inflected_key):
        reduced_key = re.sub(r'[a-z]', '', camelize(inflected_key[reducer_index]))
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


# pylint: disable=too-many-arguments
def flatten_record(d, schema, parent_key=[], sep='__', level=0, max_level=0):
    items = []
    for k, v in d.items():
        safe_key = safe_column_name(k, quotes=False)
        new_key = flatten_key(safe_key, parent_key, sep)
        key_schema = schema['properties'][k]
        if isinstance(v, MutableMapping) and level < max_level and 'properties' in key_schema:
            items.extend(flatten_record(
                v, key_schema, parent_key + [safe_key], sep=sep, level=level+1, max_level=max_level).items()
            )
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
    def __init__(self, connection_config, stream_schema_message=None, hard_delete=False):
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
        self.hard_delete = hard_delete

        # Validate connection configuration
        config_errors = validate_config(connection_config)

        # Exit if config has errors
        if len(config_errors) > 0:
            logger.error("Invalid configuration:\n   * {}".format('\n   * '.join(config_errors)))
            sys.exit(1)

        project_id = self.connection_config['project_id']
        location = self.connection_config.get('location', None)
        self.client = bigquery.Client(project=project_id, location=location)
        self.gcs = storage.Client(project=project_id)

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

        logger.info("TARGET_BIGQUERY - Running query: {}".format(query))
        query_job = self.client.query(';\n'.join(queries), job_config=job_config)
        query_job.result()

        return query_job

    def table_ref(self, stream_name: str, is_temporary: bool = False) -> bigquery.TableReference:
        stream_dict = stream_name_to_dict(stream_name)
        pattern = '[^a-zA-Z0-9]'
        project_id = self.connection_config['project_id']
        dataset_id = self.schema_name
        table_id = re.sub(pattern, '_', stream_dict['table_name']).lower()

        if is_temporary:
            dataset_id = self.connection_config.get('temp_schema', dataset_id)
            table_id = f'{table_id}_temp'

        return bigquery.DatasetReference(project_id, dataset_id).table(table_id)

    def record_primary_key_string(self, record):
        if len(self.stream_schema_message['key_properties']) == 0:
            return None
        try:
            key_props = [str(record[p.lower()]) for p in self.stream_schema_message['key_properties']]
        except Exception as exc:
            logger.info("Cannot find {} primary key(s) in record: {}".format(self.stream_schema_message['key_properties'], record))
            raise exc
        return ','.join(key_props)

    def records_to_json(self, records, schema):
        field_map = {field.name: field for field in schema}
        for record in records:
            yield json.dumps(
                record,
                cls=BigQueryJSONEncoder,
                schema=field_map,
                data_flattening_max_level=self.data_flattening_max_level,
            ) + '\n'

    # pylint: disable=too-many-locals
    def load_records(self, records, count):
        stream_schema_message = self.stream_schema_message
        target_table = self.get_table(stream_schema_message['stream'])
        logger.info("Loading {} rows into '{}'".format(
            count, target_table.table_id)
        )

        # TODO: make temp table creation and DML atomic with merge
        temp_table = self.table_ref(stream_schema_message['stream'], is_temporary=True)
        logger.info("INSERTING INTO {} ({})".format(
            temp_table.table_id,
            ', '.join(self.column_names())
        ))

        schema = [column_type(name, schema) for name, schema in self.flatten_schema.items()]

        job_config = bigquery.LoadJobConfig()
        job_config.schema = schema
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.write_disposition = 'WRITE_TRUNCATE'

        # Upload JSONL file.
        bucket = self.connection_config['gcs_bucket']
        prefix = self.connection_config.get('gcs_key_prefix', '')
        blob_name = f'{prefix}{stream_schema_message["stream"]}-{datetime.datetime.now().isoformat()}.json'
        blob = self.gcs.get_bucket(bucket).blob(blob_name)
        with blob.open('w') as f:
            f.writelines(self.records_to_json(records, schema))

        # Ingest into temp table
        job = self.client.load_table_from_uri(
            f'gs://{blob.bucket.name}/{blob.name}',
            temp_table,
            job_config=job_config,
        )

        try:
            job.result()
        finally:
            blob.delete()

        temp_table = self.client.get_table(temp_table)

        if len(self.stream_schema_message['key_properties']) > 0:
            query = self.get_merge_from_table_sql(temp_table, target_table)
        else:
            query = self.get_insert_from_table_sql(temp_table, target_table)
        drop_temp_query = self.get_drop_table_sql(temp_table)
        results = self.query([query, drop_temp_query])
        logger.info('LOADED {} rows'.format(results.num_dml_affected_rows))

    @staticmethod
    def get_drop_table_sql(table: bigquery.Table) -> str:
        return f"DROP TABLE IF EXISTS `{table.dataset_id}.{table.table_id}`"

    def get_insert_from_table_sql(self, src: bigquery.Table, dest: bigquery.Table) -> str:
        return """INSERT INTO `{}` ({})
                (SELECT s.* FROM `{}` s)
                """.format(
            f'{dest.dataset_id}.{dest.table_id}',
            ', '.join(self.column_names()),
            f'{src.dataset_id}.{src.table_id}',
        )

    def get_pruning_field(self, dest: bigquery.Table) -> Tuple[str, str]:
        # The first element of a composite PK is the primary sort and will at
        # least help optimize the MERGE.
        # TODO: incorporate the rest of the PK.
        field = safe_column_name(primary_column_names(self.stream_schema_message)[0])
        field_type = next(
            schema_field.field_type for schema_field in dest.schema
            if field == self.renamed_columns.get(schema_field.name, schema_field.name)
        )
        # Account for legacy SQL type.
        field_type = 'INT64' if field_type == 'INTEGER' else field_type
        return field, field_type

    def get_merge_from_table_sql(self, src: bigquery.Table, dest: bigquery.Table) -> str:
        columns = self.column_names()

        # First determine whether we can use partition pruning
        field, field_type = self.get_pruning_field(dest)
        range_for_upsert_sql = (
            f'DECLARE max_partition {field_type};\n'
            f'DECLARE min_partition {field_type};\n'
            'SET (max_partition, min_partition) = (\n'
            '    SELECT AS STRUCT\n'
            f'        MAX(`{field}`) AS max_partition,\n'
            f'        MIN(`{field}`) AS min_partition\n'
            f'    FROM `{src.dataset_id}.{src.table_id}`\n'
            '    );\n'
        )
        pruning_sql = (
            f' AND t.`{self.renamed_columns.get(field, field)}` BETWEEN min_partition AND max_partition'
        )

        delete_sql = ''
        if '_sdc_deleted_at' in columns:
            delete_sql = (
                "WHEN MATCHED\n"
                "    AND s.`_sdc_deleted_at` IS NOT NULL\n"
                "    THEN "
            )
            if self.hard_delete:
                delete_sql += "DELETE"
            else:
                delete_sql += "UPDATE SET `_sdc_deleted_at`=s.`_sdc_deleted_at`"

        return """
        {range_for_upsert}
        -- run the merge statement
        MERGE `{target}` t
        USING `{source}` s
        ON {primary_key_condition}{pruning}
        {deleted}
        WHEN MATCHED THEN
            UPDATE SET {set_values}
        WHEN NOT MATCHED THEN
            INSERT ({renamed_cols}) VALUES ({cols})
        """.format(
            range_for_upsert=range_for_upsert_sql,
            target=f'{dest.dataset_id}.{dest.table_id}',
            source=f'{src.dataset_id}.{src.table_id}',
            primary_key_condition=self.primary_key_condition(),
            pruning=pruning_sql,
            deleted=delete_sql,
            set_values=', '.join(
                '{}=s.{}'.format(
                    safe_column_name(self.renamed_columns.get(c, c), quotes=True),
                    safe_column_name(c, quotes=True))
                for c in columns),
            renamed_cols=', '.join(
                safe_column_name(self.renamed_columns.get(c, c), quotes=True)
                for c in columns),
            cols=', '.join(safe_column_name(c,quotes=True) for c in columns))

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

    def create_table(self, is_temporary: bool = False) -> bigquery.Table:
        stream_schema_message = self.stream_schema_message

        table_ref =  self.table_ref(stream_schema_message['stream'], is_temporary)

        schema = [
            column_type(
                name,
                schema
            )
            for (name, schema) in self.flatten_schema.items()
        ]

        table = bigquery.Table(table_ref, schema=schema)
        if is_temporary:
            table.expires = datetime.datetime.now() + datetime.timedelta(days=1)

        table = self.client.create_table(table)
        return self.update_clustering_fields(table)

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

    def create_schema_if_not_exists(self):
        schema_name = self.schema_name
        temp_schema = self.connection_config.get('temp_schema', self.schema_name)
        project_id = self.connection_config['project_id']

        for schema in set([schema_name, temp_schema]):
            try:
                self.client.create_dataset(bigquery.DatasetReference(project_id, schema))
                logger.info("Schema '{}' does not exist. Creating...".format(schema))
                self.grant_privilege(schema, self.grantees, self.grant_usage_on_schema)
            except Conflict:
                # Already exists.
                pass

    # pylint: disable=no-self-use
    def alias_field(self, field, alias):
        api_repr = field.to_api_repr()
        api_repr['name'] = alias
        return SchemaField.from_api_repr(api_repr)

    def get_table(self, table_name: str, is_temporary: bool = False) -> bigquery.Table:
        table_ref = self.table_ref(table_name, is_temporary=is_temporary)
        return self.client.get_table(table_ref)

    def get_table_columns(self, table: bigquery.Table):
        return {field.name: field for field in table.schema}

    def update_columns(self) -> bigquery.Table:
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table = self.get_table(stream)
        columns = self.get_table_columns(table)

        columns_to_add = [
            column_type(name, properties_schema)
            for (name, properties_schema) in self.flatten_schema.items()
            if safe_column_name(name, quotes=False) not in columns
        ]

        table =self.add_columns(table, columns_to_add)

        columns_to_replace = [
            column_type(name, properties_schema)
            for (name, properties_schema) in self.flatten_schema.items()
            if name.lower() in columns and
               columns[name.lower()] != column_type(name, properties_schema)
        ]

        for field in columns_to_replace:
            table = self.version_column(table, field)

        return self.update_clustering_fields(table)

    def update_clustering_fields(self, table: bigquery.Table) -> bigquery.Table:
        new_clustering_fields = [
            self.renamed_columns.get(c, c) for c in primary_column_names(self.stream_schema_message)
        ] or None
        if table.clustering_fields != new_clustering_fields:
            logger.info('Updating clustering fields: {}'.format(new_clustering_fields))
            table.clustering_fields = new_clustering_fields
            return self.client.update_table(table, ['clustering_fields'])
        return table

    def version_column(self, table: bigquery.Table, field: bigquery.SchemaField) -> bigquery.Table:
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

        table_columns = self.get_table_columns(table)

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
            table = self.add_columns(table, [self.alias_field(field, field_with_type_suffix)])
            self.renamed_columns[column] = field_with_type_suffix
        return table

    def add_columns(self, table: bigquery.Table, fields: List[bigquery.SchemaField]) -> bigquery.Table:
        # Only do an update if there are fields to add.
        if fields:
            schema = table.schema
            schema.extend(fields)
            table.schema = schema

            logger.info('Adding columns: {}'.format([field.name for field in fields]))
            return self.client.update_table(table, ['schema'])  # API request
        return table

    def sync_table(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']

        table_ref = self.table_ref(stream)
        try:
            self.create_table()
            logger.info(f"Table '{table_ref.dataset_id}.{table_ref.table_id}' does not exist. Creating...")
            self.grant_privilege(self.schema_name, self.grantees, self.grant_select_on_all_tables_in_schema)
        except Conflict:
            logger.info(f"Table '{table_ref.dataset_id}.{table_ref.table_id}' exists")
            self.update_columns()
