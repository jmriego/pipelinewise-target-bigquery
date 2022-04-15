import json
import sys
import singer
import re
import time
import datetime
from decimal import Decimal, getcontext

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import Conflict

from target_bigquery import flattening
from target_bigquery import stream_utils
from target_bigquery.stream_ref_helper import StreamRefHelper
from target_bigquery import sql_utils

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
    fields = [column_schema(col, t) for col, t in schema_property.get('properties', {}).items()]
    if fields:
        return SchemaField(safe_name, 'RECORD', mode, fields=fields)
    else:
        return SchemaField(safe_name, 'string', mode)


def column_schema(name, schema_property):
    safe_name = sql_utils.safe_column_name(name, quotes=False)
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


def column_schema_avro(name, schema_property):
    property_type = schema_property['type']
    property_format = schema_property.get('format', None)
    result = {"name": sql_utils.safe_column_name(name, quotes=False)}

    if 'array' in property_type:
        try:
            items_type = column_schema_avro(name, schema_property['items'])
            result_type = {
                'type': 'array',
                'items': items_type['type']}
        except KeyError:
            result_type = 'string'
    elif 'object' in property_type:
        items_types = [
            column_schema_avro(col, schema_property)
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
            'logicalType': 'timestamp-micros'}
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


def is_unstructured_object(props):
    """Check if property is object and it has no properties."""
    return 'object' in props['type'] and not props.get('properties')


def primary_column_names(stream_schema_message):
    try:
        return [sql_utils.safe_column_name(p) for p in stream_schema_message['key_properties']]
    except KeyError:
        return []


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

        project_id = self.connection_config['project_id']
        location = self.connection_config.get('location', None)
        self.client = bigquery.Client(project=project_id, location=location)

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
            stream_schema_name = stream_utils.stream_name_to_dict(stream_name)['schema_name']
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
            self.flatten_schema = flattening.flatten_schema(stream_schema_message['schema'], max_level=self.data_flattening_max_level)
            self.ref_helper = StreamRefHelper(
                                           self.connection_config['project_id'],
                                           self.schema_name,
                                           self.connection_config.get('temp_schema')
                                       )
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

    def record_primary_key_string(self, record):
        if len(self.stream_schema_message['key_properties']) == 0:
            return None
        flatten = flattening.flatten_record(record, max_level=self.data_flattening_max_level)
        primary_keys = [sql_utils.safe_column_name(p, quotes=False) for p in self.stream_schema_message['key_properties']]
        try:
            key_props = [str(flatten[p]) for p in primary_keys]
        except Exception as exc:
            logger.info("Cannot find {} primary key(s) in record: {}".format(primary_keys, flatten))
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
             "fields": [column_schema_avro(name, c) for name, c in self.flatten_schema.items()]}

        if re.search(pattern, schema['name']):
            schema["alias"] = schema['name']
            schema["name"] = re.sub(pattern, "_", schema['name'])

        return schema

    # TODO: write tests for the json.dumps lines below and verify nesting
    # TODO: improve performance
    def records_to_avro(self, records):
        for record in records:
            flatten = flattening.flatten_record(record, max_level=self.data_flattening_max_level)
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
        target_table_ref = self.ref_helper.table_ref_from_stream(stream, is_temporary=False)
        target_table = self.client.get_table(target_table_ref)
        logger.info("Loading {} rows into '{}'".format(count, target_table_ref.table_id))

        temp_table_ref = self.ref_helper.table_ref_from_stream(stream, is_temporary=True)

        logger.info("INSERTING INTO {} ({})".format(
            temp_table_ref.table_id,
            ', '.join(self.column_names())
        ))

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.AVRO
        job_config.use_avro_logical_types = True
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job = self.client.load_table_from_file(f, temp_table_ref, job_config=job_config)
        job.result()
        temp_table = self.client.get_table(temp_table_ref)

        pk_columns_names = primary_column_names(self.stream_schema_message)
        if pk_columns_names:
            # TODO: make temp table creation and DML atomic with merge
            query = sql_utils.merge_from_table_sql(temp_table,
                                                   target_table,
                                                   self.column_names(),
                                                   self.renamed_columns,
                                                   pk_columns_names)
        else:
            query = sql_utils.insert_from_table_sql(temp_table,
                                                    target_table,
                                                    self.column_names())
        drop_temp_query = sql_utils.drop_table_sql(temp_table)
        results = self.query([query, drop_temp_query])
        logger.info('LOADED {} rows'.format(results.num_dml_affected_rows))

    def column_names(self):
        return [sql_utils.safe_column_name(name) for name in self.flatten_schema]

    def create_table(self, is_temporary=False):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']

        table_ref = self.ref_helper.table_ref_from_stream(stream, is_temporary)

        schema = [
            column_schema(
                name,
                schema
            )
            for (name, schema) in self.flatten_schema.items()
        ]

        table = bigquery.Table(table_ref, schema=schema)
        if is_temporary:
            table.expires = datetime.datetime.now() + datetime.timedelta(days=1)

        self.client.create_table(table)

    def grant_usage_on_schema(self, schema_name, grantee):
        query = "GRANT USAGE ON SCHEMA {} TO GROUP {}".format(schema_name, grantee)
        logger.info("Granting USAGE privilegue on '{}' schema to '{}'... {}".format(schema_name, grantee, query))
        self.query(query)

    def grant_select_on_all_tables_in_schema(self, schema_name, grantee):
        query = "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO GROUP {}".format(schema_name, grantee)
        logger.info("Granting SELECT ON ALL TABLES privilegue on '{}' schema to '{}'... {}".format(schema_name, grantee, query))
        self.query(query)

    @classmethod
    def grant_privilege(cls, schema, grantees, grant_method):
        if isinstance(grantees, list):
            for grantee in grantees:
                grant_method(schema, grantee)
        elif isinstance(grantees, str):
            grant_method(schema, grantees)

    def delete_rows(self, stream):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']

        table_ref = self.ref_helper.table_ref_from_stream(stream, is_temporary=False)
        table_id = table_ref.table_id
        query = "DELETE FROM {} WHERE _sdc_deleted_at IS NOT NULL".format(
                    sql_utils.safe_table_ref(table_ref))
        logger.info("Deleting rows from '{}' table... {}".format(table_id, query))
        logger.info("DELETE {}".format(self.query(query).result().total_rows))

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

    def get_table_columns(self, table_ref: bigquery.TableReference):
        table = self.client.get_table(table_ref)
        return {field.name: field for field in table.schema}

    def update_columns(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_ref = self.ref_helper.table_ref_from_stream(stream, is_temporary=False)
        columns = self.get_table_columns(table_ref)

        columns_to_add = [
            column_schema(name, properties_schema)
            for (name, properties_schema) in self.flatten_schema.items()
            if sql_utils.safe_column_name(name, quotes=False) not in columns
        ]

        if columns_to_add:
            self.add_columns(columns_to_add, stream)

        columns_to_replace = [
            column_schema(name, properties_schema)
            for (name, properties_schema) in self.flatten_schema.items()
            if name.lower() in columns and
               columns[name.lower()] != column_schema(name, properties_schema)
        ]

        for field in columns_to_replace:
            self.version_column(field, stream)

    def update_clustering_fields(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_ref = self.ref_helper.table_ref_from_stream(stream, is_temporary=False)
        table = self.client.get_table(table_ref)  # API request

        new_clustering_fields = [
            self.renamed_columns.get(c, c) for c in primary_column_names(self.stream_schema_message)
        ]
        if not table.clustering_fields:
            logger.info('Clustering table on fields: {}'.format(new_clustering_fields))
            table.clustering_fields = new_clustering_fields
            self.client.update_table(table, ['clustering_fields'])
        # avoid changing existing clusters so its possible to manually change clustering of a table outside of this target
        elif table.clustering_fields != new_clustering_fields:
            logger.info('Primary key fields have changed. Uncluster the table to allow the change: {}'.format(new_clustering_fields))

    def version_column(self, field, stream):
        column = sql_utils.safe_column_name(field.name, quotes=False)
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

        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_ref = self.ref_helper.table_ref_from_stream(stream, is_temporary=False)
        table_columns = self.get_table_columns(table_ref)

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
            self.add_columns([self.alias_field(field, field_with_type_suffix)], stream)
            self.renamed_columns[column] = field_with_type_suffix

    def add_columns(self, fields, stream):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_ref = self.ref_helper.table_ref_from_stream(stream, is_temporary=False)
        table = self.client.get_table(table_ref)  # API request

        schema = table.schema[:]
        schema.extend(fields)
        table.schema = schema

        logger.info('Adding columns: {}'.format([field.name for field in fields]))
        self.client.update_table(table, ['schema'])  # API request

    def sync_table(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_ref = self.ref_helper.table_ref_from_stream(stream, is_temporary=False)

        table_name_with_schema = f'{table_ref.dataset_id}.{table_ref.table_id}'
        try:
            self.create_table()
            logger.info("Table '{}' does not exist. Creating...".format(table_name_with_schema))
            self.grant_privilege(self.schema_name, self.grantees, self.grant_select_on_all_tables_in_schema)
        except Conflict:
            logger.info("Table '{}' exists".format(table_name_with_schema))
            self.update_columns()
        self.update_clustering_fields()
