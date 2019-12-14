import json
import sys
import singer
import collections
import re
import uuid
import itertools
import time

from google.cloud import bigquery
from google.cloud.bigquery.job import SourceFormat
from google.cloud.bigquery import Dataset, WriteDisposition
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery import LoadJobConfig
from google.api_core import exceptions

logger = singer.get_logger()


def validate_config(config):
    errors = []
    required_config_keys = [
        'dataset_id',
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


def column_type(schema_property):
    property_type = schema_property['type']
    property_format = schema_property.get('format', None)
    # TODO: Add the STRUCT/RECORD type
    if 'object' in property_type or 'array' in property_type:
        column_type = 'struct'

    # Every date-time JSON value is currently mapped to TIMESTAMP WITHOUT TIME ZONE
    #
    # TODO: Detect if timezone postfix exists in the JSON and find if TIMESTAMP WITHOUT TIME ZONE or
    # TIMESTAMP WITH TIME ZONE is the better column type
    elif property_format == 'date-time':
        column_type = 'timestamp'
    elif property_format == 'time':
        column_type = 'time'
    elif 'number' in property_type:
        column_type = 'numeric'
    elif 'integer' in property_type and 'string' in property_type:
        column_type = 'string'
    elif 'integer' in property_type:
        column_type = 'int64'
    elif 'boolean' in property_type:
        column_type = 'bool'
    else:
        column_type = 'string'

    return column_type


def safe_column_name(name):
    return '`{}`'.format(name).lower()


def column_clause(name, schema_property):
    return '{} {}'.format(safe_column_name(name), column_type(schema_property))


def primary_column_names(stream_schema_message):
    return [safe_column_name(p) for p in stream_schema_message['key_properties']]


def stream_name_to_dict(stream_name, separator='-'):
    catalog_name = None
    schema_name = None
    table_name = stream_name

    # Schema and table name can be derived from stream if it's in <schema_nama>-<table_name> format
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
        if stream_schema_message is not None:
            # Define initial list of indices to created
            self.hard_delete = self.connection_config.get('hard_delete')
            if self.hard_delete:
                self.indices = ['_sdc_deleted_at']
            else:
                self.indices = []

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
            #                                                   "target_schema": "my_postgres_schema",
            #                                                   "target_schema_select_permissions": [ "role_with_select_privs" ],
            #                                                   "indices": ["column_1", "column_2s"]
            #                                               }
            #                                           }
            config_default_target_schema = self.connection_config.get('default_target_schema', '').strip()
            config_schema_mapping = self.connection_config.get('schema_mapping', {})

            stream_name = stream_schema_message['stream']
            stream_schema_name = stream_name_to_dict(stream_name)['schema_name']
            stream_table_name = stream_name_to_dict(stream_name)['table_name']
            if config_schema_mapping and stream_schema_name in config_schema_mapping:
                self.schema_name = config_schema_mapping[stream_schema_name].get('target_schema')

                # Get indices to create for the target table
                indices = config_schema_mapping[stream_schema_name].get('indices', {})
                if stream_table_name in indices:
                    self.indices.extend(indices.get(stream_table_name, []))

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
            #                                                                   "target_schema": "my_postgres_schema",
            #                                                                   "target_schema_select_permissions": [ "role_with_select_privs" ]
            #                                                               }
            #                                                           }
            self.grantees = self.connection_config.get('default_target_schema_select_permissions')
            if config_schema_mapping and stream_schema_name in config_schema_mapping:
                self.grantees = config_schema_mapping[stream_schema_name].get('target_schema_select_permissions', self.grantees)

            self.schema = stream_schema_message['schema']['properties']


    def open_connection(self):
        project_id = self.connection_config['project_id']
        return bigquery.Client(project=project_id)

    def query(self, query, params=[]):
        def to_query_parameter(value):
            if isinstance(value, int):
                value_type = "INT64"
            elif isinstance(value, float):
                value_type = "NUMERIC"
            elif isinstance(value, float):
                value_type = "FLOAT64"
            elif isinstance(value, bool):
                value_type = "BOOL"
            else:
                value_type = "STRING"
            return bigquery.ScalarQueryParameter(None, value_type, value)

        job_config = bigquery.QueryJobConfig()
        query_params = [to_query_parameter(p) for p in params]
        job_config.query_parameters = query_params

        client = self.open_connection()
        logger.info("TARGET_BIGQUERY - Running query: {}".format(query))
        query_job = client.query(query, job_config=job_config)
        query_job.result()

        return query_job

    def table_name(self, stream_name, is_temporary=False, without_schema=False):
        stream_dict = stream_name_to_dict(stream_name)
        table_name = stream_dict['table_name']
        bq_table_name = table_name.replace('.', '_').replace('-', '_').lower()

        if is_temporary:
            bq_table_name = 'tmp_{}'.format(str(uuid.uuid4()).replace('-', '_'))

        if without_schema:
            return '{}'.format(bq_table_name)
        else:
            return '{}.{}'.format(self.schema_name, bq_table_name)

    def record_primary_key_string(self, record):
        if len(self.stream_schema_message['key_properties']) == 0:
            return None
        try:
            key_props = [str(record[p]) for p in self.stream_schema_message['key_properties']]
        except Exception as exc:
            logger.info("Cannot find {} primary key(s) in record: {}".format(self.stream_schema_message['key_properties'], record))
            raise exc
        return ','.join(key_props)

    def record_to_json_line(self, record):
        return json.dumps(record, ensure_ascii=False)

    def load_json(self, file, count):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        logger.info("Loading {} rows into '{}'".format(count, self.table_name(stream, False)))

        client = self.open_connection()
        # TODO: make temp table creation and DML atomic with merge
        temp_table = self.table_name(stream_schema_message['stream'], is_temporary=True, without_schema=True)
        query = self.create_table_query(table_name=temp_table, is_temporary=True)
        self.query(query)

        logger.info("INSERTING INTO {} ({})".format(
            temp_table,
            ', '.join(self.column_names())
        ))

        dataset_id = self.connection_config.get('dataset_id').strip()
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(temp_table)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        file_fields = [SchemaField(name, column_type(schema)) for name, schema in self.schema.items()]
        job = client.load_table_from_file(file, table_ref, job_config=job_config)
        # job.schema = file_fields
        job.result()

        if len(self.stream_schema_message['key_properties']) > 0:
            query = self.update_from_temp_table(temp_table)
        else:
            query = self.insert_from_temp_table(temp_table)
        results = self.query(query)
        logger.info('LOADED {} rows'.format(results.num_dml_affected_rows))


    def insert_from_temp_table(self, temp_table):
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        temp_schema = self.connection_config('temp_schema', self.schema_name)
        table = self.table_name(stream_schema_message['stream'])

        if len(stream_schema_message['key_properties']) == 0:
            return """INSERT INTO {} ({})
                    (SELECT s.* FROM {}.{} s)
                    """.format(
                table,
                ', '.join(columns),
                temp_schema,
                temp_table
            )

        return """INSERT INTO {} ({})
        (SELECT s.* FROM {}.{} s LEFT OUTER JOIN {} t ON {} WHERE {})
        """.format(
            table,
            ', '.join(columns),
            temp_schema,
            temp_table,
            table,
            self.primary_key_condition('t'),
            self.primary_key_null_condition('t')
        )

    def update_from_temp_table(self, temp_table):
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        table = self.table_name(stream_schema_message['stream'])
        table_without_schema = self.table_name(stream_schema_message['stream'], without_schema=True)
        temp_schema = self.connection_config('temp_schema', self.schema_name)

        return """MERGE {table}
        USING {temp_schema}.{temp_table} s
        ON {primary_key_condition}
        WHEN MATCHED THEN
            UPDATE SET {set_values}
        WHEN NOT MATCHED THEN
            INSERT ({cols}) VALUES ({cols})
        """.format(
            table=table,
            temp_schema=temp_schema,
            temp_table=temp_table,
            primary_key_condition=self.primary_key_condition(table_without_schema),
            set_values=', '.join(['{}=s.{}'.format(c, c) for c in columns]),
            cols=', '.join(c for c in columns))

    def primary_key_condition(self, right_table):
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(['s.{} = {}.{}'.format(c, right_table, c) for c in names])

    def primary_key_null_condition(self, right_table):
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(['{}.{} is null'.format(right_table, c) for c in names])

    def column_names(self):
        return [safe_column_name(name) for name in self.schema]

    def create_table_query(self, table_name=None, is_temporary=False):
        stream_schema_message = self.stream_schema_message
        columns = [
            column_clause(
                name,
                schema
            )
            for (name, schema) in self.schema.items()
        ]

        if not table_name:
            gen_table_name = self.table_name(stream_schema_message['stream'], is_temporary=is_temporary)

        return 'CREATE {}TABLE IF NOT EXISTS {} ({})'.format(
            'TEMP ' if is_temporary else '',
            table_name if table_name else gen_table_name,
            ', '.join(columns)
        )

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
        table = self.table_name(stream)
        query = "DELETE FROM {} WHERE _sdc_deleted_at IS NOT NULL RETURNING _sdc_deleted_at".format(table)
        logger.info("Deleting rows from '{}' table... {}".format(table, query))
        logger.info("DELETE {}".format(len(self.query(query))))

    def create_schema_if_not_exists(self, table_columns_cache=None):
        schema_name = self.schema_name
        schema_rows = 0

        # table_columns_cache is an optional pre-collected list of available objects in postgres
        if table_columns_cache:
            schema_rows = list(filter(lambda x: x['TABLE_SCHEMA'] == schema_name, table_columns_cache))
        # Query realtime if not pre-collected
        else:
            schema_rows = self.query(
                'SELECT LOWER(schema_name) schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE LOWER(schema_name) = ?',
                (schema_name.lower(),)
            )

        if schema_rows.result().total_rows == 0:
            logger.info("Schema '{}' does not exist. Creating...".format(schema_name))
            client = self.open_connection()
            dataset = client.create_dataset(schema_name)

            self.grant_privilege(schema_name, self.grantees, self.grant_usage_on_schema)

    def get_tables(self):
        return self.query(
            'SELECT table_name FROM {schema}.INFORMATION_SCHEMA.TABLES'
            .format(schema=self.schema_name)
        )

    def get_table_columns(self, table_name):
        return self.query("""SELECT column_name, data_type
      FROM {}.INFORMATION_SCHEMA.COLUMNS
      WHERE lower(table_name) = '{}'""".format(self.schema_name.lower(), table_name.lower()))

    def update_columns(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, without_schema=True)
        columns = self.get_table_columns(table_name)
        columns_dict = {column['column_name'].lower(): column for column in columns}

        columns_to_add = [
            column_clause(
                name,
                properties_schema
            )
            for (name, properties_schema) in self.schema.items()
            if name.lower() not in columns_dict
        ]

        for column in columns_to_add:
            self.add_column(column, stream)

        columns_to_replace = [
            (safe_column_name(name), column_clause(
                name,
                properties_schema
            ))
            for (name, properties_schema) in self.schema.items()
            if name.lower() in columns_dict and
               columns_dict[name.lower()]['data_type'].lower() != column_type(properties_schema).lower()
        ]

        for (column_name, column) in columns_to_replace:
            self.version_column(column_name, stream)
            self.add_column(column, stream)


    def drop_column(self, column_name, stream):
        drop_column = "ALTER TABLE {} DROP COLUMN {}".format(self.table_name(stream), column_name)
        logger.info('Dropping column: {}'.format(drop_column))
        self.query(drop_column)

    def version_column(self, column_name, stream):
        version_column = "ALTER TABLE {} RENAME COLUMN {} TO \"{}_{}\"".format(self.table_name(stream, False), column_name, column_name.replace("\"",""), time.strftime("%Y%m%d_%H%M"))
        logger.info('Dropping column: {}'.format(version_column))
        self.query(version_column)

    def add_column(self, column, stream):
        add_column = "ALTER TABLE {} ADD COLUMN {}".format(self.table_name(stream), column)
        logger.info('Adding column: {}'.format(add_column))
        self.query(add_column)

    def sync_table(self):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, without_schema=True)
        found_tables = [table for table in (self.get_tables()) if table['table_name'].lower() == table_name]
        if len(found_tables) == 0:
            query = self.create_table_query()
            logger.info("Table '{}' does not exist. Creating... {}".format(table_name, query))
            self.query(query)

            self.grant_privilege(self.schema_name, self.grantees, self.grant_select_on_all_tables_in_schema)
        else:
            logger.info("Table '{}' exists".format(table_name))
            self.update_columns()

