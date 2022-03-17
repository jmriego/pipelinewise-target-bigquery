import unittest

from target_bigquery import db_sync
from target_bigquery import flattening
from target_bigquery import stream_utils


class TestDBSync(unittest.TestCase):
    """
    Unit Tests
    """

    def setUp(self):
        self.config = {}

    def test_config_validation(self):
        """Test configuration validator"""
        validator = db_sync.validate_config
        empty_config = {}
        minimal_config = {
            'dataset_id': "dummy-value",
            'project_id': "dummy-value",
            'default_target_schema': "dummy-value"
        }

        # Config validator returns a list of errors
        # If the list is empty then the configuration is valid otherwise invalid

        # Empty configuration should fail - (nr_of_errors >= 0)
        self.assertGreater(len(validator(empty_config)), 0)

        # Minimal configuratino should pass - (nr_of_errors == 0)
        self.assertEqual(len(validator(minimal_config)), 0)

        # Configuration without schema references - (nr_of_errors >= 0)
        config_with_no_schema = minimal_config.copy()
        config_with_no_schema.pop('default_target_schema')
        self.assertGreater(len(validator(config_with_no_schema)), 0)

        # Configuration with schema mapping - (nr_of_errors >= 0)
        config_with_schema_mapping = minimal_config.copy()
        config_with_schema_mapping.pop('default_target_schema')
        config_with_schema_mapping['schema_mapping'] = {
            "dummy_stream": {
                "target_schema": "dummy_schema"
            }
        }
        self.assertEqual(len(validator(config_with_schema_mapping)), 0)

    def test_column_schema_mapping(self):
        """Test JSON type to BigQuery column type mappings"""
        def mapper(schema_property):
            field = db_sync.column_schema('dummy', schema_property)
            return field.field_type, field.mode

        # Incoming JSON schema types
        json_str = {"type": ["string"]}
        json_str_or_null = {"type": ["string", "null"]}
        json_dt = {"type": ["string"], "format": "date-time"}
        json_dt_or_null = {"type": ["string", "null"], "format": "date-time"}
        json_t = {"type": ["string"], "format": "time"}
        json_t_or_null = {"type": ["string", "null"], "format": "time"}
        json_num = {"type": ["number"]}
        json_int = {"type": ["integer"]}
        json_int_or_str = {"type": ["integer", "string"]}
        json_bool = {"type": ["boolean"]}
        json_obj = {"type": ["object"]}
        json_arr = {"type": ["array"]}
        jsonb = {"type": ["null", "object"]}
        jsonb_props = {
            "type": ["null", "object"],
            "properties": {
                "prop1": json_int,
                "prop2": json_str
            }
        }
        jsonb_arr_str = {
            "type": ["array"],
            "items": {"type": ["string"]}
        }
        jsonb_arr_unstructured = {
            "type": ["array"],
            "items": {"type": ["object"], "properties": {}}
        }
        jsonb_arr_records = {
            "type": ["array"],
            "items": {
                "type": ["object"],
                "properties": {
                    "prop1": json_int,
                    "prop2": json_str
                }
            }
        }

        # Mapping from JSON schema types ot BigQuery column types
        self.assertEqual(mapper(json_str), ('string', 'NULLABLE'))
        self.assertEqual(mapper(json_str_or_null), ('string', 'NULLABLE'))
        self.assertEqual(mapper(json_dt), ('timestamp', 'NULLABLE'))
        self.assertEqual(mapper(json_dt_or_null), ('timestamp', 'NULLABLE'))
        self.assertEqual(mapper(json_t), ('time', 'NULLABLE'))
        self.assertEqual(mapper(json_t_or_null), ('time', 'NULLABLE'))
        self.assertEqual(mapper(json_num), ('numeric', 'NULLABLE'))
        self.assertEqual(mapper(json_int), ('integer', 'NULLABLE'))
        self.assertEqual(mapper(json_int_or_str), ('string', 'NULLABLE'))
        self.assertEqual(mapper(json_bool), ('boolean', 'NULLABLE'))
        self.assertEqual(mapper(json_obj), ('string', 'NULLABLE'))
        self.assertEqual(mapper(json_arr), ('string', 'NULLABLE'))
        self.assertEqual(mapper(jsonb), ('string', 'NULLABLE'))
        self.assertEqual(mapper(jsonb_props), ('RECORD', 'NULLABLE'))
        self.assertEqual(mapper(jsonb_arr_str), ('string', 'REPEATED'))
        self.assertEqual(mapper(jsonb_arr_unstructured), ('string', 'REPEATED'))
        self.assertEqual(mapper(jsonb_arr_records), ('RECORD', 'REPEATED'))

    def test_stream_name_to_dict(self):
        """Test identifying catalog, schema and table names from fully qualified stream and table names"""
        # Singer stream name format (Default '-' separator)
        self.assertEqual(
            stream_utils.stream_name_to_dict('my_table'),
            {"catalog_name": None, "schema_name": None, "table_name": "my_table"})

        # Singer stream name format (Default '-' separator)
        self.assertEqual(
            stream_utils.stream_name_to_dict('my_schema-my_table'),
            {"catalog_name": None, "schema_name": "my_schema", "table_name": "my_table"})

        # Singer stream name format (Default '-' separator)
        self.assertEqual(
            stream_utils.stream_name_to_dict('my_catalog-my_schema-my_table'),
            {"catalog_name": "my_catalog", "schema_name": "my_schema", "table_name": "my_table"})

        # BigQuery table format (Custom '.' separator)
        self.assertEqual(
            stream_utils.stream_name_to_dict('my_table', separator='.'),
            {"catalog_name": None, "schema_name": None, "table_name": "my_table"})

        # BigQuery table format (Custom '.' separator)
        self.assertEqual(
            stream_utils.stream_name_to_dict('my_schema.my_table', separator='.'),
            {"catalog_name": None, "schema_name": "my_schema", "table_name": "my_table"})

        # BigQuery table format (Custom '.' separator)
        self.assertEqual(
            stream_utils.stream_name_to_dict('my_catalog.my_schema.my_table', separator='.'),
            {"catalog_name": "my_catalog", "schema_name": "my_schema", "table_name": "my_table"})

    def test_flatten_schema(self):
        """Test flattening of SCHEMA messages"""
        flatten_schema = flattening.flatten_schema

        # Schema with no object properties should be empty dict
        schema_with_no_properties = {"type": "object"}
        self.assertEqual(flatten_schema(schema_with_no_properties), {})

        not_nested_schema = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]}}}

        # NO FLATTENING - Schema with simple properties should be a plain dictionary
        self.assertEqual(flatten_schema(not_nested_schema), not_nested_schema['properties'])

        nested_schema_with_no_properties = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]},
                "c_obj": {"type": ["null", "object"]}}}

        # NO FLATTENING - Schema with object type property but without further properties should be a plain dictionary
        self.assertEqual(flatten_schema(nested_schema_with_no_properties),
                          nested_schema_with_no_properties['properties'])

        nested_schema_with_properties = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]},
                "c_obj": {
                    "type": ["null", "object"],
                    "properties": {
                        "nested_prop1": {"type": ["null", "string"]},
                        "nested_prop2": {"type": ["null", "string"]},
                        "nested_prop3": {
                            "type": ["null", "object"],
                            "properties": {
                                "multi_nested_prop1": {"type": ["null", "string"]},
                                "multi_nested_prop2": {"type": ["null", "string"]}
                            }
                        }
                    }
                }
            }
        }

        # NO FLATTENING - Schema with object type property but without further properties should be a plain dictionary
        # No flattening (default)
        self.assertEqual(flatten_schema(nested_schema_with_properties), nested_schema_with_properties['properties'])

        # NO FLATTENING - Schema with object type property but without further properties should be a plain dictionary
        #   max_level: 0 : No flattening (default)
        self.assertEqual(flatten_schema(nested_schema_with_properties, max_level=0),
                          nested_schema_with_properties['properties'])

        # FLATTENING - Schema with object type property but without further properties should be a dict with
        # flattened properties
        self.assertEqual(flatten_schema(nested_schema_with_properties, max_level=1),
                          {
                              'c_pk': {'type': ['null', 'integer']},
                              'c_varchar': {'type': ['null', 'string']},
                              'c_int': {'type': ['null', 'integer']},
                              'c_obj__nested_prop1': {'type': ['null', 'string']},
                              'c_obj__nested_prop2': {'type': ['null', 'string']},
                              'c_obj__nested_prop3': {
                                  'type': ['null', 'object'],
                                  "properties": {
                                      "multi_nested_prop1": {"type": ["null", "string"]},
                                      "multi_nested_prop2": {"type": ["null", "string"]}
                                  }
                              }
                          })

        # FLATTENING - Schema with object type property but without further properties should be a dict with
        # flattened properties
        self.assertEqual(flatten_schema(nested_schema_with_properties, max_level=10),
                          {
                              'c_pk': {'type': ['null', 'integer']},
                              'c_varchar': {'type': ['null', 'string']},
                              'c_int': {'type': ['null', 'integer']},
                              'c_obj__nested_prop1': {'type': ['null', 'string']},
                              'c_obj__nested_prop2': {'type': ['null', 'string']},
                              'c_obj__nested_prop3__multi_nested_prop1': {'type': ['null', 'string']},
                              'c_obj__nested_prop3__multi_nested_prop2': {'type': ['null', 'string']}
                          })

    def test_flatten_record(self):
        """Test flattening of RECORD messages"""
        flatten_record = flattening.flatten_record

        empty_record = {}
        # Empty record should be empty dict
        self.assertEqual(flatten_record(empty_record), {})

        not_nested_record = {"c_pk": 1, "c_varchar": "1", "c_int": 1}
        # NO FLATTENING - Record with simple properties should be a plain dictionary
        self.assertEqual(flatten_record(not_nested_record), not_nested_record)

        nested_record = {
            "c_pk": 1,
            "c_varchar": "1",
            "c_int": 1,
            "c_obj": {
                "nested_prop1": "value_1",
                "nested_prop2": "value_2",
                "nested_prop3": {
                    "multi_nested_prop1": "multi_value_1",
                    "multi_nested_prop2": "multi_value_2",
                }}}

        # NO FLATTENING - No flattening (default)
        self.maxDiff = None
        self.assertEqual(flatten_record(nested_record),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj": {"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {
                                       "multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}
                          })

        # NO FLATTENING
        #   max_level: 0 : No flattening (default)
        self.assertEqual(flatten_record(nested_record, max_level=0),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj": {"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {
                                       "multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}
                          })

        # SEMI FLATTENING
        #   max_level: 1 : Semi-flattening (default)
        self.assertEqual(flatten_record(nested_record, max_level=1),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj__nested_prop1": "value_1",
                              "c_obj__nested_prop2": "value_2",
                              "c_obj__nested_prop3": {"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": 
                                                     "multi_value_2"}
                          })

        # FLATTENING
        self.assertEqual(flatten_record(nested_record, max_level=10),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj__nested_prop1": "value_1",
                              "c_obj__nested_prop2": "value_2",
                              "c_obj__nested_prop3__multi_nested_prop1": "multi_value_1",
                              "c_obj__nested_prop3__multi_nested_prop2": "multi_value_2"
                          })

    def test_nested_keys(self):
        """Test recursive renaming of keys in RECORD messages"""
        flatten_record = flattening.flatten_record

        empty_record = {}
        # Empty record should be empty dict
        self.assertEqual(flatten_record(empty_record), {})

        not_nested_record = {"c_pk": 1, "c_varchar": "1", "c_int": 1}
        # NO FLATTENING - Record with simple properties should be a plain dictionary
        self.assertEqual(flatten_record(not_nested_record), not_nested_record)

        # Include some uppercase and hyphens in nested keys to test that flatten_record
        # fixes the key names recursively in all dicts to match schema
        nested_record = {
            "c_pk": 1,
            "c_varchar": "1",
            "C-Int": 1,
            "c_obj": {
                "Nested-Prop1": "value_1",
                "Nested-Prop2": "value_2",
                "Nested-Prop3": {
                    "multi_Nested-Prop1": "multi_value_1",
                    "multi_Nested-Prop2": "multi_value_2",
                }}}

        # NO FLATTENING - No flattening (default)
        self.maxDiff = None
        self.assertEqual(flatten_record(nested_record),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj": {"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {
                                       "multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}
                          })

        # NO FLATTENING
        #   max_level: 0 : No flattening (default)
        self.assertEqual(flatten_record(nested_record, max_level=0),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj": {"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {
                                       "multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}
                          })

        # SEMI FLATTENING
        #   max_level: 1 : Semi-flattening (default)
        self.assertEqual(flatten_record(nested_record, max_level=1),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj__nested_prop1": "value_1",
                              "c_obj__nested_prop2": "value_2",
                              "c_obj__nested_prop3": {"multi_nested_prop1": "multi_value_1", "multi_nested_prop2":
                                                     "multi_value_2"}
                          })

        # FLATTENING
        self.assertEqual(flatten_record(nested_record, max_level=10),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj__nested_prop1": "value_1",
                              "c_obj__nested_prop2": "value_2",
                              "c_obj__nested_prop3__multi_nested_prop1": "multi_value_1",
                              "c_obj__nested_prop3__multi_nested_prop2": "multi_value_2"
                          })
