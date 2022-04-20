import datetime
import json
import os
import unittest.mock as mock
from datetime import timezone
from decimal import Decimal, getcontext

import target_bigquery
from target_bigquery.db_sync import DbSync, PRECISION

try:
    import tests.utils as test_utils
except ImportError:
    import utils as test_utils

def query(bigquery, query):
    result = bigquery.query(query)
    return [dict(row.items()) for row in result]


class TestIntegrationSchema(test_utils.TestIntegration):
    """
    Integration Tests about reading streams
    """
    def assert_three_streams_are_into_bigquery(self, should_metadata_columns_exist=False,
                                                should_hard_deleted_rows=False):
        """
        This is a helper assertion that checks if every data from the message-with-three-streams.json
        file is available in BigQuery tables correctly.
        Useful to check different loading methods (unencrypted, Client-Side encryption, gzip, etc.)
        without duplicating assertions
        """
        bigquery = DbSync(self.config)
        default_target_schema = self.config.get('default_target_schema', '')
        schema_mapping = self.config.get('schema_mapping', {})

        # Identify target schema name
        target_schema = None
        if default_target_schema is not None and default_target_schema.strip():
            target_schema = default_target_schema
        elif schema_mapping:
            target_schema = "tap_mysql_test"

        # Get loaded rows from tables
        table_one = query(bigquery, "SELECT * FROM {}.test_table_one ORDER BY c_pk".format(target_schema))
        table_two = query(bigquery, "SELECT * FROM {}.test_table_two ORDER BY c_pk".format(target_schema))
        table_three = query(bigquery, "SELECT * FROM {}.test_table_three ORDER BY c_pk".format(target_schema))

        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'c_int': 1, 'c_pk': 1, 'c_varchar': '1'}
        ]

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_one), expected_table_one)

        # ----------------------------------------------------------------------
        # Check rows in table_tow
        # ----------------------------------------------------------------------
        expected_table_two = []
        if not should_hard_deleted_rows:
            expected_table_two = [
                {'c_int': 1, 'c_pk': 1, 'c_varchar': '1', 'c_date': datetime.datetime(2019, 2, 1, 15, 12, 45, tzinfo=timezone.utc)},
                {'c_int': 2, 'c_pk': 2, 'c_varchar': '2', 'c_date': datetime.datetime(2019, 2, 10, 2, 0, 0, tzinfo=timezone.utc)}
            ]
        else:
            expected_table_two = [
                {'c_int': 2, 'c_pk': 2, 'c_varchar': '2', 'c_date': datetime.datetime(2019, 2, 10, 2, 0, 0, tzinfo=timezone.utc)}
            ]

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_two), expected_table_two)

        # ----------------------------------------------------------------------
        # Check rows in table_three
        # ----------------------------------------------------------------------
        expected_table_three = []
        if not should_hard_deleted_rows:
            expected_table_three = [
                {'c_int': 1, 'c_pk': 1, 'c_varchar': '1', 'c_time': datetime.time(4, 0, 0)},
                {'c_int': 2, 'c_pk': 2, 'c_varchar': '2', 'c_time': datetime.time(7, 15, 0)},
                {'c_int': 3, 'c_pk': 3, 'c_varchar': '3', 'c_time': datetime.time(23, 0, 3)}
            ]
        else:
            expected_table_three = [
                {'c_int': 1, 'c_pk': 1, 'c_varchar': '1', 'c_time': datetime.time(4, 0, 0)},
                {'c_int': 2, 'c_pk': 2, 'c_varchar': '2', 'c_time': datetime.time(7, 15, 0)}
            ]

        self.assertEqual(
            self.remove_metadata_columns_from_rows(table_three), expected_table_three)

        # ----------------------------------------------------------------------
        # Check if metadata columns exist or not
        # ----------------------------------------------------------------------
        if should_metadata_columns_exist:
            self.assert_metadata_columns_exist(table_one)
            self.assert_metadata_columns_exist(table_two)
            self.assert_metadata_columns_exist(table_three)
        else:
            self.assert_metadata_columns_not_exist(table_one)
            self.assert_metadata_columns_not_exist(table_two)
            self.assert_metadata_columns_not_exist(table_three)

    def assert_logical_streams_are_in_bigquery(self, should_metadata_columns_exist=False):
        # Get loaded rows from tables
        bigquery = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = query(bigquery, "SELECT * FROM {}.logical1_table1 ORDER BY cid".format(target_schema))
        table_two = query(bigquery, "SELECT * FROM {}.logical1_table2 ORDER BY cid".format(target_schema))
        table_three = query(bigquery, "SELECT * FROM {}.logical2_table1 ORDER BY cid".format(target_schema))
        table_four = query(bigquery, "SELECT cid, ctimentz, ctimetz FROM {}.logical1_edgydata WHERE cid IN(1,2,3,4,5,6,8,9) ORDER BY cid".format(target_schema))

        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'cid': 1, 'cvarchar': "inserted row", 'cvarchar2': None},
            {'cid': 2, 'cvarchar': 'inserted row', "cvarchar2": "inserted row"},
            {'cid': 3, 'cvarchar': "inserted row", 'cvarchar2': "inserted row"},
            {'cid': 4, 'cvarchar': "inserted row", 'cvarchar2': "inserted row"}
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_two
        # ----------------------------------------------------------------------
        expected_table_two = [
            {'cid': 1, 'cvarchar': "updated row"},
            {'cid': 2, 'cvarchar': 'updated row'},
            {'cid': 3, 'cvarchar': "updated row"},
            {'cid': 5, 'cvarchar': "updated row"},
            {'cid': 7, 'cvarchar': "updated row"},
            {'cid': 8, 'cvarchar': 'updated row'},
            {'cid': 9, 'cvarchar': "updated row"},
            {'cid': 10, 'cvarchar': 'updated row'}
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_three
        # ----------------------------------------------------------------------
        expected_table_three = [
            {'cid': 1, 'cvarchar': "updated row"},
            {'cid': 2, 'cvarchar': 'updated row'},
            {'cid': 3, 'cvarchar': "updated row"},
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_four
        # ----------------------------------------------------------------------
        expected_table_four = [
            {'cid': 1, 'ctimentz': None, 'ctimetz': None},
            {'cid': 2, 'ctimentz': datetime.time(23, 0, 15), 'ctimetz': datetime.time(23, 0, 15)},
            {'cid': 3, 'ctimentz': datetime.time(12, 0, 15), 'ctimetz': datetime.time(12, 0, 15)},
            {'cid': 4, 'ctimentz': datetime.time(12, 0, 15), 'ctimetz': datetime.time(9, 0, 15)},
            {'cid': 5, 'ctimentz': datetime.time(12, 0, 15), 'ctimetz': datetime.time(15, 0, 15)},
            {'cid': 6, 'ctimentz': datetime.time(0, 0), 'ctimetz': datetime.time(0, 0)},
            {'cid': 8, 'ctimentz': datetime.time(0, 0), 'ctimetz': datetime.time(1, 0)},
            {'cid': 9, 'ctimentz': datetime.time(0, 0), 'ctimetz': datetime.time(0, 0)}
        ]

        if should_metadata_columns_exist:
            self.assertEqual(self.remove_metadata_columns_from_rows(table_one), expected_table_one)
            self.assertEqual(self.remove_metadata_columns_from_rows(table_two), expected_table_two)
            self.assertEqual(self.remove_metadata_columns_from_rows(table_three), expected_table_three)
            self.assertEqual(table_four, expected_table_four)
        else:
            self.assertEqual(table_one, expected_table_one)
            self.assertEqual(table_two, expected_table_two)
            self.assertEqual(table_three, expected_table_three)
            self.assertEqual(table_four, expected_table_four)

    def assert_logical_streams_are_in_bigquery_and_are_empty(self):
        # Get loaded rows from tables
        bigquery = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = query(bigquery, "SELECT * FROM {}.logical1_table1 ORDER BY cid".format(target_schema))
        table_two = query(bigquery, "SELECT * FROM {}.logical1_table2 ORDER BY cid".format(target_schema))
        table_three = query(bigquery, "SELECT * FROM {}.logical2_table1 ORDER BY cid".format(target_schema))
        table_four = query(bigquery, "SELECT cid, ctimentz, ctimetz FROM {}.logical1_edgydata WHERE cid IN(1,2,3,4,5,6,8,9) ORDER BY cid".format(target_schema))

        self.assertEqual(table_one, [])
        self.assertEqual(table_two, [])
        self.assertEqual(table_three, [])
        self.assertEqual(table_four, [])

    #################################
    #           TESTS               #
    #################################

    def test_invalid_json(self):
        """Receiving invalid JSONs should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-json.json')
        with self.assertRaises(json.decoder.JSONDecodeError):
            self.persist_lines(tap_lines)

    def test_message_order(self):
        """RECORD message without a previously received SCHEMA message should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-message-order.json')
        with self.assertRaises(Exception):
            self.persist_lines(tap_lines)

    def test_loading_tables_with_no_encryption(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning off client-side encryption and load
        self.config['client_side_encryption_master_key'] = ''
        self.persist_lines(tap_lines)

        self.assert_three_streams_are_into_bigquery()

    def test_loading_tables_with_client_side_encryption(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on client-side encryption and load
        self.config['client_side_encryption_master_key'] = os.environ.get('CLIENT_SIDE_ENCRYPTION_MASTER_KEY')
        self.persist_lines(tap_lines)

        self.assert_three_streams_are_into_bigquery()

    def test_loading_tables_with_metadata_columns(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on adding metadata columns
        self.config['add_metadata_columns'] = True
        self.persist_lines(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_bigquery(should_metadata_columns_exist=True)

    def test_loading_tables_with_defined_parallelism(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Using fixed 1 thread parallelism
        self.config['parallelism'] = 1
        self.persist_lines(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_bigquery()

    def test_loading_tables_with_hard_delete(self):
        """Loading multiple tables from the same input tap with deleted rows"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.persist_lines(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_bigquery(
            should_metadata_columns_exist=True,
            should_hard_deleted_rows=True
        )

    def test_loading_with_multiple_schema(self):
        """Loading table with multiple SCHEMA messages"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-multi-schemas.json')

        # Load with default settings
        self.persist_lines(tap_lines)

        # Check if data loaded correctly
        self.assert_three_streams_are_into_bigquery(
            should_metadata_columns_exist=False,
            should_hard_deleted_rows=False
        )

    def test_loading_unicode_characters(self):
        """Loading unicode encoded characters"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-unicode-characters.json')

        # Load with default settings
        self.persist_lines(tap_lines)

        # Get loaded rows from tables
        bigquery = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_unicode = query(bigquery, "SELECT * FROM {}.test_table_unicode ORDER BY c_int".format(target_schema))

        self.assertEqual(
            table_unicode,
            [
                {'c_int': 1, 'c_pk': 1, 'c_varchar': 'Hello world, Καλημέρα κόσμε, コンニチハ'},
                {'c_int': 2, 'c_pk': 2, 'c_varchar': 'Chinese: 和毛泽东 <<重上井冈山>>. 严永欣, 一九八八年.'},
                {'c_int': 3, 'c_pk': 3,
                 'c_varchar': 'Russian: Зарегистрируйтесь сейчас на Десятую Международную Конференцию по'},
                {'c_int': 4, 'c_pk': 4, 'c_varchar': 'Thai: แผ่นดินฮั่นเสื่อมโทรมแสนสังเวช'},
                {'c_int': 5, 'c_pk': 5, 'c_varchar': 'Arabic: لقد لعبت أنت وأصدقاؤك لمدة وحصلتم علي من إجمالي النقاط'},
                {'c_int': 6, 'c_pk': 6, 'c_varchar': 'Special Characters: [",\'!@£$%^&*()]'}
            ])

    def test_decimal_number_range(self):
        """Loading table with decimals outside BigQuery accepted values"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-bad-decimals.json')

        # Load with default settings
        self.persist_lines(tap_lines)

        # Get loaded rows from tables
        bigquery = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        getcontext().prec = PRECISION
        table_bad_decimals = query(bigquery,
            "SELECT * FROM {}.test_table_bad_decimals ORDER BY c_pk".format(target_schema))

        self.assertEqual(
            table_bad_decimals,
            [
                {'c_pk': 1, 'decimalcolumn': Decimal('99999999999999999999999999999.999999999')},
                {'c_pk': 2, 'decimalcolumn': Decimal('-99999999999999999999999999999.999999999')},
                {'c_pk': 3, 'decimalcolumn': Decimal('3.141592654')},
                {'c_pk': 4, 'decimalcolumn': None},
                {'c_pk': 5, 'decimalcolumn': Decimal('1.000000010')}
            ])

    def test_reserved_word_table_name(self):
        """Loading table with a reserved name"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-reserved-table-name.json')

        # Load with default settings
        self.persist_lines(tap_lines)

        # Get loaded rows from tables
        bigquery = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_non_db_friendly_columns = query(bigquery,
            "SELECT * FROM {}.`full` ORDER BY c_pk".format(target_schema))

        self.assertEqual(
            table_non_db_friendly_columns,
            [
                {'c_pk': 1, 'camelcasecolumn': 'Dummy row 1', 'minus_column': 'Dummy row 1'}
            ])

    def test_non_db_friendly_columns(self):
        """Loading non-db friendly columns like, camelcase, minus signs, etc."""
        tap_lines = test_utils.get_test_tap_lines('messages-with-non-db-friendly-columns.json')

        # Load with default settings
        self.persist_lines(tap_lines)

        # Get loaded rows from tables
        bigquery = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_non_db_friendly_columns = query(bigquery,
            "SELECT * FROM {}.test_table_non_db_friendly_columns ORDER BY c_pk".format(target_schema))

        self.assertEqual(
            table_non_db_friendly_columns,
            [
                {'c_pk': 1, 'camelcasecolumn': 'Dummy row 1', 'minus_column': 'Dummy row 1'},
                {'c_pk': 2, 'camelcasecolumn': 'Dummy row 2', 'minus_column': 'Dummy row 2'},
                {'c_pk': 3, 'camelcasecolumn': 'Dummy row 3', 'minus_column': 'Dummy row 3'},
                {'c_pk': 4, 'camelcasecolumn': 'Dummy row 4', 'minus_column': 'Dummy row 4'},
                {'c_pk': 5, 'camelcasecolumn': 'Dummy row 5', 'minus_column': 'Dummy row 5'},
            ])

    def test_nested_schema_unflattening(self):
        """Loading nested JSON objects with no props without flattening"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-nested-schema.json')

        # Load with default settings - Flattening disabled
        self.persist_lines(tap_lines)

        # Get loaded rows from tables - Transform JSON to string at query time
        bigquery = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        unflattened_table = query(bigquery, """
            SELECT c_pk
                  , c_array c_array
                  , c_object c_object
                  , c_object c_object_with_props
                  , c_nested_object c_nested_object
              FROM {}.test_table_nested_schema
             ORDER BY c_pk""".format(target_schema))

        # Should be valid nested JSON strings
        self.assertEqual(
            unflattened_table,
            [{
                'c_pk': 1,
                'c_array': '[1, 2, 3]',
                'c_object': '{"key_1": "value_1"}',
                'c_object_with_props': '{"key_1": "value_1"}',
                'c_nested_object': {'nested_prop_1': 'nested_value_1',
                                    'nested_prop_2': 'nested_value_2',
                                    'nested_prop_3': {'multi_nested_prop_1': 'multi_value_1',
                                                      'multi_nested_prop_2': 'multi_value_2'}},
            }])

    def test_nested_schema_flattening(self):
        """Loading nested JSON objects with flattening and not not flattening"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-nested-schema.json')

        # Turning on data flattening
        self.config['data_flattening_max_level'] = 10

        # Load with default settings - Flattening disabled
        self.persist_lines(tap_lines)

        # Get loaded rows from tables
        bigquery = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        flattened_table = query(bigquery,
            "SELECT * FROM {}.test_table_nested_schema ORDER BY c_pk".format(target_schema))

        # Should be flattened columns
        self.assertEqual(
            flattened_table,
            [{
                'c_pk': 1,
                'c_array': '[1, 2, 3]',
                'c_object': None,
                'c_object_with_props__key_1': 'value_1',
                'c_nested_object__nested_prop_1': 'nested_value_1',
                'c_nested_object__nested_prop_2': 'nested_value_2',
                'c_nested_object__nested_prop_3__multi_nested_prop_1': 'multi_value_1',
                'c_nested_object__nested_prop_3__multi_nested_prop_2': 'multi_value_2',
            }])

    def test_repeated_records(self):
        """Loading arrays of JSON objects."""
        tap_lines = test_utils.get_test_tap_lines('messages-with-repeated-records.json')
        tap_lines_modified = test_utils.get_test_tap_lines('messages-with-repeated-records-modified.json')

        # Load with default settings
        self.persist_lines(tap_lines)
        self.persist_lines(tap_lines_modified)

        # Get loaded rows from tables
        bigquery = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        flattened_table = query(
            bigquery,
            "SELECT * FROM {}.test_table_repeated_records ORDER BY c_pk".format(
                target_schema,
            )

        )

        # Structured objects should be handled as dictionaries,
        # unstructured objects should be handled as strings.
        self.assertEqual(
            flattened_table,
            [
                {
                    'c_pk': 1,
                    'c_array_integers': [1, 2, 3],
                    'c_array_integers__st': [],
                    'c_array_objects': [
                        {'nested': 1},
                        {'nested': 2},
                    ],
                    'c_array_objects_no_props': [
                        '{"nested": 1}',
                        '{"nested": 2}',
                    ],
                },
                {
                    'c_pk': 2,
                    'c_array_integers': [],
                    'c_array_integers__st': ["1", "2", "3"],
                    'c_array_objects': [
                        {'nested': 1},
                        {'nested': 2},
                    ],
                    'c_array_objects_no_props': [
                        '{"nested": 1}',
                        '{"nested": 2}',
                    ],
                },
            ]
        )

    def test_column_name_change(self):
        """Tests correct renaming of bigquery columns after source change"""
        tap_lines_before_column_name_change = test_utils.get_test_tap_lines('messages-with-three-streams.json')
        tap_lines_after_column_name_change = test_utils.get_test_tap_lines(
            'messages-with-three-streams-modified-column.json')

        # Load with default settings
        self.persist_lines(tap_lines_before_column_name_change)
        self.persist_lines(tap_lines_after_column_name_change)

        # Get loaded rows from tables
        bigquery = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = query(bigquery, "SELECT * FROM {}.test_table_one ORDER BY c_pk".format(target_schema))
        table_two = query(bigquery, "SELECT * FROM {}.test_table_two ORDER BY c_pk".format(target_schema))
        table_three = query(bigquery, "SELECT * FROM {}.test_table_three ORDER BY c_pk".format(target_schema))

        # Table one should have no changes
        self.assertEqual(
            table_one,
            [{'c_int': 1, 'c_pk': 1, 'c_varchar': '1'}])

        # Table two should have versioned column
        self.assertEquals(
            table_two,
            [
                {'c_int': 1, 'c_pk': 1,
                 'c_varchar': '1', 'c_date': datetime.datetime(2019, 2, 1, 15, 12, 45, tzinfo=timezone.utc), 'c_date__st': None},
                {'c_int': 2, 'c_pk': 2, 'c_varchar': '2',
                 'c_date': datetime.datetime(2019, 2, 10, 2, tzinfo=timezone.utc), 'c_date__st': '2019-02-12 02:00:00'},
                {'c_int': 3, 'c_pk': 3, 'c_varchar': '2', 'c_date': None, 'c_date__st': '2019-02-15 02:00:00'}
            ]
        )

        # Table three should have renamed columns
        self.assertEqual(
            table_three,
            [
                {'c_int': 1, 'c_pk': 1, 'c_time': datetime.time(4, 0), 'c_varchar': '1', 'c_time_renamed': None},
                {'c_int': 2, 'c_pk': 2, 'c_time': datetime.time(7, 15), 'c_varchar': '2', 'c_time_renamed': None},
                {'c_int': 3, 'c_pk': 3, 'c_time': datetime.time(23, 0, 3), 'c_varchar': '3',
                 'c_time_renamed': datetime.time(8, 15)},
                {'c_int': 4, 'c_pk': 4, 'c_time': None, 'c_varchar': '4', 'c_time_renamed': datetime.time(23, 0, 3)}
            ])

    def test_logical_streams_from_pg_with_hard_delete_and_default_batch_size_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.persist_lines(tap_lines)

        self.assert_logical_streams_are_in_bigquery(True)

    def test_logical_streams_from_pg_with_hard_delete_mapping(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.config['hard_delete_mapping'] = {
            'logical1-logical1_table2': False,
        }
        self.persist_lines(tap_lines)

        # Get loaded rows from tables
        bigquery = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = query(bigquery, "SELECT * FROM {}.logical1_table1 ORDER BY cid".format(target_schema))
        table_two = query(bigquery, "SELECT * FROM {}.logical1_table2 ORDER BY cid".format(target_schema))
        table_three = query(bigquery, "SELECT * FROM {}.logical2_table1 ORDER BY cid".format(target_schema))
        table_four = query(bigquery, "SELECT cid, ctimentz, ctimetz FROM {}.logical1_edgydata WHERE cid IN(1,2,3,4,5,6,8,9) ORDER BY cid".format(target_schema))

        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'cid': 1, 'cvarchar': "inserted row", 'cvarchar2': None},
            {'cid': 2, 'cvarchar': 'inserted row', "cvarchar2": "inserted row"},
            {'cid': 3, 'cvarchar': "inserted row", 'cvarchar2': "inserted row"},
            {'cid': 4, 'cvarchar': "inserted row", 'cvarchar2': "inserted row"}
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_two
        # ----------------------------------------------------------------------
        delete_time = datetime.datetime(2019, 10, 13, 14, 6, 31, 838328, tzinfo=timezone.utc)
        expected_table_two = [
            {'cid': 1, 'cvarchar': "updated row", "_sdc_deleted_at": None},
            {'cid': 2, 'cvarchar': 'updated row', "_sdc_deleted_at": None},
            {'cid': 3, 'cvarchar': "updated row", "_sdc_deleted_at": None},
            {'cid': 4, 'cvarchar': None, "_sdc_deleted_at": delete_time},
            {'cid': 5, 'cvarchar': "updated row", "_sdc_deleted_at": None},
            {'cid': 6, 'cvarchar': None, "_sdc_deleted_at": delete_time},
            {'cid': 7, 'cvarchar': "updated row", "_sdc_deleted_at": None},
            {'cid': 8, 'cvarchar': "updated row", "_sdc_deleted_at": None},
            {'cid': 9, 'cvarchar': "updated row", "_sdc_deleted_at": None},
            {'cid': 10, 'cvarchar': 'updated row', "_sdc_deleted_at": None},
            {"cid": 11, "cvarchar": None, "_sdc_deleted_at": delete_time},
            {"cid": 12, "cvarchar": None, "_sdc_deleted_at": delete_time},
            {"cid": 13, "cvarchar": None, "_sdc_deleted_at": delete_time},
            {"cid": 14, "cvarchar": None, "_sdc_deleted_at": delete_time},
            {"cid": 15, "cvarchar": None, "_sdc_deleted_at": delete_time},
            {"cid": 16, "cvarchar": None, "_sdc_deleted_at": delete_time},
            {"cid": 17, "cvarchar": None, "_sdc_deleted_at": delete_time},
            {"cid": 18, "cvarchar": None, "_sdc_deleted_at": delete_time},
            {"cid": 19, "cvarchar": None, "_sdc_deleted_at": delete_time},
            {"cid": 20, "cvarchar": None, "_sdc_deleted_at": delete_time},
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_three
        # ----------------------------------------------------------------------
        expected_table_three = [
            {'cid': 1, 'cvarchar': "updated row"},
            {'cid': 2, 'cvarchar': 'updated row'},
            {'cid': 3, 'cvarchar': "updated row"},
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_four
        # ----------------------------------------------------------------------
        expected_table_four = [
            {'cid': 1, 'ctimentz': None, 'ctimetz': None},
            {'cid': 2, 'ctimentz': datetime.time(23, 0, 15), 'ctimetz': datetime.time(23, 0, 15)},
            {'cid': 3, 'ctimentz': datetime.time(12, 0, 15), 'ctimetz': datetime.time(12, 0, 15)},
            {'cid': 4, 'ctimentz': datetime.time(12, 0, 15), 'ctimetz': datetime.time(9, 0, 15)},
            {'cid': 5, 'ctimentz': datetime.time(12, 0, 15), 'ctimetz': datetime.time(15, 0, 15)},
            {'cid': 6, 'ctimentz': datetime.time(0, 0), 'ctimetz': datetime.time(0, 0)},
            {'cid': 8, 'ctimentz': datetime.time(0, 0), 'ctimetz': datetime.time(1, 0)},
            {'cid': 9, 'ctimentz': datetime.time(0, 0), 'ctimetz': datetime.time(0, 0)}
        ]

        self.assertEqual(self.remove_metadata_columns_from_rows(table_one), expected_table_one)
        self.assertEqual(table_two, expected_table_two)
        self.assertEqual(self.remove_metadata_columns_from_rows(table_three), expected_table_three)
        self.assertEqual(table_four, expected_table_four)

    def test_logical_streams_from_pg_with_hard_delete_and_batch_size_of_5_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 5
        self.persist_lines(tap_lines)

        self.assert_logical_streams_are_in_bigquery(True)

    def test_logical_streams_from_pg_with_hard_delete_and_batch_size_of_5_and_no_records_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams-no-records.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 5
        self.persist_lines(tap_lines)

        self.assert_logical_streams_are_in_bigquery_and_are_empty()

    @mock.patch('target_bigquery.emit_state')
    def test_flush_streams_with_no_intermediate_flushes(self, mock_emit_state):
        """Test emitting states when no intermediate flush required"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size big enough to never has to flush in the middle
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 1000
        self.persist_lines(tap_lines)

        # State should be emitted only once with the latest received STATE message
        self.assertEquals(
            mock_emit_state.mock_calls,
            [
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}})
            ])

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_bigquery(True)

    @mock.patch('target_bigquery.emit_state')
    def test_flush_streams_with_intermediate_flushes(self, mock_emit_state):
        """Test emitting states when intermediate flushes required"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size small enough to trigger multiple stream flushes
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 10
        self.persist_lines(tap_lines)

        # State should be emitted multiple times, updating the positions only in the stream which got flushed
        self.assertEquals(
            mock_emit_state.call_args_list,
            [
                # Flush #1 - Flushed edgydata until lsn: 108197216
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #2 - Flushed logical1-logical1_table2 until lsn: 108201336
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #3 - Flushed logical1-logical1_table2 until lsn: 108237600
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #4 - Flushed logical1-logical1_table2 until lsn: 108238768
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #5 - Flushed logical1-logical1_table2 until lsn: 108239704,
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108196176, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
                # Flush #6 - Last flush, update every stream lsn: 108240872,
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723618, "xmin": None},
                     "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723635, "xmin": None},
                     "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723651, "xmin": None},
                     "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                     "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                     "public2-wearehere": {}}}),
            ])

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_bigquery(True)

    @mock.patch('target_bigquery.emit_state')
    def test_flush_streams_with_intermediate_flushes_on_all_streams(self, mock_emit_state):
        """Test emitting states when intermediate flushes required and flush_all_streams is enabled"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size small enough to trigger multiple stream flushes
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 10
        self.config['flush_all_streams'] = True
        self.persist_lines(tap_lines)

        # State should be emitted 6 times, flushing every stream and updating every stream position
        self.assertEquals(
            mock_emit_state.call_args_list,
            [
                # Flush #1 - Flush every stream until lsn: 108197216
                mock.call({"currently_syncing": None, "bookmarks": {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108197216, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #2 - Flush every stream until lsn 108201336
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108201336, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #3 - Flush every stream until lsn: 108237600
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108237600, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #4 - Flush every stream until lsn: 108238768
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108238768, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #5 - Flush every stream until lsn: 108239704,
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108239896, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
                # Flush #6 - Last flush, update every stream until lsn: 108240872,
                mock.call({'currently_syncing': None, 'bookmarks': {
                    "logical1-logical1_edgydata": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723596, "xmin": None},
                    "logical1-logical1_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723618, "xmin": None},
                    "logical1-logical1_table2": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723635, "xmin": None},
                    "logical2-logical2_table1": {"last_replication_method": "LOG_BASED", "lsn": 108240872, "version": 1570922723651, "xmin": None},
                    "public-city": {"last_replication_method": "INCREMENTAL", "replication_key": "id", "version": 1570922723667, "replication_key_value": 4079},
                    "public-country": {"last_replication_method": "FULL_TABLE", "version": 1570922730456, "xmin": None},
                    "public2-wearehere": {}}}),
            ])

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_bigquery(True)

    @mock.patch('target_bigquery.emit_state')
    def test_flush_streams_based_on_batch_wait_limit(self, mock_emit_state):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        mock_emit_state.get.return_value = None

        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 1000
        self.config['batch_wait_limit_seconds'] = 0.1
        self.persist_lines(tap_lines)

        self.assert_logical_streams_are_in_bigquery(True)
        self.assertGreater(mock_emit_state.call_count, 1, 'Expecting multiple flushes')
