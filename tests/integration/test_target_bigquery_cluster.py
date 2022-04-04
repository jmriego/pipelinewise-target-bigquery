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
    Integration Tests about clustering
    """
    def test_table_with_pk_adds_clustering(self):
        """Tests table with a primary key gets clustered on those fields"""
        tap_lines = test_utils.get_test_tap_lines('table_with_pk_cluster.json')
        self.persist_lines(tap_lines)

        # Get loaded rows from tables
        bigquery = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table = query(bigquery, "SELECT * FROM {}.test_table_cluster ORDER BY c_pk".format(target_schema))
        cluster_columns = query(bigquery, "SELECT clustering_ordinal_position, column_name FROM {}.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'test_table_cluster' AND clustering_ordinal_position > 0 ORDER BY 1".format(target_schema))

        # ----------------------------------------------------------------------
        # Check rows in table
        # ----------------------------------------------------------------------
        expected_table = [
            {'c_pk': 2, 'c_int': 2, 'c_varchar': '2', 'c_date': datetime.datetime(2019, 2, 12, 2, 0, 0, tzinfo=timezone.utc)},
            {'c_pk': 3, 'c_int': 3, 'c_varchar': '2', 'c_date': datetime.datetime(2019, 2, 15, 2, 0, 0, tzinfo=timezone.utc)}
        ]

        expected_cluster_columns = [
            {'clustering_ordinal_position': 1, 'column_name': 'c_pk'},
        ]

        self.assertEqual(self.remove_metadata_columns_from_rows(table), expected_table)
        self.assertEqual(cluster_columns, expected_cluster_columns)

        # ----------------------------------------------------------------------
        # Change the primary key and check if clustering stayed unchanged
        # ----------------------------------------------------------------------
        tap_lines = test_utils.get_test_tap_lines('table_with_pk_cluster_changed.json')
        self.persist_lines(tap_lines)

        table_changed = query(bigquery, "SELECT * FROM {}.test_table_cluster ORDER BY c_pk".format(target_schema))
        cluster_columns_changed = query(bigquery, "SELECT clustering_ordinal_position, column_name FROM {}.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'test_table_cluster' AND clustering_ordinal_position > 0 ORDER BY 1".format(target_schema))

        expected_table_changed = [
            {'c_pk': 2, 'c_int': 2, 'c_varchar': 'c', 'c_date': datetime.datetime(2019, 2, 12, 2, 0, 0, tzinfo=timezone.utc)},
            {'c_pk': 3, 'c_int': 3, 'c_varchar': 'c', 'c_date': datetime.datetime(2022, 5, 15, 5, 0, 0, tzinfo=timezone.utc)}
        ]

        self.assertEqual(self.remove_metadata_columns_from_rows(table_changed), expected_table_changed)
        self.assertEqual(cluster_columns_changed, expected_cluster_columns)

    def test_table_with_pk_multi_column_removed(self):
        """Test table with a pk with multiple columns gets clustered by those and removing the pk doesnt cause errors"""
        tap_lines = test_utils.get_test_tap_lines('table_with_multi_pk_cluster.json')
        self.persist_lines(tap_lines)

        # Get loaded rows from tables
        bigquery = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table = query(bigquery, "SELECT * FROM {}.test_table_cluster_multi ORDER BY c_pk".format(target_schema))
        cluster_columns = query(bigquery, "SELECT clustering_ordinal_position, column_name FROM {}.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'test_table_cluster_multi' AND clustering_ordinal_position > 0 ORDER BY 1".format(target_schema))

        # ----------------------------------------------------------------------
        # Check rows in table
        # ----------------------------------------------------------------------
        expected_table = [
            {'c_pk': 2, 'c_int': 2, 'c_varchar': '2', 'c_date': datetime.datetime(2019, 2, 12, 2, 0, 0, tzinfo=timezone.utc)},
            {'c_pk': 3, 'c_int': 3, 'c_varchar': '2', 'c_date': datetime.datetime(2019, 2, 15, 2, 0, 0, tzinfo=timezone.utc)}
        ]

        expected_cluster_columns = [
            {'clustering_ordinal_position': 1, 'column_name': 'c_pk'},
            {'clustering_ordinal_position': 2, 'column_name': 'c_varchar'}
        ]

        self.assertEqual(self.remove_metadata_columns_from_rows(table), expected_table)
        self.assertEqual(cluster_columns, expected_cluster_columns)

        # ----------------------------------------------------------------------
        # Remove the primary key and check if clustering stayed unchanged
        # ----------------------------------------------------------------------
        self.config['primary_key_required'] = False
        tap_lines = test_utils.get_test_tap_lines('table_with_multi_pk_cluster_changed.json')
        self.persist_lines(tap_lines)

        table_changed = query(bigquery, "SELECT * FROM {}.test_table_cluster_multi ORDER BY c_pk".format(target_schema))
        cluster_columns_changed = query(bigquery, "SELECT clustering_ordinal_position, column_name FROM {}.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'test_table_cluster_multi' AND clustering_ordinal_position > 0 ORDER BY 1".format(target_schema))

        expected_table_changed = [
            {'c_pk': 2, 'c_int': 2, 'c_varchar': '2', 'c_date': datetime.datetime(2019, 2, 12, 2, 0, 0, tzinfo=timezone.utc)},
            {'c_pk': 2, 'c_int': 2, 'c_varchar': '2', 'c_date': datetime.datetime(2019, 2, 12, 2, 0, 0, tzinfo=timezone.utc)},
            {'c_pk': 3, 'c_int': 3, 'c_varchar': '2', 'c_date': datetime.datetime(2019, 2, 15, 2, 0, 0, tzinfo=timezone.utc)}
        ]

        expected_cluster_columns_changed = []

        self.assertEqual(self.remove_metadata_columns_from_rows(table_changed), expected_table_changed)
        self.assertEqual(cluster_columns_changed, expected_cluster_columns)
