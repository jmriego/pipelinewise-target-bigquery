import os
import json
import unittest

import target_bigquery
from target_bigquery.db_sync import DbSync

METADATA_COLUMNS = [
    '_sdc_extracted_at',
    '_sdc_batched_at',
    '_sdc_deleted_at'
]


def get_db_config():
    config = {}

    # --------------------------------------------------------------------------
    # Default configuration settings for integration tests.
    # --------------------------------------------------------------------------
    # The following values needs to be defined in environment variables with
    # valid details to a Snowflake instace, AWS IAM role and an S3 bucket
    # --------------------------------------------------------------------------
    # Snowflake instance
    config['project_id'] = os.environ.get('TARGET_BIGQUERY_PROJECT')
    config['default_target_schema'] = os.environ.get("TARGET_BIGQUERY_SCHEMA")

    # --------------------------------------------------------------------------
    # The following variables needs to be empty.
    # The tests cases will set them automatically whenever it's needed
    # --------------------------------------------------------------------------
    config['schema_mapping'] = None
    config['add_metadata_columns'] = None
    config['hard_delete'] = None
    config['flush_all_streams'] = None

    return config


def get_test_config():
    db_config = get_db_config()

    return db_config


def get_test_tap_lines(filename):
    lines = []
    with open('{}/resources/{}'.format(os.path.dirname(__file__), filename)) as tap_stdout:
        for line in tap_stdout.readlines():
            lines.append(line)

    return lines

class TestIntegration(unittest.TestCase):
    """
    Integration Tests
    """
    maxDiff = None

    def setUp(self):
        self.config = get_test_config()
        bigquery = DbSync(self.config)

        # Drop target schema
        if self.config['default_target_schema']:
            bigquery.client.delete_dataset(
                self.config['default_target_schema'],
                delete_contents=True,
                not_found_ok=True)

    def persist_lines(self, lines):
        """Loads singer messages into bigquery.
        """
        target_bigquery.persist_lines(self.config, lines)

    def remove_metadata_columns_from_rows(self, rows):
        """Removes metadata columns from a list of rows"""
        d_rows = []
        for r in rows:
            # Copy the original row to a new dict to keep the original dict
            # and remove metadata columns
            d_row = r.copy()
            for md_c in METADATA_COLUMNS:
                d_row.pop(md_c, None)

            # Add new row without metadata columns to the new list
            d_rows.append(d_row)

        return d_rows

    def assert_metadata_columns_exist(self, rows):
        """This is a helper assertion that checks if every row in a list has metadata columns"""
        for r in rows:
            for md_c in METADATA_COLUMNS:
                self.assertTrue(md_c in r)

    def assert_metadata_columns_not_exist(self, rows):
        """This is a helper assertion that checks metadata columns don't exist in any row"""
        for r in rows:
            for md_c in METADATA_COLUMNS:
                self.assertFalse(md_c in r)

