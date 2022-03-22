import unittest
from datetime import datetime

from target_bigquery import stream_utils

class TestStreamUtils(unittest.TestCase):
    """
    Unit Tests
    """

    def test_add_metadata_values_to_record(self):
        """Test adding metadata"""

        dt = "2017-11-20T16:45:33.000Z"
        record = { "type": "RECORD", "stream": "foo", "time_extracted": dt, "record": {"id": "2"} }
        result = stream_utils.add_metadata_values_to_record(record)

        self.assertEqual(result.get("id"), "2")
        self.assertEqual(result.get("_sdc_extracted_at"), datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%fZ'))

        extra_attrs = ['_sdc_batched_at', '_sdc_deleted_at']
        for attr in extra_attrs:
            self.assertTrue(attr in result)

    def test_add_metadata_values_to_record_when_no_time_extracted(self):
        """Test adding metadata when there's no time extracted in the record message """

        record = { "type": "RECORD", "stream": "foo", "record": {"id": "2"} }

        result = stream_utils.add_metadata_values_to_record(record)
        self.assertEqual(result.get("id"), "2")

        extra_attrs = ['_sdc_extracted_at', '_sdc_batched_at', '_sdc_deleted_at']
        for attr in extra_attrs:
            self.assertTrue(attr in result)
