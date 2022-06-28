#!/usr/bin/env python3

import argparse
from datetime import datetime, timedelta
import copy
import io
import json
import logging
import os
import sys
from multiprocessing.pool import ThreadPool as Pool

from tempfile import mkstemp
from fastavro import writer, parse_schema
from jsonschema import Draft7Validator, FormatChecker
from singer import get_logger

from target_bigquery import stream_utils
from target_bigquery.db_sync import DbSync
from target_bigquery.exceptions import (
    RecordValidationException,
    InvalidValidationOperationException
)

LOGGER = get_logger('target_bigquery')
logging.getLogger('bigquery.connector').setLevel(logging.WARNING)

DEFAULT_BATCH_SIZE_ROWS = 100000
DEFAULT_PARALLELISM = 0  # 0 The number of threads used to flush tables
DEFAULT_MAX_PARALLELISM = 16  # Don't use more than this number of threads by default when flushing streams in parallel
DEFAULT_HARD_DELETE = False



def add_metadata_columns_to_schema(schema_message):
    """Metadata _sdc columns according to the stitch documentation at
    https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns

    Metadata columns gives information about data injections
    """
    extended_schema_message = schema_message
    extended_schema_message['schema']['properties']['_sdc_extracted_at'] = {'type': ['null', 'string'],
                                                                            'format': 'date-time'}
    extended_schema_message['schema']['properties']['_sdc_batched_at'] = {'type': ['null', 'string'],
                                                                          'format': 'date-time'}
    extended_schema_message['schema']['properties']['_sdc_deleted_at'] = {'type': ['null', 'string'],
                                                                          'format': 'date-time'}
    extended_schema_message['schema']['properties']['_sdc_table_version'] = {'type': ['null', 'integer']}

    return extended_schema_message


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        LOGGER.info('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


# pylint: disable=too-many-locals,too-many-branches,too-many-statements
def persist_lines(config, lines) -> None:
    state = None
    flushed_state = None
    schemas = {}
    key_properties = {}
    validators = {}
    records_to_load = {}
    row_count = {}
    flush_timestamp = {}
    stream_to_sync = {}
    total_row_count = {}
    batch_size_rows = config.get('batch_size_rows', DEFAULT_BATCH_SIZE_ROWS)
    default_hard_delete = config.get('hard_delete', DEFAULT_HARD_DELETE)
    hard_delete_mapping = config.get('hard_delete_mapping', {})
    batch_wait_limit_seconds = config.get('batch_wait_limit_seconds', None)

    # Loop over lines from stdin
    for line in lines:
        # Check to see if any streams should be flushed based on time.
        # This assumes that each individual record takes a negligible period
        # of time to be processed.
        streams_to_flush_timestamp = set()
        if batch_wait_limit_seconds:
            streams_to_flush_timestamp = {
                stream for stream, timestamp in flush_timestamp.items()
                if datetime.utcnow() >= timestamp + timedelta(seconds=batch_wait_limit_seconds)
            }

        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            LOGGER.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))

        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            # Get schema for this record's stream
            stream = o['stream']

            stream_utils.adjust_timestamps_in_record(o['record'], schemas[stream])

            # Validate record
            if config.get('validate_records'):
                try:
                    validators[stream].validate(stream_utils.float_to_decimal(o['record']))
                except Exception as ex:
                    if type(ex).__name__ == "InvalidOperation":
                        raise InvalidValidationOperationException(
                            f"Data validation failed and cannot load to destination. RECORD: {o['record']}\n"
                            "multipleOf validations that allows long precisions are not supported (i.e. with 15 digits"
                            "or more) Try removing 'multipleOf' methods from JSON schema.")
                    raise RecordValidationException(f"Record does not pass schema validation. RECORD: {o['record']}")

            primary_key_string = stream_to_sync[stream].record_primary_key_string(o['record'])
            if not primary_key_string:
                primary_key_string = 'RID-{}'.format(total_row_count[stream])

            # increment row count only when a new PK is encountered in the current batch
            if primary_key_string not in records_to_load[stream]:
                row_count[stream] += 1
                total_row_count[stream] += 1

            # append record
            if config.get('add_metadata_columns') or hard_delete_mapping.get(stream, default_hard_delete):
                records_to_load[stream][primary_key_string] = stream_utils.add_metadata_values_to_record(o)
            else:
                records_to_load[stream][primary_key_string] = o['record']

            flush = False
            if row_count[stream] >= batch_size_rows:
                flush = True
                LOGGER.info("Flush triggered by batch_size_rows (%s) reached in %s",
                             batch_size_rows, stream)
            elif streams_to_flush_timestamp:
                flush = True
                LOGGER.info("Flush triggered by batch_wait_limit_seconds (%s)",
                            batch_wait_limit_seconds)

            if flush:
                # flush all streams, delete records if needed, reset counts and then emit current state
                if config.get('flush_all_streams'):
                    filter_streams = None
                else:
                    filter_streams = list(streams_to_flush_timestamp | {stream})

                # Flush and return a new state dict with new positions only for the flushed streams
                flushed_state, flushed_timestamps = flush_streams(
                    records_to_load,
                    row_count,
                    stream_to_sync,
                    config,
                    state,
                    flushed_state,
                    filter_streams=filter_streams)

                flush_timestamp.update(flushed_timestamps)

                # emit last encountered state
                emit_state(copy.deepcopy(flushed_state))

        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))

            stream = o['stream']

            schemas[stream] = stream_utils.float_to_decimal(o['schema'])
            validators[stream] = Draft7Validator(schemas[stream], format_checker=FormatChecker())

            # flush records from previous stream SCHEMA
            # if same stream has been encountered again, it means the schema might have been altered
            # so previous records need to be flushed
            if row_count.get(stream, 0) > 0:
                if config.get('flush_all_streams'):
                    filter_streams = None
                else:
                    filter_streams = list(streams_to_flush_timestamp | {stream})

                flushed_state, flushed_timestamps = flush_streams(
                    records_to_load, row_count, stream_to_sync, config, state, flushed_state, filter_streams=filter_streams
                )

                flush_timestamp.update(flushed_timestamps)

                # emit latest encountered state
                emit_state(flushed_state)

            # key_properties key must be available in the SCHEMA message.
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")

            # Log based and Incremental replications on tables with no Primary Key
            # cause duplicates when merging UPDATE events.
            # Stop loading data by default if no Primary Key.
            #
            # If you want to load tables with no Primary Key:
            #  1) Set ` 'primary_key_required': false ` in the target-bigquery config.json
            #  or
            #  2) Use fastsync [postgres-to-bigquery, mysql-to-bigquery, etc.]
            if config.get('primary_key_required', True) and len(o['key_properties']) == 0:
                LOGGER.critical("Primary key is set to mandatory but not defined in the [{}] stream".format(stream))
                raise Exception("key_properties field is required")

            key_properties[stream] = o['key_properties']

            if config.get('add_metadata_columns') or hard_delete_mapping.get(stream, default_hard_delete):
                stream_to_sync[stream] = DbSync(config, add_metadata_columns_to_schema(o))
            else:
                stream_to_sync[stream] = DbSync(config, o)

            try:
                stream_to_sync[stream].create_schema_if_not_exists()
                stream_to_sync[stream].sync_table()
            except Exception as e:
                LOGGER.error("""
                    Cannot sync table structure in BigQuery schema: {} .
                """.format(
                    stream_to_sync[stream].schema_name))
                raise e

            records_to_load[stream] = {}
            row_count[stream] = 0
            total_row_count[stream] = 0
            flush_timestamp[stream] = datetime.utcnow()

        elif t == 'ACTIVATE_VERSION':
            stream = o['stream']
            version = o['version']

            if hard_delete_mapping.get(stream, default_hard_delete):
                if stream in stream_to_sync:
                    LOGGER.debug('ACTIVATE_VERSION message, clearing records with versions other than {}'.format(version))
                    stream_to_sync[stream].activate_table_version(stream, version)
                else:
                    LOGGER.warn('ACTIVATE_VERSION message, unknown stream {}'.format(stream))
            else:
                LOGGER.debug('ACTIVATE_VERSION message - ignoring due hard_delete not set')

        elif t == 'STATE':
            LOGGER.debug('Setting state to {}'.format(o['value']))
            state = o['value']

            # Initially set flushed state
            if not flushed_state:
                flushed_state = copy.deepcopy(state)

        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

    # if some bucket has records that need to be flushed but haven't reached batch size
    # then flush all buckets.
    if sum(row_count.values()) > 0:
        # flush all streams one last time, delete records if needed, reset counts and then emit current state
        flushed_state, _ = flush_streams(records_to_load, row_count, stream_to_sync, config, state, flushed_state)

    # emit latest state
    emit_state(copy.deepcopy(flushed_state))


# pylint: disable=too-many-arguments
def flush_streams(
        streams,
        row_count,
        stream_to_sync,
        config,
        state,
        flushed_state,
        filter_streams=None):
    """
    Flushes all buckets and resets records count to 0 as well as empties records to load list
    :param streams: dictionary with records to load per stream
    :param row_count: dictionary with row count per stream
    :param stream_to_sync: BigQuery db sync instance per stream
    :param config: dictionary containing the configuration
    :param state: dictionary containing the original state from tap
    :param flushed_state: dictionary containing updated states only when streams got flushed
    :param filter_streams: Keys of streams to flush from the streams dict. Default is every stream
    :return: State dict with flushed positions
    :return: Dictionary with flush timestamps for each stream flushed
    """
    parallelism = config.get("parallelism", DEFAULT_PARALLELISM)
    max_parallelism = config.get("max_parallelism", DEFAULT_MAX_PARALLELISM)
    default_hard_delete = config.get("hard_delete", DEFAULT_HARD_DELETE)
    hard_delete_mapping = config.get("hard_delete_mapping", {})

    # Parallelism 0 means auto parallelism:
    #
    # Auto parallelism trying to flush streams efficiently with auto defined number
    # of threads where the number of threads is the number of streams that need to
    # be loaded but it's not greater than the value of max_parallelism
    if parallelism == 0:
        n_streams_to_flush = len(streams.keys())
        if n_streams_to_flush > max_parallelism:
            parallelism = max_parallelism
        else:
            parallelism = n_streams_to_flush

    # Select the required streams to flush
    if filter_streams:
        streams_to_flush = filter_streams
    else:
        streams_to_flush = list(streams.keys())

    if len(streams_to_flush) > 1:
        # Single-host, process-based parallelism to avoid the dreaded GIL.
        with Pool(parallelism) as pool:
            jobs = []
            for stream in streams_to_flush:
                jobs.append(
                    pool.apply_async(
                        load_stream_batch,
                        kwds={
                            'stream': stream,
                            'records_to_load': streams[stream],
                            'row_count': row_count,
                            'db_sync': stream_to_sync[stream],
                            'delete_rows': hard_delete_mapping.get(
                                stream, default_hard_delete
                            ),
                        },
                    )
                )
            for future in jobs:
                future.get()
    else:
        # If we only have one stream to sync let's not introduce overhead.
        # for stream in streams_to_flush:
        load_stream_batch(
            stream=streams_to_flush[0],
            records_to_load=streams[streams_to_flush[0]],
            row_count=row_count,
            db_sync=stream_to_sync[streams_to_flush[0]],
            delete_rows=hard_delete_mapping.get(streams_to_flush[0], default_hard_delete)
        )

    # reset flushed stream records to empty to avoid flushing same records
    # reset row count for flushed streams
    for stream in streams_to_flush:
        streams[stream] = {}
        row_count[stream] = 0

        # Update flushed streams
        if filter_streams:
            # update flushed_state position if we have state information for the stream
            if state is not None and stream in state.get('bookmarks', {}):
                # Create bookmark key if not exists
                if 'bookmarks' not in flushed_state:
                    flushed_state['bookmarks'] = {}
                # Copy the stream bookmark from the latest state
                flushed_state['bookmarks'][stream] = copy.deepcopy(state['bookmarks'][stream])

        # If we flush every bucket use the latest state
        else:
            flushed_state = copy.deepcopy(state)

    flushed_timestamps = {stream: datetime.utcnow() for stream in streams_to_flush}
    # Return with state message with flushed positions
    return flushed_state, flushed_timestamps


def load_stream_batch(stream, records_to_load, row_count, db_sync, delete_rows=False):
    # Load into bigquery
    if row_count[stream] > 0:
        flush_records(stream, records_to_load, row_count[stream], db_sync)

        # Delete soft-deleted, flagged rows - where _sdc_deleted at is not null
        if delete_rows:
            db_sync.delete_rows(stream)


def flush_records(stream, records_to_load, row_count, db_sync):
    parsed_schema = parse_schema(db_sync.avro_schema())
    csv_fd, csv_file = mkstemp()
    with open(csv_file, 'wb') as out:
        writer(out, parsed_schema, db_sync.records_to_avro(records_to_load.values()))

    # Seek to the beginning of the file and load
    with open(csv_file, 'r+b') as f:
        db_sync.load_avro(f, row_count)

    # Delete temp file
    os.remove(csv_file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as config_input:
            config = json.load(config_input)
    else:
        config = {}

    # Consume singer messages
    singer_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    persist_lines(config, singer_messages)

    LOGGER.debug("Exiting normally")


if __name__ == '__main__':
    main()
