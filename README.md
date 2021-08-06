# pipelinewise-target-bigquery

[![PyPI version](https://badge.fury.io/py/pipelinewise-target-bigquery.svg)](https://badge.fury.io/py/pipelinewise-target-bigquery)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-target-bigquery.svg)](https://pypi.org/project/pipelinewise-target-bigquery/)
[![License: Apache2](https://img.shields.io/badge/License-Apache2-yellow.svg)](https://opensource.org/licenses/Apache-2.0)

[Singer](https://www.singer.io/) target that loads data into BigQuery following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible target connector.

## How to use it

The recommended method of running this target is to use it from [PipelineWise](https://transferwise.github.io/pipelinewise). When running it from PipelineWise you don't need to configure this tap with JSON files and most of things are automated. Please check the related documentation at [Target BigQuery](https://transferwise.github.io/pipelinewise/connectors/targets/bigquery.html)

If you want to run this [Singer Target](https://singer.io) independently please read further.

## Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
make venv
```

### To run

Like any other target that's following the singer specification:

`some-singer-tap | target-bigquery --config [config.json]`

It's reading incoming messages from STDIN and using the properties in `config.json` to upload data into BigQuery.

**Note**: To avoid version conflicts run `tap` and `targets` in separate virtual environments.

### Configuration settings

Running the the target connector requires a `config.json` file. An example with the minimal settings:

   ```json
   {
     "dataset_id": "source",
     "project_id": "mygbqproject"
   }
   ```

Full list of options in `config.json`:

| Property                                | Type      | Required?    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| -------------------------------------   | --------- | ------------ | ---------------------------------------------------------------                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| dataset_id                              | String    | Yes          | BigQuery dataset                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| project_id                              | String    | Yes          | BigQuery project                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| location                                | String    |              | Region where BigQuery stores your dataset                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| batch_size_rows                         | Integer   |              | (Default: 100000) Maximum number of rows in each batch. At the end of each batch, the rows in the batch are loaded into BigQuery.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| flush_all_streams                       | Boolean   |              | (Default: False) Flush and load every stream into BigQuery when one batch is full. Warning: This may trigger transfer of data with low number of records, and may cause performance problems.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| parallelism                             | Integer   |              | (Default: 0) The number of threads used to flush tables. 0 will create a thread for each stream, up to parallelism_max. -1 will create a thread for each CPU core. Any other positive number will create that number of threads, up to parallelism_max.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| max_parallelism                         | Integer   |              | (Default: 16) Max number of parallel threads to use when flushing tables.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| default_target_schema                   | String    |              | Name of the schema where the tables will be created. If `schema_mapping` is not defined then every stream sent by the tap is loaded into this schema.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| default_target_schema_select_permission | String    |              | Grant USAGE privilege on newly created schemas and grant SELECT privilege on newly created                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| schema_mapping                          | Object    |              | Useful if you want to load multiple streams from one tap to multiple BigQuery schemas.<br><br>If the tap sends the `stream_id` in `<schema_name>-<table_name>` format then this option overwrites the `default_target_schema` value. Note, that using `schema_mapping` you can overwrite the `default_target_schema_select_permission` value to grant SELECT permissions to different groups per schemas or optionally you can create indices automatically for the replicated tables.<br><br> **Note**: This is an experimental feature and recommended to use via PipelineWise YAML files that will generate the object mapping in the right JSON format. For further info check a [PipelineWise YAML Example](https://transferwise.github.io/pipelinewise/connectors/taps/mysql.html#configuring-what-to-replicate). |
| add_metadata_columns                    | Boolean   |              | (Default: False) Metadata columns add extra row level information about data ingestions, (i.e. when was the row read in source, when was inserted or deleted in bigquery etc.) Metadata columns are creating automatically by adding extra columns to the tables with a column prefix `_sdc_`. The column names are following the stitch naming conventions documented at https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns. Enabling metadata columns will flag the deleted rows by setting the `_sdc_deleted_at` metadata column. Without the `add_metadata_columns` option the deleted rows from singer taps will not be recognisable in BigQuery.                                                                                                                                      |
| hard_delete                             | Boolean   |              | (Default: False) When `hard_delete` option is true then DELETE SQL commands will be performed in BigQuery to delete rows in tables. It's achieved by continuously checking the  `_sdc_deleted_at` metadata column sent by the singer tap. Due to deleting rows requires metadata columns, `hard_delete` option automatically enables the `add_metadata_columns` option as well.                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| data_flattening_max_level               | Integer   |              | (Default: 0) Object type RECORD items from taps can be loaded into VARIANT columns as JSON (default) or we can flatten the schema by creating columns automatically.<br><br>When value is 0 (default) then flattening functionality is turned off.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| primary_key_required                    | Boolean   |              | (Default: True) Log based and Incremental replications on tables with no Primary Key cause duplicates when merging UPDATE events. When set to true, stop loading data if no Primary Key is defined.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| validate_records                        | Boolean   |              | (Default: False) Validate every single record message to the corresponding JSON schema. This option is disabled by default and invalid RECORD messages will fail only at load time by BigQuery. Enabling this option will detect invalid records earlier but could cause performance degradation.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| temp_schema                             | String    |              | Name of the schema where the temporary tables will be created. Will default to the same schema as the target tables                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |


### Schema Changes

This target does follow the [PipelineWise specification](https://transferwise.github.io/pipelinewise/user_guide/schema_changes.html) for schema changes except versioning columns because of the way BigQuery works.

BigQuery does not allow for column renames so a column modification works like this instead:

#### Versioning columns

Target connectors are versioning columns **when data type change is detected** in the source
table. Versioning columns means that the old column with the old datatype is kept
and a new column is created by adding a suffix to the name depending of the type (and also a timestamp for struct and arrays)
to the column name with the new data type. This new column will be added to the table.

For example if the data type of ``COLUMN_THREE`` changes from ``INTEGER`` to ``VARCHAR``
PipelineWise will replicate data in this order:

1. Before changing data type ``COLUMN_THREE`` is ``INTEGER`` just like in in source table:

| **COLUMN_ONE** | **COLUMN_TWO** | **COLUMN_THREE** (INTEGER) |
|----------------|----------------|----------------------------|
| text           | text           | 1                          |
| text           | text           | 2                          |
| text           | text           | 3                          |

2. After the data type change ``COLUMN_THREE`` remains ``INTEGER`` with
the old data and a new ``COLUMN_TREE__st`` column created with ``STRING`` type that keeps
data only after the change.

| **COLUMN_ONE** | **COLUMN_TWO** | **COLUMN_THREE** (INTEGER) | **COLUMN_THREE__st** (VARCHAR) |
|----------------|----------------|----------------------------|--------------------------------|
| text           | text           | 111                        |                                |
| text           | text           | 222                        |                                |
| text           | text           | 333                        |                                |
| text           | text           |                            | 444-ABC                        |
| text           | text           |                            | 555-DEF                        |

.. warning::

  Please note the ``NULL`` values in ``COLUMN_THREE`` and ``COLUMN_THREE__st`` columns.
  **Historical values are not converted to the new data types!**
  If you need the actual representation of the table after data type changes then
  you need to resync the table.


### To run tests:

1. Define environment variables that requires running the tests
```
  export GOOGLE_APPLICATION_CREDENTIALS=<credentials-json-file>
  export TARGET_BIGQUERY_PROJECT=<bigquery project to run your tests on>
  export TARGET_BIGQUERY_SCHEMA=<temporary schema for running the tests>
```

2. Install python dependencies in a virtual env and run nose unit and integration tests
```
make venv
```

3. To run unit tests:
```
make unit_test
```

4. To run integration tests:
```
make integration_test
```

### To run pylint:

1. Install python dependencies and run python linter
```
make venv pylint
```

## License

Apache License Version 2.0

See [LICENSE](LICENSE) to see the full text.
