1.3.0 (2021-12-20)
-------------------
- Add `hard_delete_mapping` for switching specific streams
- Add `batch_wait_limit_seconds` config
- Fix nested keys issue
- bump multiple dependencies

1.2.0 (2021-12-20)
-------------------
- Allow `REPEATED` fields of type `RECORD` to be created.
- bump pylint

1.1.2 (2021-12-02)
-------------------
- bump joblib to 1.1.0 to fix issue https://github.com/transferwise/pipelinewise-target-bigquery/issues/7
- bump test dependencies
- remove unused code

1.1.1 (2021-06-04)
-------------------
- Minor internal bugfixes

1.1.0 (2021-05-21)
-------------------
- Add location BigQuery configuration
- Fix for reserved table names
- Fix for bad decimals
- Improved logging of bad data
- Refactor code to make it more similar to other taps
- Truncate big numbers to the maximum range for NUMERIC in BigQuery
- Fix an issue when loading table names with reserved keywords

1.0.1 (2020-07-05)
-------------------

- Fixed an issue when the source tap can include uppercase primary keys

1.0.0 (2020-06-09)
-------------------

- Initial release
