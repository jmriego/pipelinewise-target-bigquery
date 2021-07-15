- Bump `google-cloud-bigquery` from `1.23.0` to `2.20.0`

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
