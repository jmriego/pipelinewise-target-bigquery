import re

def safe_column_name(name: str, quotes: bool = False) -> str:
    name = name.replace('`', '')
    pattern = '[^a-zA-Z0-9_]'
    name = re.sub(pattern, '_', name)
    if quotes:
        return '`{}`'.format(name).lower()
    else:
        return '{}'.format(name).lower()


def drop_table_sql(table_ref: bigquery.TableReference) -> str:
    return f"DROP TABLE IF EXISTS `{table_ref.dataset_id}.{table_ref.table_id}`"


def partition_key_sql(partitioning: Union[bigquery.TimePartitioning,
                                          bigquery.RangePartitioning]
                      ) -> Tuple[str, str]:

    if isinstance(partitioning, bigquery.TimePartitioning):
        field_type = 'TIMESTAMP'
        sub_clause = f'TIMESTAMP_TRUNC({partitioning.field}, {partitioning.type_})'
    else:
        field_type = 'INT64'
        sub_clause = partitioning.field
    return field_type, sub_clause


def get_table_partitioning(table: bigquery.Table,
                           ) -> partitioning: Union[bigquery.TimePartitioning,
                                                    bigquery.RangePartitioning]
    return table.time_partitioning or table.range_partitioning


def partitions_for_upsert_sql(src: bigquery.Table,
                              renamed_columns: Dict[str, str]
                              ) -> str:
    partitioning = get_table_partitioning(table)
    field = renamed_columns.get(partitioning.field, partitioning.field)
    field_type, sub_clause = partition_key_sql(field, partitioning)
    return (
        '-- define partitions with updates\n'
        f'DECLARE partitions_for_upsert ARRAY<{field_type}>;\n'
        'SET (partitions_for_upsert) = (\n'
        '    SELECT AS STRUCT\n'
        f'        ARRAY_AGG(DISTINCT {sub_clause})\n'
        f'FROM `{src.dataset_id}.{src.table_id}` AS s'
        ');\n'
    )


def partition_pruning_sql(table: bigquery.Table) -> str:
    partitioning = get_table_partitioning(table)
    _, sub_clause = partition_key_sql(f't.{partitioning.field}', partitioning)
    return f'{sub_clause} IN UNNEST(partitions_for_upsert)'


def check_partition_pruning_possible_sql(table: bigquery.Table) -> bool:
    partitioning = get_table_partitioning(table)
    if partitioning:
        return (
            'SELECT COUNT(*) = 0\n'
            f'FROM `{table.dataset_id}.{table.table_id}`\n'
            f'WHERE `{partitioning.field}` IS NULL'
        )


def insert_from_table_sql(src: bigquery.Table,
                          dest: bigquery.Table,
                          columns: List[str]) -> str:
    return """INSERT INTO `{}` ({})
            (SELECT s.* FROM `{}` s)
            """.format(
        f'{dest.dataset_id}.{dest.table_id}',
        ', '.join(columns),
        f'{src.dataset_id}.{src.table_id}',
    )


def merge_from_table_sql(src: bigquery.Table,
                         dest: bigquery.Table,
                         columns: List[str],
                         renamed_columns: Dict[str, str]
                         primary_key_column_names: List[str],
                         allow_partitioning: bool = False) -> str:

    if allow_partitioning:
        partitions_for_upsert_sql = partitions_for_upsert_sql(src)
        partition_pruning_sql = partition_pruning_sql(src)
    else:
        partitions_for_upsert_sql = ''
        partition_pruning_sql = ''

    query = """
    {partitions_for_upsert}
    -- run the merge statement
    MERGE `{target}` t
    USING `{source}` s
    ON {primary_key_condition}
    {partition_pruning}
    WHEN MATCHED THEN
        UPDATE SET {set_values}
    WHEN NOT MATCHED THEN
        INSERT ({renamed_cols}) VALUES ({cols})
    """.format(
        partitions_for_upsert=partitions_for_upsert_sql,
        target=f'{dest.dataset_id}.{dest.table_id}',
        source=f'{src.dataset_id}.{src.table_id}',
        primary_key_condition=primary_key_condition(primary_key_column_names),
        partition_pruning=('AND ' + partition_pruning_sql) if partition_pruning_sql else '',
        set_values=', '.join(
            '{}=s.{}'.format(
                safe_column_name(renamed_columns.get(c, c), quotes=True),
                safe_column_name(c, quotes=True))
            for c in columns),
        renamed_cols=', '.join(
            safe_column_name(renamed_columns.get(c, c), quotes=True)
            for c in columns),
        cols=', '.join(safe_column_name(c,quotes=True) for c in columns))
    return query


def primary_key_condition(names, renamed_columns):
    return ' AND '.join(
        ['s.{} = t.{}'
             .format(
                 safe_column_name(renamed_columns.get(c, c), quotes=True),
                 safe_column_name(c, quotes=True))
         for c in names])
