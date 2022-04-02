from google.cloud import bigquery
from typing import List, Tuple, Union, Dict
import re

def safe_column_name(name: str, quotes: bool = False) -> str:
    name = name.replace('`', '')
    pattern = '[^a-zA-Z0-9_]'
    name = re.sub(pattern, '_', name)
    if quotes:
        return '`{}`'.format(name).lower()
    return '{}'.format(name).lower()

def safe_table_ref(table_ref: bigquery.TableReference) -> str:
    project_name = table_ref.project
    dataset_name = table_ref.dataset_id
    table_name = table_ref.table_id
    return '`{}`.`{}`.`{}`'.format(project_name, dataset_name, table_name)

def drop_table_sql(table: bigquery.Table) -> str:
    return f"DROP TABLE IF EXISTS `{table.dataset_id}.{table.table_id}`"


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


#pylint: disable=too-many-arguments
def merge_from_table_sql(src: bigquery.Table,
                         dest: bigquery.Table,
                         columns: List[str],
                         renamed_columns: Dict[str, str],
                         primary_key_column_names: List[str]) -> str:

    query = """
    -- run the merge statement
    MERGE `{target}` t
    USING `{source}` s
    ON {primary_key_condition}
    WHEN MATCHED THEN
        UPDATE SET {set_values}
    WHEN NOT MATCHED THEN
        INSERT ({renamed_cols}) VALUES ({cols})
    """.format(
        target=f'{dest.dataset_id}.{dest.table_id}',
        source=f'{src.dataset_id}.{src.table_id}',
        primary_key_condition=primary_key_condition(primary_key_column_names, renamed_columns),
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
