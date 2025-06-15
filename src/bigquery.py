from typing import List, Dict, Union, Optional

from google.oauth2 import service_account
from google.cloud import bigquery
from .enums import *


def init_client(credentials_dict: Dict) -> bigquery.Client:
    credentials_dict["private_key"] = credentials_dict["private_key"].replace(
        "\\n", "\n"
    )

    credentials = service_account.Credentials.from_service_account_info(
        credentials_dict
    )

    return bigquery.Client(
        credentials=credentials, project=credentials_dict["project_id"]
    )


def load_table(
    client: bigquery.Client,
    table_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    query: Optional[str] = None,
    conditions: List[str] = None,
    fields: List[str] = None,
    order_by: str = None,
    descending: Optional[bool] = None,
    limit: int = None,
    to_list: bool = True,
) -> Union[List[Dict], bigquery.table.RowIterator]:
    field_str = ", ".join(fields) if fields else "*"

    if table_id and dataset_id:
        source_table = f"`{PROJECT_ID}.{dataset_id}.{table_id}`"
    elif query:
        source_table = f"({query})"
    else:
        raise ValueError("Either table_id and dataset_id or query must be provided")

    query = f"SELECT {field_str} FROM {source_table}"

    if conditions:
        query += f" WHERE {' AND '.join(conditions)}"

    if order_by:
        query += f" ORDER BY {order_by}"
        if descending is not None:
            query += f" {'DESC' if descending else 'ASC'}"

    if limit:
        query += f" LIMIT {limit}"

    query_job = client.query(query)
    results = query_job.result()

    if to_list:
        return [dict(row) for row in results]
    else:
        return results


def upload(
    client: bigquery.Client, dataset_id: str, table_id: str, rows: List[Dict]
) -> bool:
    try:
        errors = client.insert_rows_json(
            table=f"{PROJECT_ID}.{dataset_id}.{table_id}", json_rows=rows
        )
        return len(errors) == 0
    except Exception as e:
        print(e)
        return False


def insert_staging_rows(
    client: bigquery.Client, dataset_id: str, table_id: str, reference_field: str
) -> int:
    query = f"""
    INSERT INTO `{PROJECT_ID}.{dataset_id}.{table_id}`
    SELECT * FROM `{PROJECT_ID}.{dataset_id}.{table_id}_staging`
    WHERE {reference_field} NOT IN (SELECT {reference_field} FROM `{PROJECT_ID}.{dataset_id}.{table_id}`)
    ORDER BY RAND()
    """

    try:
        query_job = client.query(query)
        query_job.result()
        return query_job.num_dml_affected_rows

    except Exception as e:
        print(e)
        return -1


def reset_staging_table(
    client: bigquery.Client, dataset_id: str, table_id: str, field_id: str
) -> bool:
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{dataset_id}.{table_id}_staging` AS
    SELECT * FROM `{PROJECT_ID}.{dataset_id}.{table_id}` LIMIT 0;
    """

    try:
        client.query(query).result()
        return True
    except Exception as e:
        print(e)
        return False


def query_catalogs_importance(importance_score: int) -> str:
    return f"""
    SELECT c.*
    FROM `{PROJECT_ID}.{DATASET_ID}.{CATALOG_TABLE_ID}` AS c
    INNER JOIN (
    SELECT catalog_id, score
    FROM `{PROJECT_ID}.{DATASET_ID}.{CATALOG_IMPORTANCE_TABLE_ID}`
    WHERE score = {importance_score}
    ) AS ci ON c.id = ci.catalog_id
    """
