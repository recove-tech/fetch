from typing import List, Dict, Iterable, Optional, Tuple

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


def create_table(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    schema: List[bigquery.SchemaField],
) -> Tuple[bool, Optional[str]]:
    table_ref = client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)

    try:
        client.create_table(table)
        return True, None

    except Exception as e:
        return False, str(e)


def load_table(
    client: bigquery.Client,
    table_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    query: Optional[str] = None,
    conditions: Optional[List[str]] = None,
    fields: Optional[List[str]] = None,
    order_by: Optional[str] = None,
    descending: Optional[bool] = None,
    limit: Optional[int] = None,
    to_list: bool = True,
) -> Iterable:
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
) -> Tuple[bool, Optional[List[str]]]:
    try:
        errors = client.insert_rows_json(
            table=f"{PROJECT_ID}.{dataset_id}.{table_id}", json_rows=rows
        )

        success = len(errors) == 0

        return success, errors

    except Exception as e:
        return False, [str(e)]
