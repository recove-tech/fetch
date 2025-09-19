from typing import Optional
from .enums import *


CATALOG_BASE_QUERY = f"""
SELECT catalog.*, category.category_type
FROM `{PROJECT_ID}.{DATASET_ID}.{CATALOG_TABLE_ID}` AS catalog
INNER JOIN `{PROJECT_ID}.{DATASET_ID}.{CATEGORY_TABLE_ID}` AS category ON catalog.id = category.catalog_id
"""


def query_catalogs(
    is_women: bool, importance_score: Optional[int] = None, n: Optional[int] = None
) -> str:
    if not importance_score:
        subquery = f"""
WITH base_query AS ({CATALOG_BASE_QUERY})
        """
    else:
        subquery = f"""
WITH 
    catalog_importance AS (
    SELECT catalog_id, score
    FROM `{PROJECT_ID}.{DATASET_ID}.{CATALOG_IMPORTANCE_TABLE_ID}`
    WHERE score = {importance_score})  
    , base_query AS (
    SELECT c.*
    FROM ({CATALOG_BASE_QUERY}) AS c
    INNER JOIN catalog_importance AS ci ON c.id = ci.catalog_id)
    """

    query = f"""
{subquery}
SELECT *
FROM base_query
WHERE women = {is_women} AND is_valid = TRUE AND is_active = TRUE
ORDER BY RAND()
    """

    if n:
        query += f"LIMIT {n}"

    return query
