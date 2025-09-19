from google.cloud import bigquery


ITEM_STAGING_SCHEMA = schema = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("unix_created_at", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("vinted_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("vinted_domain", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("catalog_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("brand", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("size", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("condition", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("image_location", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("category_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("women", "BOOLEAN", mode="REQUIRED"),
    bigquery.SchemaField("material_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("pattern_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("color_id", "INTEGER", mode="NULLABLE"),
]
