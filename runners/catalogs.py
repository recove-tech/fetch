import sys


sys.path.append("../")


import src
import json, os


def main():
    secrets = json.loads(os.getenv("SECRETS_JSON"))
    gcp_credentials = secrets.get("GCP_CREDENTIALS")

    bq_client = src.bigquery.init_client(credentials_dict=gcp_credentials)
    vinted_client = src.vinted.Vinted(domain="fr")

    bq_dataset = src.bigquery.load_table(
        client=bq_client,
        table_id=src.enums.CATALOG_TABLE_ID,
    )

    index = [entry["id"] for entry in bq_dataset]

    response = vinted_client.catalogs_list()
    vinted_dataset = src.catalog.get_all_catalogs(response)

    new_catalogs, bq_rows = [], []

    for entry in vinted_dataset:
        if entry.id not in index:
            new_catalogs.append(entry)
            bq_rows.append(entry.to_dict())

    print(f"New catalogs: {len(new_catalogs)}")

    success = src.bigquery.upload(
        client=bq_client,
        dataset_id=src.enums.DATASET_ID,
        table_id=src.enums.CATALOG_TABLE_ID,
        rows=bq_rows,
    )

    print(f"Upload: {success}")


if __name__ == "__main__":
    main()
