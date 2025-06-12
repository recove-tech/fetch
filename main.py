import sys

sys.path.append("../")

from typing import List, Tuple, Dict
import json, os, argparse
import src


DOMAIN = "fr"
FILTER_BY_CHOICES = ["material", "patterns", "color"]
REFERENCE_FIELD = "vinted_id"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--only_vintage",
        "-v",
        default=False,
        type=lambda x: x.lower() == "true",
    )
    parser.add_argument(
        "--filter_by",
        "-fby",
        choices=FILTER_BY_CHOICES + ["None"],
        default="None",
    )
    args = parser.parse_args()

    if args.filter_by == "None":
        args.filter_by = None

    return vars(args)


def initialize_clients() -> Tuple:
    secrets = json.loads(os.getenv("SECRETS_JSON"))
    gcp_credentials = secrets.get("GCP_CREDENTIALS")

    bq_client = src.bigquery.init_client(credentials_dict=gcp_credentials)
    vinted_client = src.vinted.Vinted(domain=DOMAIN)

    return bq_client, vinted_client


def load_catalogs(women: bool) -> List[Dict]:
    conditions = [
        f"women = {women}",
        "is_valid = TRUE",
        "is_active = TRUE",
    ]

    return src.bigquery.load_table(
        client=bq_client,
        table_id=src.enums.CATALOG_TABLE_ID,
        conditions=conditions,
        order_by="RAND()",
    )


def main(only_vintage: bool, filter_by: str = None):
    global bq_client, vinted_client
    bq_client, vinted_client = initialize_clients()

    for women in [True, False]:
        catalogs = load_catalogs(women)
        print(f"women: {women} | filter_by: {filter_by} | catalogs: {len(catalogs)}")

        scraper = src.scraper.VintedScraper(
            bq_client=bq_client,
            vinted_client=vinted_client,
        )

        scraper.run(
            catalogs=catalogs,
            filter_by=filter_by,
            only_vintage=only_vintage,
            women=women,
        )

        scraper.insert_from_staging()
        print(f"Inserted: {scraper.num_inserted}")

    scraper.reset_staging()


if __name__ == "__main__":
    kwargs = parse_args()
    main(**kwargs)
