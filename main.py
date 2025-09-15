import sys

sys.path.append("../")

from typing import Tuple, Optional, Iterable
import argparse, random, logging
import src

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


DOMAIN = "fr"
FILTER_BY_CHOICES = ["material", "patterns", "color"]
REFERENCE_FIELD = "vinted_id"
SHUFFLE_ALPHA = 0.4


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
    secrets = src.utils.load_json_file("secrets.json")

    gcp_credentials = secrets.get("GCP_CREDENTIALS")
    bq_client = src.bigquery.init_client(credentials_dict=gcp_credentials)

    proxy_config = src.vinted.ProxyConfig(password=secrets.get("APIFY_PROXY_PASSWORD"))
    vinted_client = src.vinted.Vinted(domain=DOMAIN, proxy_config=proxy_config)
    vinted_client.fetch_cookies()

    return bq_client, vinted_client


def get_dataloader(women: bool, catalog_importance: Optional[int] = None) -> Iterable:
    conditions = [
        f"women = {women}",
        "is_valid = TRUE",
        "is_active = TRUE",
    ]

    kwargs = {
        "client": bq_client,
        "conditions": conditions,
        "order_by": "RAND()",
    }

    if catalog_importance:
        query = src.bigquery.query_catalogs_importance(catalog_importance)
        loader = src.bigquery.load_table(
            query=query,
            **kwargs,
        )

    else:
        loader = src.bigquery.load_table(
            table_id=src.enums.CATALOG_TABLE_ID,
            dataset_id=src.enums.DATASET_ID,
            **kwargs,
        )

    return loader


def main(filter_by: str = None, only_vintage: bool = False):
    global bq_client, vinted_client
    bq_client, vinted_client = initialize_clients()

    if random.random() < SHUFFLE_ALPHA:
        catalog_importances = [None]
    else:
        catalog_importances = [1, 2, 3]

    for catalog_importance in catalog_importances:
        for women in [True, False]:
            loader = get_dataloader(women, catalog_importance)
            logging.info(
                f"Processing catalogs - women: {women} | filter_by: {filter_by} | catalogs: {len(loader)}"
            )

            scraper = src.scraper.VintedScraper(
                bq_client=bq_client,
                vinted_client=vinted_client,
            )

            scraper.run(
                catalogs=loader,
                filter_by=filter_by,
                only_vintage=only_vintage,
                women=women,
            )

            scraper.insert_from_staging()
            logging.info(f"Inserted {scraper.num_inserted} records")
            scraper.reset_staging()


if __name__ == "__main__":
    kwargs = parse_args()
    main(**kwargs)
