import sys

sys.path.append("../")

from typing import Tuple, Optional, Iterable
import argparse, random, logging
import src

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/run.log"), logging.StreamHandler()],
)


DOMAIN = "fr"
FILTER_BY_CHOICES = ["material", "patterns", "color"]
REFERENCE_FIELD = "vinted_id"
MAIN_CATALOG_IMPORTANCE_ALPHA = 1.0
NUM_CATALOGS = None


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--vinted_domain",
        "-domain",
        choices=src.vinted.VALID_VINTED_DOMAINS,
        default=DOMAIN,
        required=True,
    )
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


def initialize_clients(vinted_domain: src.vinted.Domain) -> Tuple:
    secrets = src.utils.load_json_file("secrets.json")

    gcp_credentials = secrets.get("GCP_CREDENTIALS")
    bq_client = src.bigquery.init_client(credentials_dict=gcp_credentials)

    proxy_config = src.vinted.ProxyConfig(password=secrets.get("APIFY_PROXY_PASSWORD"))
    vinted_client = src.vinted.Vinted(domain=vinted_domain, proxy_config=proxy_config)
    vinted_client.fetch_cookies()

    return bq_client, vinted_client


def get_catalog_importance() -> Optional[int]:
    if random.random() < MAIN_CATALOG_IMPORTANCE_ALPHA:
        return 1

    return 2


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
        query = src.bigquery.query_catalogs_importance(catalog_importance, NUM_CATALOGS)
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


def main(
    vinted_domain: str = src.vinted.Domain,
    filter_by: str = None,
    only_vintage: bool = False,
):
    global bq_client, vinted_client
    bq_client, vinted_client = initialize_clients(vinted_domain)

    scraper = src.scraper.VintedScraper(
        bq_client=bq_client,
        vinted_client=vinted_client,
    )

    catalog_importance = get_catalog_importance()

    for women in [True, False]:
        loader = get_dataloader(women, catalog_importance)

        logging.info(
            f"vinted_domain: {vinted_domain} | "
            f"women: {women} | "
            f"catalog_importance: {catalog_importance} | "
            f"catalogs: {len(loader)}"
        )

        scraper.run(
            catalogs=loader,
            filter_by=filter_by,
            only_vintage=only_vintage,
        )

        scraper.insert_from_staging()
        logging.info(f"Inserted {scraper.num_inserted} records")

    scraper.reset_staging()


if __name__ == "__main__":
    kwargs = parse_args()
    main(**kwargs)
