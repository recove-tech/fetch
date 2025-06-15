import sys

sys.path.append("../")

from typing import List, Tuple, Dict
import json, os, argparse, random
import src


DOMAIN = "fr"
FILTER_BY_CHOICES = ["material", "patterns", "color"]
REFERENCE_FIELD = "vinted_id"
SHUFFLE_ALPHA = .3


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--women",
        "-w",
        default=True,
        type=lambda x: x.lower() == "true",
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


def initialize_clients() -> Tuple:
    secrets = json.loads(os.getenv("SECRETS_JSON"))
    gcp_credentials = secrets.get("GCP_CREDENTIALS")

    bq_client = src.bigquery.init_client(credentials_dict=gcp_credentials)
    vinted_client = src.vinted.Vinted(domain=DOMAIN)

    return bq_client, vinted_client


def get_dataloader(women: bool) -> List[List[Dict]]:
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

    if random.random() < SHUFFLE_ALPHA:
        loader = src.bigquery.load_table(table_id=src.enums.CATALOG_TABLE_ID, **kwargs)

        return [loader]

    else:
        loaders = []

        for importance_score in range(1, 4):
            query = src.bigquery.query_catalogs_importance(importance_score)

            loader = src.bigquery.load_table(
                query=query,
                **kwargs,
            )

            loaders.append(loader)

        return loaders


def main(women: bool, only_vintage: bool, filter_by: str = None):
    global bq_client, vinted_client
    bq_client, vinted_client = initialize_clients()

    loaders = get_dataloader(women)

    for loader in loaders:
        print(f"women: {women} | filter_by: {filter_by} | catalogs: {len(loader)}")

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
        print(f"Inserted: {scraper.num_inserted}")

        scraper.reset_staging()


if __name__ == "__main__":
    kwargs = parse_args()
    main(**kwargs)
