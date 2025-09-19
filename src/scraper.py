from typing import List, Dict, Optional, Iterable

from tqdm import tqdm
from google.cloud import bigquery

from .vinted import Vinted
from .models import VintedResponse, VintedCatalog
from .preprocess import prepare_search_kwargs
from .parse import parse_filters, parse_item
from .utils import random_sleep, generate_timestamp, generate_unix_timestamp
from .bigquery import upload, create_table, ITEM_STAGING_SCHEMA, STAGING_DATASET_ID
from .enums import *


class VintedScraper:
    CREATED_AT = generate_timestamp()
    UNIX_CREATED_AT = generate_unix_timestamp()

    def __init__(
        self,
        bq_client: bigquery.Client,
        vinted_client: Vinted,
    ):
        self.bq_client = bq_client
        self.vinted_client = vinted_client
        self.vinted_client.set_use_proxy(False)

        self._reference_field = DEFAULT_REFERENCE_FIELD
        self._filter_batch_size = 1

        self._create_table()
        self.reset()

    def reset(self):
        self.n = 0
        self.n_success = 0
        self.current_catalog = 0
        self.counter = 0
        self.visited = []
        self.num_uploaded = 0

    def run(
        self,
        catalogs: List[VintedCatalog],
        filter_by: Optional[str] = None,
        only_vintage: bool = False,
    ):
        loop = tqdm(iterable=catalogs, total=len(catalogs))

        for catalog in loop:
            self.current_catalog = 0
            self.counter += 1

            filters_response = self.vinted_client.catalog_filters(
                catalog_ids=[catalog.id],
            )

            filters = parse_filters(filters_response)

            search_kwargs_list = self._process_catalog_filters(
                catalog.id, filters, filter_by, only_vintage
            )

            item_entries = []

            for search_kwargs in search_kwargs_list:
                material_id = search_kwargs.get("material_ids", [None])[0]
                pattern_id = search_kwargs.get("patterns_ids", [None])[0]
                color_id = search_kwargs.get("color_ids", [None])[0]

                response = self.vinted_client.search(**search_kwargs)
                if not response.ok:
                    continue

                entries = self._process_search_response(
                    response=response,
                    catalog=catalog,
                    material_id=material_id,
                    pattern_id=pattern_id,
                    color_id=color_id,
                )

                item_entries.extend(entries)

                self._update_progress(
                    loop,
                    catalog.women,
                    catalog.title,
                    color_id,
                )

            self.num_uploaded += self._upload(item_entries)

    def _update_progress(
        self,
        loop: Iterable,
        women: bool,
        catalog_title: str,
        color_id: Optional[int] = None,
    ):
        success_rate = self.n_success / self.n if self.n > 0 else 0

        msg = f"Women: {women} | " f"Catalog: {catalog_title} | "

        if color_id:
            msg += f"Color: {color_id} | "

        msg += (
            f"Items: {self.current_catalog} | "
            f"Processed: {self.n} | "
            f"Success: {self.n_success} | "
            f"Success rate: {success_rate:.2f} | "
            f"Uploaded: {self.num_uploaded} | "
        )

        loop.set_description(msg)

    def _create_table(self) -> bool:
        success, error = create_table(
            client=self.bq_client,
            dataset_id=STAGING_DATASET_ID,
            table_id=self.UNIX_CREATED_AT,
            schema=ITEM_STAGING_SCHEMA,
        )

        if not success:
            raise Exception(error)

        return success

    def _upload(self, item_entries: List[Dict]) -> int:
        num_uploaded = 0

        success, errors = upload(
            client=self.bq_client,
            dataset_id=STAGING_DATASET_ID,
            table_id=self.UNIX_CREATED_AT,
            rows=item_entries,
        )

        if success:
            num_uploaded += len(item_entries)

        return num_uploaded

    def _process_catalog_filters(
        self,
        catalog_id: int,
        filters: Dict,
        filter_by: Optional[str] = None,
        only_vintage: bool = False,
    ) -> List[Dict]:
        filter_by_updated = [filter_by]

        if catalog_id in DESIGNER_CATALOG_IDS:
            filter_by_updated.append("brand")

        search_kwargs_list = []

        for filter_key in filter_by_updated:
            search_kwargs = prepare_search_kwargs(
                catalog_id=catalog_id,
                filter_key=filter_key,
                filters=filters,
                batch_size=self._filter_batch_size,
                only_vintage=only_vintage,
            )
            search_kwargs_list.extend(search_kwargs)

        return search_kwargs_list

    def _process_search_response(
        self,
        response: VintedResponse,
        catalog: VintedCatalog,
        material_id: Optional[int] = None,
        pattern_id: Optional[int] = None,
        color_id: Optional[int] = None,
    ) -> List[Dict]:
        item_entries = []

        if response.status_code == 403:
            random_sleep()
            return item_entries

        elif response.status_code == 200 and isinstance(response.data, dict):
            data_list = response.data.get("items", [])

            for data in data_list:
                self.n += 1
                self.current_catalog += 1

                try:
                    item = parse_item(
                        data=data,
                        catalog=catalog,
                        vinted_domain=self.vinted_client.domain,
                        material_id=material_id,
                        pattern_id=pattern_id,
                        color_id=color_id,
                        created_at=self.CREATED_AT,
                        unix_created_at=self.UNIX_CREATED_AT,
                    )

                    if item.vinted_id in self.visited:
                        continue

                    item_entries.append(item.to_dict())
                    self.visited.append(item.vinted_id)
                    self.n_success += 1

                except Exception as e:
                    continue

        return item_entries
