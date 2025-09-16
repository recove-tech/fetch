from typing import List, Dict, Optional

import random
from copy import deepcopy
from .enums import N_ITEMS_MAX, VINTAGE_BRAND_ID
from .utils import create_batches


def prepare_search_kwargs(
    catalog_id: int,
    filters: Dict,
    filter_key: Optional[str] = None,
    batch_size: int = 1,
    max_filter_options: Optional[int] = 10,
    only_vintage: bool = False,
) -> List[Dict]:
    base_search_kwargs = {"catalog_ids": [catalog_id], "per_page": N_ITEMS_MAX}

    if only_vintage:
        filter_search_kwargs = deepcopy(base_search_kwargs)
        filter_search_kwargs["brand_ids"] = [VINTAGE_BRAND_ID]

        return [filter_search_kwargs]

    filter_options = filters.get(filter_key, {}).get("id", [])
    filter_options = _select_filter_options(filter_options, max_filter_options)

    if filter_options:
        search_kwargs = []
        random.shuffle(filter_options)
        filter_options = create_batches(filter_options, batch_size)

        for batch_filter_options in filter_options:
            filter_search_kwargs = deepcopy(base_search_kwargs)
            filter_search_kwargs[f"{filter_key}_ids"] = batch_filter_options
            search_kwargs.append(filter_search_kwargs)

        return search_kwargs

    return [base_search_kwargs]


def _select_filter_options(options: List[int], n: Optional[int] = None) -> List[int]:
    if n is None:
        return options

    n = min(n, len(options))

    return random.sample(options, n)
