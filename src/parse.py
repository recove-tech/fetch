from typing import Dict, Optional

from .enums import VALID_FILTER_KEYS
from .models import Item, CategoryType, Domain, VintedResponse, VintedCatalog


def parse_filters(response: VintedResponse) -> Dict:
    if response.status_code != 200:
        return {}

    filters = dict()
    iterator = (response.data or {}).get("filters", [])

    for entry in iterator:
        filter_key = entry.get("code")

        if not isinstance(filter_key, str):
            continue

        if filter_key in VALID_FILTER_KEYS:
            filter_options = entry.get("options", [])

            if not isinstance(filter_options, list):
                continue

            if filter_options:
                option_ids, option_titles = [], []

                for option in filter_options:
                    option_id = option.get("id")
                    option_title = option.get("title")

                    if not isinstance(option_id, int):
                        continue

                    if not isinstance(option_title, str):
                        continue

                    option_ids.append(option_id)
                    option_titles.append(option_title)

                filters[filter_key] = {
                    "id": option_ids,
                    "title": option_titles,
                }

    return filters


def parse_item(
    data: Dict,
    catalog: VintedCatalog,
    vinted_domain: Domain,
    material_id: Optional[int] = None,
    pattern_id: Optional[int] = None,
    color_id: Optional[int] = None,
    created_at: Optional[str] = None,
    unix_created_at: Optional[int] = None,
) -> Item:
    return Item(
        created_at=created_at,
        unix_created_at=unix_created_at,
        vinted_id=str(data["id"]),
        vinted_domain=vinted_domain,
        catalog_id=catalog.id,
        category_type=catalog.category_type,
        women=catalog.women,
        url=data["url"],
        title=data["title"],
        brand=_parse_brand(data),
        price=_parse_price(data),
        currency=_parse_currency(data),
        size=_parse_size(data),
        condition=data.get("status"),
        image_location=_parse_image_location(data),
        material_id=material_id,
        pattern_id=pattern_id,
        color_id=color_id,
    )


def _parse_size(data: Dict) -> Optional[str]:
    size = data.get("size_title")
    if not size:
        return None

    try:
        return size.split(" / ")[0].replace(",", ".")
    except:
        return None


def _parse_price(data: Dict) -> float:
    return float(data.get("price", {}).get("amount"))


def _parse_currency(data: Dict) -> Optional[str]:
    try:
        return data.get("price", {}).get("currency_code")
    except:
        return None


def _parse_brand(data: Dict) -> str:
    return data.get("brand_title")


def _parse_image_location(data: Dict) -> str:
    return data["photo"]["url"]
