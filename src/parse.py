from typing import Dict, Tuple, Optional, List

from .enums import VALID_FILTER_KEYS, MAX_BRAND_TITLE_LENGTH
from .vinted import VintedResponse, Domain
from .models import Item, Image, ItemDetails, ItemLocalization


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
    catalog_id: int,
    vinted_domain: Domain,
    visited: List[int] = [],
    material_id: Optional[int] = None,
    pattern_id: Optional[int] = None,
    color_id: Optional[int] = None,
) -> Optional[Tuple[Item, Image, ItemDetails, ItemLocalization]]:
    try:
        result = _parse_item(
            data=data,
            catalog_id=catalog_id,
            vinted_domain=vinted_domain,
            material_id=material_id,
            pattern_id=pattern_id,
            color_id=color_id,
        )

        if not result:
            return None

        item_entry, image_entry, item_details_entry, localization_entry = result

        if item_entry.vinted_id in visited:
            return None

        return item_entry, image_entry, item_details_entry, localization_entry

    except:
        return None


def _parse_item(
    data: Dict,
    catalog_id: int,
    vinted_domain: Domain,
    material_id: Optional[int] = None,
    pattern_id: Optional[int] = None,
    color_id: Optional[int] = None,
) -> Optional[Tuple[Item, Image, ItemDetails, ItemLocalization]]:
    vinted_id = str(data.get("id"))
    if not vinted_id:
        return None

    image_url = data.get("photo", {}).get("url")
    if not image_url:
        return None

    item_url = data.get("url")
    if not item_url:
        return None

    brand_title = _parse_brand(data)
    if brand_title is None or len(brand_title) >= MAX_BRAND_TITLE_LENGTH:
        return None

    parsed_item = Item(
        vinted_id=vinted_id,
        catalog_id=catalog_id,
        title=data["title"],
        url=item_url,
        price=_parse_price(data),
        currency=_parse_currency(data),
        brand=brand_title,
        size=_parse_size(data),
        condition=data.get("status"),
        is_available=True,
    )

    image = Image(
        vinted_id=vinted_id,
        url=image_url,
        nobg=False,
        size="original",
    )

    item_details = ItemDetails(
        item_id=parsed_item.id,
        material_id=material_id,
        pattern_id=pattern_id,
        color_id=color_id,
    )

    localization = ItemLocalization(
        item_id=parsed_item.id,
        domain=vinted_domain,
    )

    return parsed_item, image, item_details, localization


def _parse_size(data: Dict) -> Optional[str]:
    size = data.get("size_title")
    if not size:
        return None

    try:
        return size.split(" / ")[0].replace(",", ".")
    except:
        return None


def _parse_price(data: Dict) -> Optional[float]:
    try:
        return float(data.get("price", {}).get("amount"))
    except:
        return


def _parse_currency(data: Dict) -> Optional[str]:
    try:
        return data.get("price", {}).get("currency_code")
    except:
        return None


def _parse_brand(data: Dict) -> Optional[str]:
    try:
        return data.get("brand_title")
    except:
        return None
