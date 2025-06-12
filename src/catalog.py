from typing import List, Dict, Any

from .vinted.models import VintedResponse, VintedCatalog
from .enums import VALID_CATALOG_CODES


def get_all_catalogs(response: VintedResponse) -> List[Dict[str, Any]]:
    iterator = response.data.get("dtos", {}).get("catalogs", [])
    all_catalogs = []

    for entry in iterator:
        catalog_code = entry.get("code")
        input_catalogs = []

        if catalog_code not in VALID_CATALOG_CODES:
            continue

        if catalog_code == "DESIGNER_ROOT":
            input_catalogs = entry.get("catalogs", [])

        else:
            input_catalogs = [entry]

        for input_catalog in input_catalogs:
            is_women = check_is_women(input_catalog)
            unnested_catalogs = unnest(input_catalog)

            for unnested_catalog in unnested_catalogs:
                all_catalogs.append(parse(unnested_catalog, is_women))

    return all_catalogs


def unnest(catalog: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = []

    if "catalogs" in catalog and catalog["catalogs"]:
        for subcatalog in catalog["catalogs"]:
            result.extend(unnest(subcatalog))

    else:
        result.append(catalog)

    return result


def check_is_women(catalog: Dict[str, Any]) -> bool:
    return "WOMEN" in catalog.get("code")


def parse(entry: Dict[str, Any], is_women: bool) -> VintedCatalog:
    return VintedCatalog(
        id=entry.get("id"),
        title=entry.get("title"),
        code=entry.get("code"),
        url=entry.get("url"),
        women=is_women,
    )
