from typing import List, Dict, Any, Optional

from .vinted.models import VintedResponse, VintedCatalog
from .enums import VALID_CATALOG_CODES


def get_all_catalogs(response: VintedResponse) -> List[Dict[str, Any]]:
    iterator = (response.data or {}).get("dtos", {}).get("catalogs", [])
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
    return "WOMEN" in catalog.get("code", "")


def parse(entry: Dict[str, Any], is_women: bool) -> Optional[VintedCatalog]:
    id_value = entry.get("id")
    title_value = entry.get("title")
    code_value = entry.get("code")
    url_value = entry.get("url")

    if not isinstance(id_value, int):
        return None

    if not isinstance(title_value, str):
        return None

    if not isinstance(code_value, str):
        return None

    if not isinstance(url_value, str):
        return None

    return VintedCatalog(
        id=id_value,
        title=title_value,
        code=code_value,
        url=url_value,
        women=is_women,
    )
