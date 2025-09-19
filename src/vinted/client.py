from typing import List, Literal, Optional, Dict

import requests, time
from requests.exceptions import RequestException, ConnectionError, Timeout

from .endpoints import Endpoints
from .utils import parse_url_to_params, retry_on_failure
from ..models import VintedResponse, ProxyConfig, Domain
from .enums import SortOption, USER_AGENT


class Vinted:
    BASE_HEADERS = {
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": USER_AGENT,
    }

    def __init__(
        self, domain: Domain = "fr", proxy_config: Optional[ProxyConfig] = None
    ) -> None:
        self.BASE_URL = f"https://www.vinted.{domain}"
        self.api_url = f"{self.BASE_URL}/api/v2"

        self.domain = domain
        self.proxy_config = proxy_config
        self.cookies = None
        self._use_proxy = False

        self.REQUESTS_KWARGS = {
            "headers": {**self.BASE_HEADERS, "Referer": self.BASE_URL},
            "allow_redirects": True,
            "timeout": 30,
        }

    @retry_on_failure(
        max_retries=3,
        initial_delay=1.0,
        backoff_factor=2.0,
        exceptions=(RequestException, ConnectionError, Timeout),
    )
    def fetch_cookies(self):
        response = self._call(method="get", url=self.BASE_URL)
        self.cookies = response.cookies

    def search(
        self,
        page: int = 1,
        per_page: int = 96,
        url: Optional[str] = None,
        query: Optional[str] = None,
        price_from: Optional[float] = None,
        price_to: Optional[float] = None,
        order: SortOption = "relevance",
        catalog_ids: Optional[List[int]] = None,
        size_ids: Optional[List[int]] = None,
        brand_ids: Optional[List[int]] = None,
        status_ids: Optional[List[int]] = None,
        color_ids: Optional[List[int]] = None,
        patterns_ids: Optional[List[int]] = None,
        material_ids: Optional[List[int]] = None,
    ) -> VintedResponse:
        params = {
            "page": page,
            "per_page": per_page,
            "time": time.time(),
            "search_text": query,
            "price_from": price_from,
            "price_to": price_to,
            "catalog_ids": catalog_ids,
            "order": order,
            "size_ids": size_ids,
            "brand_ids": brand_ids,
            "status_ids": status_ids,
            "color_ids": color_ids,
            "patterns_ids": patterns_ids,
            "material_ids": material_ids,
        }
        if url:
            params.update(parse_url_to_params(url))

        return self._get(Endpoints.CATALOG_ITEMS, params=params)

    def catalog_filters(
        self,
        query: Optional[str] = None,
        catalog_ids: Optional[int] = None,
        brand_ids: Optional[List[int]] = None,
        status_ids: Optional[List[int]] = None,
        color_ids: Optional[List[int]] = None,
    ) -> VintedResponse:
        params = {
            "search_text": query,
            "catalog_ids": catalog_ids,
            "time": time.time(),
            "brand_ids": brand_ids,
            "status_ids": status_ids,
            "color_ids": color_ids,
        }
        return self._get(Endpoints.CATALOG_FILTERS, params=params)

    def catalogs_list(self) -> VintedResponse:
        return self._get(
            Endpoints.CATALOG_INITIALIZERS,
            params={"page": 1, "time": time.time()},
        )

    def set_use_proxy(self, value: bool):
        self._use_proxy = value

    def _get(
        self,
        endpoint: Endpoints,
        format_values: Optional[Dict] = None,
        *args,
        **kwargs,
    ) -> VintedResponse:
        if format_values:
            url = self.api_url + endpoint.value.format(format_values)
        else:
            url = self.api_url + endpoint.value

        try:
            response = self._call(method="get", url=url, *args, **kwargs)

            if response.status_code == 200:
                model = VintedResponse(
                    status_code=response.status_code, data=response.json()
                )
            else:
                model = VintedResponse(
                    status_code=response.status_code, error=response.text
                )

        except Exception as e:
            model = VintedResponse(status_code=500, error=str(e))

        if not model.ok and not self._use_proxy and self.proxy_config:
            self.set_use_proxy(True)
            return self._get(endpoint, format_values, *args, **kwargs)

        return model

    def _call(self, method: Literal["get"], *args, **kwargs):
        kwargs.update(self.REQUESTS_KWARGS)
        use_proxy = self._use_proxy or not self.cookies

        if self.cookies:
            kwargs["cookies"] = self.cookies

        if self.proxy_config and use_proxy:
            kwargs["proxies"] = {
                "http": self.proxy_config.url,
                "https": self.proxy_config.url,
            }

        return requests.request(method=method, *args, **kwargs)
