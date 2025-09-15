from typing import Dict, Optional
from dataclasses import dataclass
from datetime import datetime

from .enums import Domain, ROOT_URL


@dataclass
class VintedResponse:
    status_code: int
    data: Optional[Dict] = None


@dataclass
class VintedCatalog:
    id: int
    title: str
    code: str
    url: str
    women: bool
    domain: Domain = "fr"
    is_valid: bool = True
    is_active: bool = True

    def __post_init__(self):
        self.created_at = datetime.now().isoformat()
        self.url = ROOT_URL(self.domain) + self.url

    def to_dict(self) -> Dict:
        return self.__dict__


@dataclass
class ProxyConfig:
    password: str
    country_code: str = "FR"
    _hostname: str = "proxy.apify.com"
    _port: int = 8000

    @property
    def url_datacenter(self) -> str:
        username = f"auto:{self.password}"

        return f"http://{username}@{self._hostname}:{self._port}"

    @property
    def url_residential(self) -> str:
        username = f"groups-RESIDENTIAL,country-{self.country_code}:{self.password}"
        return f"http://{username}@{self._hostname}:{self._port}"

    @property
    def url(self) -> str:
        return self.url_residential
