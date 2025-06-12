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
