from typing import Optional, Dict, Literal, Union
from datetime import datetime
from pydantic import BaseModel, Field, validator

from .utils import generate_uuid, generate_timestamp, generate_unix_timestamp
from .enums import MAX_BRAND_TITLE_LENGTH


CategoryType = Literal[
    "top", "accessories", "bottom", "outerwear", "footwear", "dress", "suit"
]


Domain = Literal[
    "pl",
    "fr",
    "at",
    "be",
    "cz",
    "de",
    "dk",
    "es",
    "fi",
    "gr",
    "hr",
    "hu",
    "it",
    "lt",
    "lu",
    "nl",
    "pt",
    "ro",
    "se",
    "sk",
    "co.uk",
    "com",
]


class Item(BaseModel):
    vinted_id: str
    vinted_domain: Domain
    url: str
    catalog_id: int
    title: str
    brand: str = Field(..., max_length=MAX_BRAND_TITLE_LENGTH)
    price: float
    condition: str
    image_location: str
    category_type: CategoryType
    women: bool
    currency: Optional[str] = "EUR"
    size: Optional[str] = None
    material_id: Optional[int] = None
    pattern_id: Optional[int] = None
    color_id: Optional[int] = None
    id: Optional[str] = None
    created_at: Optional[str] = None
    unix_created_at: Optional[int] = None

    class Config:
        validate_assignment = True

    @validator("id", pre=True, always=True)
    def set_id(cls, v):
        return v or generate_uuid()

    @validator("created_at", pre=True, always=True)
    def set_created_at(cls, v):
        return v or generate_timestamp()

    @validator("unix_created_at", pre=True, always=True)
    def set_unix_created_at(cls, v):
        return v or generate_unix_timestamp()

    def to_dict(self) -> Dict:
        return self.dict()


class VintedResponse(BaseModel):
    status_code: int = 500
    data: Optional[Dict] = None
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.status_code == 200


class VintedCatalog(BaseModel):
    id: int
    title: str
    code: str
    url: str
    women: bool
    domain: Domain = "fr"
    is_valid: bool = True
    is_active: bool = True
    created_at: str = Field(default_factory=lambda: generate_timestamp())
    category_type: Optional[CategoryType] = None

    @validator("created_at", pre=True)
    def validate_created_at(cls, v):
        if isinstance(v, datetime):
            return v.isoformat()
        return v

    def __init__(self, **data):
        super().__init__(**data)
        self.url = f"https://www.vinted.{self.domain}{self.url}"

    def to_dict(self) -> Dict:
        return self.dict()

    @classmethod
    def from_dict(cls, data: Dict) -> "VintedCatalog":
        return cls(**data)


class ProxyConfig(BaseModel):
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
