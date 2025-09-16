from typing import Optional, Dict

from pydantic import BaseModel

from .vinted.enums import Domain
from .utils import generate_uuid, generate_timestamp, generate_unix_timestamp


class Item(BaseModel):
    vinted_id: str
    catalog_id: int
    title: str
    url: str
    price: float
    currency: str
    brand: str
    is_available: bool = True
    size: Optional[str] = None
    condition: Optional[str] = None
    id: str = None
    created_at: str = None
    updated_at: str = None
    unix_created_at: int = None

    class Config:
        validate_assignment = True

    def __init__(self, **data):
        super().__init__(**data)
        if self.id is None:
            self.id = generate_uuid()
        if self.created_at is None:
            self.created_at = generate_timestamp()
        if self.updated_at is None:
            self.updated_at = generate_timestamp()
        if self.unix_created_at is None:
            self.unix_created_at = generate_unix_timestamp()

    def to_dict(self) -> Dict:
        return self.__dict__


class Image(BaseModel):
    vinted_id: str
    url: str
    nobg: bool
    size: str
    id: str = None
    created_at: str = None

    class Config:
        validate_assignment = True

    def __init__(self, **data):
        super().__init__(**data)
        if self.id is None:
            self.id = generate_uuid()
        if self.created_at is None:
            self.created_at = generate_timestamp()

    def to_dict(self) -> Dict:
        return self.__dict__


class ItemDetails(BaseModel):
    item_id: str
    material_id: Optional[int] = None
    pattern_id: Optional[int] = None
    color_id: Optional[int] = None
    created_at: str = None

    class Config:
        validate_assignment = True

    def __init__(self, **data):
        super().__init__(**data)
        if self.created_at is None:
            self.created_at = generate_timestamp()

    def to_dict(self) -> Dict:
        return self.__dict__


class ItemLocalization(BaseModel):
    item_id: str
    domain: Domain
    created_at: str = None

    class Config:
        validate_assignment = True

    def __init__(self, **data):
        super().__init__(**data)
        if self.created_at is None:
            self.created_at = generate_timestamp()

    def to_dict(self) -> Dict:
        return self.__dict__
