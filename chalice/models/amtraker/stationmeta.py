from typing import List
from pydantic import BaseModel


class StationMeta(BaseModel):
    name: str
    code: str
    tz: str
    lat: float
    lon: float
    hasAddress: bool
    address1: str
    address2: str
    city: str
    state: str
    zip: int
    trains: List[str]
