from typing import Optional
from pydantic import BaseModel


class Station(BaseModel):
    name: str
    code: str
    tz: Optional[str] = None
    bus: bool
    schArr: str
    schDep: str
    arr: Optional[str] = None
    dep: Optional[str] = None
    arrCmnt: str
    depCmnt: str
    platform: str
    status: str
