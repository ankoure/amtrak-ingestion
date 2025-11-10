from typing import List, Optional
from pydantic import BaseModel
from .station import Station
from .train_alert import TrainAlert


class Train(BaseModel):
    routeName: str
    trainNum: str
    trainNumRaw: str
    trainID: str
    lat: float
    lon: float
    trainTimely: str
    iconColor: str
    stations: List[Station]
    heading: str
    eventCode: str
    eventTZ: Optional[str] = None
    eventName: Optional[str] = None
    origCode: str
    originTZ: Optional[str] = None
    origName: str
    destCode: str
    destTZ: Optional[str] = None
    destName: str
    trainState: str
    velocity: float
    statusMsg: str
    createdAt: str
    updatedAt: str
    lastValTS: str
    objectID: Optional[int] = None
    provider: str
    providerShort: str
    onlyOfTrainNum: bool
    alerts: List[TrainAlert]
