from dataclasses import dataclass
from datetime import datetime


@dataclass
class Velib:
    code: int
    is_electric: bool


@dataclass
class Station:
    name: str
    longitude: float
    latitude: float
    code: str


@dataclass
class Rating:
    velib_code: int
    rate: int
    timestamp: datetime


@dataclass
class VelibDocked:
    velib_code: int
    station_code: int
    timestamp: datetime
