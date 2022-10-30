from dataclasses import dataclass
from datetime import datetime


@dataclass
class VelibSql:
    code: int
    is_electric: bool


@dataclass(frozen=True)
class StationSql:
    name: str
    longitude: float
    latitude: float
    code: int


@dataclass
class RatingSql:
    velib_code: int
    rate: int
    timestamp: datetime


@dataclass
class VelibDockedSql:
    velib_code: int
    station_code: int
    timestamp: datetime
    available: bool
