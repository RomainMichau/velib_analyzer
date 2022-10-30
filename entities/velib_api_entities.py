from dataclasses import dataclass
from datetime import datetime


@dataclass
class VelibApi:
    code: int
    is_electric: bool
    is_available: bool


@dataclass
class StationApi:
    name: str
    longitude: float
    latitude: float
    code: int
