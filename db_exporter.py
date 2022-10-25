import logging
from datetime import datetime

from services.sql_service import SqlService
from services.velib_api_service import VelibApiService
from dateutil.tz import tzlocal


class DB_export:
    def __init__(self, sql: SqlService, api: VelibApiService):
        self.sql = sql
        self.api = api
        self.local_tz = tzlocal()

    def run(self):
        res = self.sql.get_last_velib_docked(42706)
        self.sql.insert_run(self.__get_now())
        stations = self.api.get_all_station()
        for station in stations:
            if self.sql.get_station_by_code(station.code) is None:
                logging.info(f"Inserting station code: {station.code}/{station.name} in database")
                self.sql.insert_station(station.name, station.longitude, station.latitude, station.code)
            velibs = self.api.get_velib_at_station(station.name)
            for velib in velibs:
                if self.sql.get_velib_by_code(velib.code) is None:
                    logging.info(f"Inserting velib code: {velib.code} in database")
                    self.sql.insert_velib(velib.code, velib.is_electric)
                last_docked = self.sql.get_last_velib_docked(velib.code)
                if last_docked is None or last_docked.station_code != station.code:
                    self.sql.insert_docked(velib.code, self.__get_now(), station.code)
        print(stations)

    def __get_now(self):
        return datetime.now(tz=self.local_tz)
