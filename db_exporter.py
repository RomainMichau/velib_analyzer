import asyncio
import logging
import time
from datetime import datetime
from typing import List

from entities import Station
from services.sql_service import SqlService
from services.velib_api_service import VelibApiService
from dateutil.tz import tzlocal


class DB_export:
    def __init__(self, sql: SqlService, api: VelibApiService):
        self.sql = sql
        self.api = api
        self.local_tz = tzlocal()

    def run(self):
        asyncio.run(self.async_run())

    async def insert_station_if_required_async(self, station: Station):
        if await self.sql.get_station_by_code_async(station.code) is None:
            logging.info(f"Inserting station code: {station.code}/{station.name} in database")
            await self.sql.insert_station_async(station.name, station.longitude, station.latitude, station.code)

    def insert_station_if_required(self, station: Station):
        if self.sql.get_station_by_code(station.code) is None:
            logging.info(f"Inserting station code: {station.code}/{station.name} in database")
            self.sql.insert_station(station.name, station.longitude, station.latitude, station.code)

    async def async_run(self):
        task = []
        self.sql.insert_run(self.__get_now())
        stations = self.api.get_all_station()[:10000]
        task = []
        start = time.time()
        for station in stations:
            task.append(self.insert_station_if_required_async(station))
        result = await asyncio.gather(*task)
        end = time.time()
        print(task)
        print(end - start)
        #     for velib in velibs:
        #         if self.sql.get_velib_by_code(velib.code) is None:
        #             logging.info(f"Inserting velib code: {velib.code} in database")
        #             self.sql.insert_velib(velib.code, velib.is_electric)
        #         last_docked = self.sql.get_last_velib_docked(velib.code)
        #         if last_docked is None or last_docked.station_code != station.code:
        #             task.append(await self.sql.insert_docked_async(velib.code, self.__get_now(), station.code))
        # #  await asyncio.gather(*task)
        # print(stations)

    def __get_now(self):
        return datetime.now(tz=self.local_tz)
