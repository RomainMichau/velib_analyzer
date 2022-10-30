import asyncio
import dataclasses
import json
import logging
import time
from datetime import datetime
from typing import List, Dict, Set
import concurrent.futures
from entities.velib_api_entities import StationApi
from services.sql_service import SqlService
from services.velib_api_service import VelibApiService
from dateutil.tz import tzlocal


class DB_export:
    def __init__(self, sql: SqlService, api: VelibApiService):
        self.sql = sql
        self.api = api
        self.local_tz = tzlocal()
        self.__inserted_velib = set()

    def run(self):

        asyncio.run(self.async_run())

    async def insert_station_if_required_async(self, station: StationApi, all_docked_velib: Dict[int, int],
                                               all_stations: Set[int], run_id: int):
        if station.code not in all_stations:
            await self.sql.insert_station_async(station.name, station.longitude, station.latitude, station.code, run_id)
        velibs = self.api.get_velib_at_station(station.name)
        for velib in velibs:
            last_docked = all_docked_velib.get(velib.code)
            if last_docked is None:
                await self.sql.insert_velib_async(velib.code, velib.is_electric, run_id)
            if last_docked is None or last_docked != station.code:
                await self.sql.insert_docked_async(velib.code, self.__get_now(), station.code, run_id,
                                                   velib.is_available)

    def insert_station_if_required(self, station: StationApi, run_id: int):
        if self.sql.get_station_by_code(station.code) is None:
            self.sql.insert_station(station.name, station.longitude, station.latitude, station.code, run_id)
        velibs = self.api.get_velib_at_station(station.name)
        for velib in velibs:
            if self.sql.get_velib_by_code(velib.code) is None:
                self.sql.insert_velib(velib.code, velib.is_electric, run_id)
            last_docked = self.sql.get_last_velib_docked(velib.code)
            if last_docked is None or last_docked.station_code != station.code:
                self.sql.insert_docked(velib.code, self.__get_now(), station.code, run_id, velib.is_available)

    async def async_run(self):
        all_docked_velib = self.sql.get_last_station_for_all_velib()
        all_stations = self.sql.get_all_stations_code()
        task = []
        run_id = self.sql.insert_run(self.__get_now())
        stations = self.api.get_all_station()
        res = []
        start = time.time()
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            for station in stations:
                futures.append(executor.submit(self.run_async, self.insert_station_if_required_async, station, all_docked_velib, all_stations, run_id))
            for future in concurrent.futures.as_completed(futures):
                task.append(future.result())
            #   task.append(self.insert_station_if_required_async(station, all_docked_velib, all_stations, run_id))
            await asyncio.gather(*task)
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

    async def run_async(self, funct, station, all_docked_velib, all_stations, run_id):
        await funct(station, all_docked_velib, all_stations, run_id)

    def __get_now(self):
        return datetime.now(tz=self.local_tz)
