import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Set

import psycopg
from psycopg_pool import AsyncConnectionPool

from entities.sql_entities import VelibSql, StationSql, VelibDockedSql

SELECT_ALL_VELIB_CODE = """
SELECT velib_code FROM public.velibs
"""

GET_LAST_STATION_FOR_ALL_VELIB = """
select a.velib_code, a.station_code
    from velib_docked as a
    where "timestamp" = 
        (select max( "timestamp") from velib_docked as b where a.velib_code =b.velib_code) 
"""

SELECT_ALL_STATION = """
SELECT id, station_code, station_name, long, lat, station_code
FROM public.stations;
"""

SELECT_VELIB_BY_CODE = """
SELECT id, velib_code, electric
FROM public.velibs WHERE velib_code = %s;
"""

SELECT_STATION_BY_CODE = """
SELECT station_name, long, lat
FROM public.stations WHERE station_code = %s;
"""

GET_LAST_DOCKED_STATION_FOR_VELIB = """
SELECT "timestamp", station_code
FROM public.velib_docked code where velib_code = %s order by "timestamp" desc limit 1
"""

INSERT_VELIB = """
INSERT INTO public.velibs
(velib_code, electric, run)
VALUES(%s, %s, %s);
"""

INSERT_RATING = """
INSERT INTO public.rating
(velib_code, rate, rate_time, run)
VALUES(%s, %s, %s, %s);
"""

INSERT_RUN = """
INSERT INTO public.run
(run_time)
VALUES(%s) RETURNING id;
"""

INSERT_STATION = """
INSERT INTO public.stations
(station_name, long, lat, station_code, run)
VALUES(%s, %s, %s, %s, %s);
"""

INSERT_VELIB_DOCKED = """
INSERT INTO public.velib_docked
(velib_code, timestamp, station_code, run ,available)
VALUES(%s, %s, %s, %s, %s);
"""


class SqlService:
    def __init__(self, hostname: str, db_name: str, username: str, password: str, port: int = 4532):
        self.port = port
        self.hostname = hostname
        self.db_name = db_name
        self.username = username
        self.password = password
        self.conn_pool = asyncio.Queue()

        self.conn = psycopg.connect(
            f"host={self.hostname} dbname={self.db_name} user={self.username} password={self.password} port={self.port}")
        self.pool = asyncio.run(self.__init_pool(), debug=True)
        print(self.pool)

    async def __init_pool(self):
        pool = AsyncConnectionPool(
            f"hostaddr=94.23.43.148 dbname={self.db_name} user={self.username} password={self.password} port={self.port}"
            , min_size=4,
            timeout=1000)
        await pool.wait()
        return pool

    def get_all_velibs_code(self):
        cur = self.conn.cursor()
        cur.execute(SELECT_ALL_VELIB_CODE)
        sql_res = cur.fetchall()
        if sql_res is None:
            return list()
        res = list()
        for r in sql_res:
            res.append(r[0])
        cur.close()
        return res

    def get_velib_by_code(self, code: int) -> Optional[VelibSql]:
        cur = self.conn.cursor()
        cur.execute(SELECT_VELIB_BY_CODE, (code,))
        sql_res = cur.fetchall()
        if sql_res is None or len(sql_res) == 0:
            return None
        if len(sql_res) != 1:
            raise Exception(f"Several velib with code {code}")
        res = VelibSql(sql_res[0][1], sql_res[0][2])
        cur.close()
        return res

    def get_last_station_for_all_velib(self) -> Dict[int, int]:
        cur = self.conn.cursor()
        cur.execute(GET_LAST_STATION_FOR_ALL_VELIB)
        sql_res = cur.fetchall()
        res = {}
        for row in sql_res:
            res[row[0]] = row[1]
        cur.close()
        return res

    def get_all_stations_code(self) -> Set[int]:
        cur = self.conn.cursor()
        cur.execute(SELECT_ALL_STATION)
        sql_res = cur.fetchall()
        res = set()
        for row in sql_res:
            res.add(row[1])
        cur.close()
        return res

    async def get_velib_by_code_async(self, code: int) -> Optional[VelibSql]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SELECT_VELIB_BY_CODE, (code,))
                sql_res = await cur.fetchall()

                if sql_res is None or len(sql_res) == 0:
                    return None
                if len(sql_res) != 1:
                    raise Exception(f"Several velib with code {code}")
                res = VelibSql(sql_res[0][1], sql_res[0][2])
                await cur.close()
                return res

    def get_station_by_code(self, code: int) -> Optional[StationSql]:
        cur = self.conn.cursor()
        cur.execute(SELECT_STATION_BY_CODE, (code,))
        sql_res = cur.fetchall()
        if sql_res is None or len(sql_res) == 0:
            return None
        if len(sql_res) != 1:
            raise Exception(f"Several statiob with code {code}")
        res = StationSql(sql_res[0][0], sql_res[0][1], sql_res[0][2], code)
        cur.close()
        return res

    async def get_station_by_code_async(self, code: int) -> Optional[StationSql]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SELECT_STATION_BY_CODE, (code,))
                sql_res = await cur.fetchall()
                if sql_res is None or len(sql_res) == 0:
                    return None
                if len(sql_res) != 1:
                    raise Exception(f"Several statiob with code {code}")
                res = StationSql(sql_res[0][0], sql_res[0][1], sql_res[0][2], code)
                await cur.close()
                return res

    def get_last_velib_docked(self, velib_code: int) -> Optional[VelibDockedSql]:
        cur = self.conn.cursor()
        cur.execute(GET_LAST_DOCKED_STATION_FOR_VELIB, (velib_code,))
        sql_res = cur.fetchall()
        if sql_res is None or len(sql_res) == 0:
            return None
        if len(sql_res) != 1:
            raise Exception(f"Bug in the query for velib code {velib_code}")
        res = VelibDockedSql(velib_code, sql_res[0][0], sql_res[0][1])
        cur.close()
        return res

    async def get_last_velib_docked_async(self, velib_code: int) -> Optional[VelibDockedSql]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(GET_LAST_DOCKED_STATION_FOR_VELIB, (velib_code,))
                sql_res = await cur.fetchall()
                if sql_res is None or len(sql_res) == 0:
                    return None
                if len(sql_res) != 1:
                    raise Exception(f"Bug in the query for velib code {velib_code}")
                res = VelibDockedSql(velib_code, sql_res[0][0], sql_res[0][1])
                await cur.close()
                return res

    def insert_velib(self, velib_code: int, is_electric: bool, run_id: int):
        logging.debug(f"Inserting velib {velib_code} in DB")
        cur = self.conn.cursor()
        cur.execute(INSERT_VELIB, (velib_code, is_electric, run_id))
        self.conn.commit()
        cur.close()

    async def insert_velib_async(self, velib_code: int, is_electric: bool, run_id: int):
        if velib_code == 41260:
            print("Stop")
        logging.debug(f"Inserting velib {velib_code} in DB")
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(INSERT_VELIB, (velib_code, is_electric, run_id))
                    await conn.commit()
                    await cur.close()
                except Exception as e:
                    print(e)

    def insert_station(self, station_name: str, long: float, lat: float, code: int, run_id: int):
        logging.debug(f"Inserting station {station_name}/{code} in DB")
        cur = self.conn.cursor()
        cur.execute(INSERT_STATION, (station_name, long, lat, code, run_id))
        self.conn.commit()
        cur.close()

    async def insert_station_async(self, station_name: str, long: float, lat: float, code: int, run_id: int):
        logging.debug(f"Inserting station {station_name}/{code} in DB")
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(INSERT_STATION, (station_name, long, lat, code, run_id))
                await conn.commit()
                await cur.close()

    def insert_docked(self, velib_code: int, timestamp: datetime, station_code: int, run_id: int, available: bool):
        logging.debug(f"Inserting docked velib:{velib_code} station:{station_code} in DB")
        cur = self.conn.cursor()
        cur.execute(INSERT_VELIB_DOCKED, (velib_code, timestamp, station_code, run_id, available))
        self.conn.commit()
        cur.close()

    async def insert_docked_async(self, velib_code: int, timestamp: datetime, station_code: int, run_id: int,
                                  available: bool):
        logging.debug(f"Inserting docked velib:{velib_code} station:{station_code} in DB")
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(INSERT_VELIB_DOCKED, (velib_code, timestamp, station_code, run_id, available))
                await conn.commit()
                await cur.close()

    def insert_rating(self, velib_code: int, rate: int, rate_time: datetime, run_id: int):
        cur = self.conn.cursor()
        cur.execute(INSERT_RATING, (velib_code, rate, rate_time, run_id))
        self.conn.commit()
        cur.close()

    def insert_run(self, time: datetime) -> int:
        logging.debug(f"Inserting run {time} in DB")
        cur = self.conn.cursor()
        cur.execute(INSERT_RUN, (time,))
        self.conn.commit()
        id = cur.fetchone()[0]
        cur.close()
        return id
