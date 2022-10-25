from datetime import datetime
from typing import Optional
import asyncio
import psycopg
from psycopg import AsyncConnection
from psycopg_pool import ConnectionPool, AsyncConnectionPool
from entities import Velib, Station, VelibDocked

SELECT_ALL_VELIB_CODE = """
SELECT velib_code FROM public.velibs
"""

SELECT_ALL_STATION = """
SELECT id, station_name, long, lat
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
(velib_code, electric)
VALUES(%s, %s);
"""

INSERT_RATING = """
INSERT INTO public.rating
(velib_code, rate, rate_time)
VALUES(%s, %s, %s);
"""

INSERT_RUN = """
INSERT INTO public.run
(run_time)
VALUES(%s);
"""

INSERT_STATION = """
INSERT INTO public.stations
(station_name, long, lat, station_code)
VALUES(%s, %s, %s, %s);
"""

INSERT_VELIB_DOCKED = """
INSERT INTO public.velib_docked
(velib_code, timestamp, station_code)
VALUES(%s, %s, %s);
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
                , min_size=4, max_size=50,
                timeout=100)
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

    def get_velib_by_code(self, code: int) -> Optional[Velib]:
        cur = self.conn.cursor()
        cur.execute(SELECT_VELIB_BY_CODE, (code,))
        sql_res = cur.fetchall()
        if sql_res is None or len(sql_res) == 0:
            return None
        if len(sql_res) != 1:
            raise Exception(f"Several velib with code {code}")
        res = Velib(sql_res[0][1], sql_res[0][2])
        cur.close()
        return res

    def get_station_by_code(self, code: str) -> Optional[Station]:
        cur = self.conn.cursor()
        cur.execute(SELECT_STATION_BY_CODE, (code,))
        sql_res = cur.fetchall()
        if sql_res is None or len(sql_res) == 0:
            return None
        if len(sql_res) != 1:
            raise Exception(f"Several statiob with code {code}")
        res = Station(sql_res[0][0], sql_res[0][1], sql_res[0][2], code)
        cur.close()
        return res

    async def get_station_by_code_async(self, code: str) -> Optional[Station]:
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(SELECT_STATION_BY_CODE, (code,))
                sql_res = await cur.fetchall()
                if sql_res is None or len(sql_res) == 0:
                    return None
                if len(sql_res) != 1:
                    raise Exception(f"Several statiob with code {code}")
                res = Station(sql_res[0][0], sql_res[0][1], sql_res[0][2], code)
                await cur.close()
                return res

    def get_last_velib_docked(self, velib_code: int) -> Optional[VelibDocked]:
        cur = self.conn.cursor()
        cur.execute(GET_LAST_DOCKED_STATION_FOR_VELIB, (velib_code,))
        sql_res = cur.fetchall()
        if sql_res is None or len(sql_res) == 0:
            return None
        if len(sql_res) != 1:
            raise Exception(f"Bug in the query for velib code {velib_code}")
        res = VelibDocked(velib_code, sql_res[0][0], sql_res[0][1])
        cur.close()
        return res

    def insert_velib(self, velib_code: int, is_electric: bool):
        cur = self.conn.cursor()
        cur.execute(INSERT_VELIB, (velib_code, is_electric))
        self.conn.commit()
        cur.close()

    def insert_station(self, station_name: str, long: float, lat: float, code: str):
        cur = self.conn.cursor()
        cur.execute(INSERT_STATION, (station_name, long, lat, code))
        self.conn.commit()
        cur.close()

    async def insert_station_async(self, station_name: str, long: float, lat: float, code: str):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                print("INserting stuff2")
                await cur.execute(INSERT_STATION, (station_name, long, lat, code))
                await conn.commit()
                await cur.close()

    def insert_docked(self, velib_code: int, timestamp: datetime, station_code: str):
        cur = self.conn.cursor()
        cur.execute(INSERT_VELIB_DOCKED, (velib_code, timestamp, station_code))
        self.conn.commit()
        cur.close()

    async def insert_docked_async(self, velib_code: int, timestamp: datetime, station_code: str):
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                print("INserting stuff")
                await cur.execute(INSERT_VELIB_DOCKED, (velib_code, timestamp, station_code))
                await conn.commit()
                await cur.close()

    def insert_rating(self, velib_code: int, rate: int, rate_time: datetime):
        cur = self.conn.cursor()
        cur.execute(INSERT_RATING, (velib_code, rate, rate_time))
        self.conn.commit()
        cur.close()

    def insert_run(self, time: datetime):
        cur = self.conn.cursor()
        cur.execute(INSERT_RUN, (time,))
        self.conn.commit()
        cur.close()
