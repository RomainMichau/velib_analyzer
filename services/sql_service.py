from datetime import datetime
from typing import Optional

import psycopg2

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
SELECT station_code, max("timestamp")
FROM public.velib_docked code where velib_code = %s group by station_code 
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
        self.conn = psycopg2.connect(
            host=hostname,
            database=db_name,
            user=username,
            password=password,
            port=port)

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

    def insert_docked(self, velib_code: int, timestamp: datetime, station_code: str):
        cur = self.conn.cursor()
        cur.execute(INSERT_VELIB_DOCKED, (velib_code, timestamp, station_code))
        self.conn.commit()
        cur.close()

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
