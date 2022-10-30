import logging
from typing import List

import cloudscraper

from entities.velib_api_entities import VelibApi, StationApi


class VelibApiService:
    def __init__(self, token: str):
        self.__scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        self.__headers = {
            "Authorization": f"Basic {token}",
            "accept-encoding": "gzip",
            "user-agent": "okhttp/3.12.12"

        }
        self.__base_url = "https://www.velib-metropole.fr/api"

    def get_velib_at_station(self, station_name: str) -> List[VelibApi]:
        json = {
            'stationName': station_name,
            'disponibility': 'yes',
        }
        url = f"{self.__base_url}/secured/searchStation?disponibility=yes"
        response = self.__scraper.post(
            url, headers=self.__headers, json=json)
        if response.ok:
            logging.debug(f"Get station {station_name} success, Responded with: {response.text}")
        else:
            logging.error(f"Get station {station_name} failed. Responded with {response.status_code} / {response.text}")
            raise Exception(f"Get station {station_name} failed")
        res = []
        # Some station name are used twice (y tho)
        stations = response.json()
        for station in stations:
            # The API will return all station having a name starting with `station_name` (y tho).
            # I only want exact match
            if station['station']['name'] != station_name:
                continue
            for velib in station["bikes"]:
                code = int(velib["bikeName"])
                is_electric = velib["bikeElectric"] == "yes"
                is_available = velib["bikeStatus"] == "disponible"
                res.append(VelibApi(code, is_electric, is_available))
        return res

    def get_all_station(self) -> List[StationApi]:
        data = {
            'disponibility': 'yes',
        }
        url = f" {self.__base_url}/map/details?zoomLevel=1&gpsTopLatitude=49.05546&gpsTopLongitude=2.662193&gpsBotLongitude=1.898879&gpsBotLatitude=48.572554&nbStations=0&bikes=yes"
        response = self.__scraper.get(
            url, headers=self.__headers, data=data)
        names = set()
        if response.ok:
            logging.debug(f"Get all stations success, Responded with: {response.text}")
        else:
            logging.error(f"Get all stations failed. Responded with {response.status_code} / {response.text}")
            raise Exception(f"Get all stations failed")
        res = []
        stations = response.json()
        for station in stations:
            name = station["station"]["name"]
            # Some stations are duplicated in Velib API
            if name in names:
                continue
            names.add(name)
            if "_relais" in station["station"]["code"]:
                continue
            code = int(station["station"]["code"])
            longitude = station["station"]["gps"]["longitude"]
            latitude = station["station"]["gps"]["latitude"]
            res.append(StationApi(name, longitude, latitude, code))
        return res
