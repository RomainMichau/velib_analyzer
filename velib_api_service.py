import logging

import cloudscraper


# @dataclasses
# class Station:

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

    def get_station(self, station_name: str):
        json = {
            'stationName': station_name,
            'disponibility': 'yes',
        }
        url = f"{self.__base_url}/secured/searchStation?disponibility=yes"
        res = self.__scraper.post(
            url, headers=self.__headers, json=json)
        if res.ok:
            logging.debug(f"Get station {station_name} success, Responded with: {res.text}")
        else:
            logging.error(f"Get station {station_name} failed. Responded with {res.status_code} / {res.text}")
            raise Exception(f"Get station {station_name} failed")
        return res.json()

    def get_all_station(self):
        data = {
            'disponibility': 'yes',
        }
        url = f" {self.__base_url}/map/details?zoomLevel=1&gpsTopLatitude=49.05546&gpsTopLongitude=2.662193&gpsBotLongitude=1.898879&gpsBotLatitude=48.572554&nbStations=0&bikes=yes"
        res = self.__scraper.get(
            url, headers=self.__headers, data=data)

        if res.ok:
            logging.debug(f"Get all stations success, Responded with: {res.text}")
        else:
            logging.error(f"Get all stations failed. Responded with {res.status_code} / {res.text}")
            raise Exception(f"Get all stations failed")
        return res.json()
