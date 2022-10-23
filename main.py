import logging
import sys

from velib_api_service import VelibApiService

logging.basicConfig(level="INFO")


def main():
    # res = VelibApiService().get_station("Quai de l'Horloge - Pont Neuf")
    velib_api = VelibApiService()
    velib_api.get_station("Quai de l'Horloge - Pont Neuf")
    # stations = velib_api.get_all_station()
    # size = len(stations)
    # i = 0
    # for station in stations:
    #     station_name = station["station"]["name"]
    #     print(f"{station_name} {i}/{size}")
    #     i += 1
    #     velib_api.get_station(station["station"]["name"])


if __name__ == '__main__':
    sys.exit(main())  # next section explains the use of sys.exit
