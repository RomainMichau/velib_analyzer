import argparse
import logging
import sys

from velib_api_service import VelibApiService

def main(velib_api_token: str):
    # res = VelibApiService().get_station("Quai de l'Horloge - Pont Neuf")
    velib_api = VelibApiService(velib_api_token)
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
    parser = argparse.ArgumentParser(description='Query velib api')
    parser.add_argument('--api_token', '-t', dest="token", type=str, required=True)
    parser.add_argument('--log', dest="log_level", type=str, default="INFO")
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level.upper())

    sys.exit(main(velib_api_token=args.token))  # next section explains the use of sys.exit
