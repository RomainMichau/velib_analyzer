import argparse
import logging
import sys

from services.sql_service import SqlService
from db_exporter import DB_export
from services.velib_api_service import VelibApiService


def main():
    args = parse_args()
    sql = SqlService(args.db_hostname, args.db_name, args.db_user, args.db_password, args.db_port)
    velib_api = VelibApiService(args.token)
    export = DB_export(sql, velib_api)
    export.run()


def parse_args():
    parser = argparse.ArgumentParser(description='Query velib api')
    parser.add_argument('--api_token', '-t', dest="token", type=str, required=True)
    parser.add_argument('--log', '-l', dest="log_level", type=str, default="INFO")
    parser.add_argument('-v', action='store_true', dest="verbose", help="enable DEBUG log mode", default=False)
    parser.add_argument('--db_hostname', dest='db_hostname', type=str,
                        help='db hostname', required=True)
    parser.add_argument('--db_password', dest='db_password', type=str,
                        help='db password', required=True)
    parser.add_argument('--db_user', dest='db_user', type=str,
                        help='db user. Need rw access', required=True)
    parser.add_argument('--db_port', dest='db_port', type=int,
                        help='db port', default="5432")
    parser.add_argument('--db_name', dest='db_name', type=str,
                        help='db name', default="tricount")
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level='DEBUG')
    else:
        logging.basicConfig(level=args.log_level.upper())
    return args


if __name__ == '__main__':
    sys.exit(main())
