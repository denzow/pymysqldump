import sys
import argparse
import csv

import MySQLdb
from MySQLdb.cursors import DictCursor


def init():
    """
    arguments.
    """
    parser = argparse.ArgumentParser(
        description='help',
        formatter_class=argparse.RawTextHelpFormatter,
        add_help=False,
    )

    parser.add_argument(
        '-u',
        '--user',
        type=str,
        required=True,
        dest='user',
    )

    parser.add_argument(
        '-p',
        '--password',
        type=str,
        required=False,
        default='',
        dest='password',
        help='password'
    )

    parser.add_argument(
        '-P',
        '--port',
        type=int,
        required=False,
        default=3306,
        dest='port',
        help='port'
    )
    parser.add_argument(
        '-h',
        '--host',
        type=str,
        required=False,
        default='localhost',
        dest='host',
        help='host'
    )
    parser.add_argument(
        '-d',
        '--database',
        type=str,
        required=True,
        dest='database',
        help='databse'
    )
    parser.add_argument(
        '-q',
        '--query',
        type=str,
        required=True,
        dest='query',
        help='query'
    )

    return parser.parse_args()


def dump_query(host, port, user, password, database, query):
    # DBに接続しカーソルを取得する
    connect = MySQLdb.connect(
        host=host,
        port=port,
        user=user,
        passwd=password,
        db=database,
        charset='utf8'
    )
    cursor = connect.cursor(DictCursor)
    cursor.execute(query)
    headers = [meta[0] for meta in cursor.description]
    writer = None
    for row in cursor.fetchall():
        if not writer:
            writer = csv.DictWriter(sys.stdout, fieldnames=list(headers))
            writer.writeheader()
        writer.writerow(row)
    cursor.close()
    connect.close()


def main():
    args = init()
    dump_query(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        query=args.query,
    )


if __name__ == '__main__':
    main()
