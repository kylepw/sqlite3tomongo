""" sqlt3import
    ~~~~~~~~~~~

    Import SQLite3 database into MongoDB database.

    Usage:
        $ sqlt3import data.db --host 'mongodb://mydatabase:27017'

    Notes:
        Database named after filename by default (i.e. `data` if filename
        is `data.db`). Host defaults to 'mongodb://localhost:27017' if none
        provided. Collections named after SQL tables. Collections are dropped
        before inserts by default. Use '--append' option to keep collections.

"""
import argparse
import json
import logging
import os
from pymongo import MongoClient
import sqlite3
import sys

__version__ = '0.1.0'

logging.basicConfig(
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    level=os.environ.get('LOGLEVEL', 'CRITICAL').upper(),
)
logger = logging.getLogger(__name__)


def load_sql(db_path):
    """Load SQLite3 data"""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tables = c.fetchall()

        data = {'db': os.path.splitext(os.path.basename(db_path))[0], 'collections': {}}
        for table in tables:
            c.execute(f"SELECT * FROM {table['name']}")
            rows = c.fetchall()
            keys = rows[0].keys()
            docs = []
            for row in rows:
                doc = {}
                for key in keys:
                    if key not in ('id'):
                        doc[key] = row[key]
                docs.append(doc)
            data['collections'][table['name']] = docs

        return data
    except Exception:
        print('Failed to load SQLite3 data. Check the logs.')
        raise
    finally:
        conn.close()


def dump_mongo(data, uri=None):
    """Dump collection data into mongod"""
    uri = uri or 'mongodb://localhost:27017'
    client = MongoClient(uri)
    try:
        if 'db' not in data:
            raise ValueError("'db' key missing in dictionary")
        if 'collections' not in data:
            raise ValueError("'collections' key missing in dictionary")
        db = client[data['db']]
        results = []
        for coll_name, docs in data['collections'].items():
            coll = db[coll_name]
            coll.drop()
            logger.info('Dropped %s collection from %s', coll_name, data['db'])
            coll.insert_many(docs)
            expected_count = len(data['collections'][coll_name])
            count = coll.count_documents({})
            result_str = '%s docs inserted into %s.%s' % (count, data['db'], coll_name)
            logger.info(result_str)
            assert count == expected_count, (
                "Only %s of %s docs imported into '%s' collection"
                % (count, expected_count, data['db'])
            )
            print(result_str)
    except Exception:
        print('Failed to import data into mongod. Check the logs.')
        raise
    finally:
        client.close()


def get_parser():
    parser = argparse.ArgumentParser(description='Import SQLite3 database into MongoDB')
    parser.add_argument('dbfile', metavar='file', help='sqlite3 database file')
    parser.add_argument(
        '--host',
        dest='uri',
        metavar='string',
        help='mongodb uri string',
        default='mongodb://localhost:27017',
    )
    parser.add_argument('-v', '--version', action='version', version=__version__)
    return parser


def main():
    parser = get_parser()
    args = vars(parser.parse_args())
    try:
        db_path = os.path.abspath(args['dbfile'])
        if not os.path.isfile(db_path):
            err_msg = f"Invalid file: {args['dbfile']}"
            print(err_msg)
            raise OSError(err_msg)

        # load and dump data (sql to mongo)
        data = load_sql(db_path)
        dump_mongo(data, args['uri'])
    except KeyboardInterrupt:
        print('\nBye!')
    except Exception:
        logger.exception('Something went wrong.')
    else:
        print('Done.')


if __name__ == '__main__':
    main()
