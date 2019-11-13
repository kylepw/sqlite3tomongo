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
import logging
import os
from pymongo import MongoClient
import sqlite3

__version__ = '0.1.0'

logging.basicConfig(
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    level=os.environ.get('LOGLEVEL', 'CRITICAL').upper(),
)
logger = logging.getLogger(__name__)


def load_sql(args):
    """Load SQLite3 data.

    Args:
        args (dict): command line arg values. 'dbfile' key value used.

    Returns:
        Dictionary with database name in 'db' and list of docs in
        'collections' grouped by collection (table) name.
    """
    conn = None
    try:
        db_path = os.path.abspath(args['dbfile'])
        if not os.path.isfile(db_path):
            raise OSError(f"Invalid file: {args['dbfile']}")

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tables = c.fetchall()

        data = {
            'db': args['dbname'] or os.path.splitext(os.path.basename(db_path))[0],
            'collections': {},
        }
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
            logger.info('`%s` table loaded with %s rows', table['name'], len(docs))

        return data
    except Exception:
        print('Failed to load SQLite3 data. Check the logs.')
        raise
    finally:
        if conn:
            conn.close()


def dump_mongo(data, args):
    """Dump collection data into mongod

    Args:
        data (dict): database name and collection docs.
            'db': database name
            'collections': list of docs grouped by collection names
        args (dict): command line arg values.
            'uri' (str): mongodb uri
            'append' (bool): whether to drop collection or not
    """
    client = MongoClient(args['uri'])
    try:
        if not isinstance(data, dict) or not isinstance(args, dict):
            raise ValueError('Arguments must be dict type')
        if 'db' not in data or not isinstance(data['db'], str):
            raise ValueError("'db' key value missing or invalid")
        if 'collections' not in data or not isinstance(data['collections'], dict):
            raise ValueError("'collections' key value missing or invalid")
        db = client[data['db']]
        for coll_name, docs in data['collections'].items():
            coll = db[coll_name]
            if not args['append']:
                coll.drop()
                logger.info('dropped %s collection from %s', coll_name, data['db'])
            expected_count = len(data['collections'][coll_name])
            count = len(coll.insert_many(docs).inserted_ids)
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
    parser = argparse.ArgumentParser(
        description='Import SQLite3 database into MongoDB.'
    )
    parser.add_argument('dbfile', metavar='file', help='sqlite3 database file')
    parser.add_argument(
        '--host',
        dest='uri',
        metavar='string',
        help='mongodb uri',
        default='mongodb://localhost:27017',
    )
    parser.add_argument(
        '--db',
        '--database',
        dest='dbname',
        metavar='string',
        help='mongod database name (defaults to filename)',
    )
    parser.add_argument(
        '-a',
        '--append',
        help='append to existing collections (dropped by default)',
        action='store_true',
        default=False,
    )
    parser.add_argument('-v', '--version', action='version', version=__version__)
    return parser


def main():
    parser = get_parser()
    args = vars(parser.parse_args())
    try:
        data = load_sql(args)
        dump_mongo(data, args)
    except KeyboardInterrupt:
        print('\nBye!')
    except Exception:
        logger.exception('Something went wrong.')
    else:
        print('Done.')


if __name__ == '__main__':
    main()
