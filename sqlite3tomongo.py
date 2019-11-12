#!/usr/bin/env python3
""" sqlite3import.py
    ~~~~~~~~~~~~~~~~

    Import SQLite3 database files into mongod instance.
"""
import json
import logging
import os
from pymongo import MongoClient
import sqlite3
import sys

logging.basicConfig(
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    level=os.environ.get('LOGLEVEL', 'WARNING').upper(),
)
logger = logging.getLogger(__name__)


def load_sql(db_path):
    """Load SQLite3 data."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    tables = c.fetchall()

    data = {
        'db': os.path.splitext(os.path.basename(db_path))[0],
        'collections': {}
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
    conn.close()

    return data

def dump_mongo(data, uri=None):
    """ Dump collection data into mongod. """
    if 'db' not in data:
        print('error: database name missing in data.')
        return 1
    if 'collections' not in data:
        print('error: collections missing in data.')
        return 1

    uri = uri or 'mongodb://localhost:27017'

    client = MongoClient(uri)
    db = client[data['db']]
    for coll_name, docs in data['collections'].items():
        coll = db[coll_name]
        coll.drop()
        coll.insert_many(docs)

        print(f"Inserted {coll.count_documents({})} docs into {data['db']}.{coll_name} on {uri}")

def main(argv):
    try:
        if len(argv) < 2:
            print(f"usage: {argv[0]} [database file] 'mongod uri'")
            exit(1)
        db_path = os.path.abspath(argv[1])
        if not os.path.isfile(db_path):
            print(f'{argv[1]} file not found.')
            exit(1)
        uri = argv[2] if len(argv) > 2 else None

        # load and dump data (sql to mongo)
        data = load_sql(db_path)
        dump_mongo(data, uri)

    except Exception:
        logger.exception('Something went wrong.')

if __name__ == '__main__':
    main(sys.argv)
