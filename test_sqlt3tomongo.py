"""Tests for sqlt3tomongo script"""
import argparse
from io import StringIO
import logging
import os
import sqlite3
from sqlt3tomongo import dump_mongo, get_parser, load_sql, logger, MongoClient
import sys
import unittest
from unittest.mock import patch

# Suppress prints
sys.stdout = StringIO()

class TestDumpMongo(unittest.TestCase):
    def setUp(self):
        self.data = {
            'db': 'company',
            'collections':
                {'people': [
                        {'name': 'Jimmy', 'age': 19},
                        {'name': 'Calico', 'age': 54},
                        {'name': 'Sandra', 'age': 88}
                ]}
        }
        self.args = vars(get_parser().parse_args())

    def test_missing_db_value_in_data(self):
        del self.data['db']
        with self.assertRaises(ValueError):
            dump_mongo(self.data, self.args)

    def test_missing_collections_value_in_data(self):
        del self.data['collections']
        with self.assertRaises(ValueError):
            dump_mongo(self.data, self.args)

    @patch('sqlt3tomongo.MongoClient', spec=True)
    def test_drop_called_when_append_option_is_false(self, mock_mongo_client):
        # Assure inserted count matches expected count.
        for coll in self.data['collections']:
            mock_mongo_client.return_value[self.data['db']][coll].insert_many.return_value.inserted_ids = [1] * len(self.data['collections'][coll])

        dump_mongo(self.data, self.args)
        for coll in self.data['collections']:
            mock_mongo_client()[self.data['db']][coll].drop.assert_called_once()

    @patch('sqlt3tomongo.MongoClient', spec=True)
    def test_drop_not_called_when_append_option_is_true(self, mock_mongo_client):
        # Assure inserted count matches expected count.
        for coll in self.data['collections']:
            mock_mongo_client.return_value[self.data['db']][coll].insert_many.return_value.inserted_ids = [1] * len(self.data['collections'][coll])

        dump_mongo(self.data, self.args)
        for coll in self.data['collections']:
            mock_mongo_client()[self.data['db']][coll].drop.assert_called_once()




class TestGetParser(unittest.TestCase):
    def setUp(self):
        pass

class TestLoadSql(unittest.TestCase):
    def setUp(self):
        pass

if __name__ == '__main__':
    unittest.main()