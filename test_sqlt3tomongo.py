"""Tests for sqlt3tomongo script"""
import argparse
from io import StringIO
from sqlt3tomongo import __version__, dump_mongo, get_parser, load_sql
import sys
import unittest
from unittest.mock import patch

# Suppress prints
sys.stdout = StringIO()


class TestDumpMongo(unittest.TestCase):
    def setUp(self):
        patcher_logger = patch('sqlt3tomongo.logger')
        patcher_mongo_client = patch('sqlt3tomongo.MongoClient')
        self.mock_logger = patcher_logger.start()
        self.mock_mongo_client = patcher_mongo_client.start()

        self.args = vars(get_parser().parse_args(['test.db']))
        self.data = {
            'db': 'company',
            'collections': {
                'people': [
                    {'name': 'Jimmy', 'age': 19},
                    {'name': 'Calico', 'age': 54},
                    {'name': 'Sandra', 'age': 88},
                ]
            },
        }
        # Assure inserted doc count matches expected count.
        for coll in self.data['collections']:
            self.mock_mongo_client.return_value[self.data['db']][
                coll
            ].insert_many.return_value.inserted_ids = [1] * len(
                self.data['collections'][coll]
            )

        self.addCleanup(patch.stopall)

    def test_missing_db_value_in_data(self):
        del self.data['db']
        with self.assertRaises(ValueError):
            dump_mongo(self.data, self.args)

    def test_missing_collections_value_in_data(self):
        del self.data['collections']
        with self.assertRaises(ValueError):
            dump_mongo(self.data, self.args)

    def test_drop_called_when_append_option_is_false(self):
        dump_mongo(self.data, self.args)
        for coll in self.data['collections']:
            self.mock_mongo_client()[self.data['db']][coll].drop.assert_called()

    def test_drop_not_called_when_append_option_is_true(self):
        self.args['append'] = True
        dump_mongo(self.data, self.args)
        for coll in self.data['collections']:
            self.mock_mongo_client()[self.data['db']][coll].drop.assert_not_called()

    def test_info_logs_are_called(self):
        dump_mongo(self.data, self.args)
        self.mock_logger.info.assert_called()


class TestGetParser(unittest.TestCase):
    def test_no_file(self):
        with self.assertRaises(SystemExit):
            # Suppress error message.
            with patch('sys.stderr'):
                get_parser().parse_args([])

    def test_version_only(self):
        with self.assertRaises(SystemExit):
            get_parser().parse_args(['-v'])

    def test_file_only(self):
        expected = {
            'dbfile': 'test.db',
            'uri': 'mongodb://localhost:27017',
            'append': False,
            'dbname': None,
        }
        self.assertEqual(vars(get_parser().parse_args(['test.db'])), expected)


class TestLoadSql(unittest.TestCase):
    def setUp(self):
        patcher_logger = patch('sqlt3tomongo.logger')
        patcher_is_file = patch('sqlt3tomongo.os.path.isfile', return_value=True)
        patcher_sqlite3 = patch('sqlt3tomongo.sqlite3')
        self.mock_logger = patcher_logger.start()
        self.mock_is_file = patcher_is_file.start()
        self.mock_sqlite3 = patcher_sqlite3.start()

        self.args = vars(get_parser().parse_args(['test.db']))

        self.addCleanup(patch.stopall)

    def test_invalid_file_raises_error(self):
        self.mock_is_file.return_value = False

        with self.assertRaises(OSError):
            load_sql(self.args)

    def test_sqlite3_methods_called(self):
        load_sql(self.args)
        self.mock_sqlite3.connect().cursor().execute.assert_called()
        self.mock_sqlite3.connect().cursor().fetchall.assert_called()


if __name__ == '__main__':
    unittest.main()
