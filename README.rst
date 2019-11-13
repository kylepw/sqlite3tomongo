==============
sqlite3tomongo
==============

Script to import SQLite3 database into MongoDB database.

Requirements
------------
- Python 3.6+
- Running mongod instance

Installation
------------
::

    $ git clone git@github.com:kylepw/sqlite3tomongo.git && cd sqlite3tomongo
    $ pip3 install pipenv
    $ pipenv install && pipenv shell

Usage
-----
::

    $ python3 sqlt3tomongo.py -h
    usage: sqlt3tomongo.py [-h] [--host string] [--db string] [-a] [-v] file

    Import SQLite3 database into MongoDB.

    positional arguments:
    file                  sqlite3 database file

    optional arguments:
    -h, --help            show this help message and exit
    --host string         mongodb uri
    --db string, --database string
                            mongod database name (defaults to filename)
    -a, --append          append to existing collections (dropped by default)
    -v, --version         show program's version number and exit

To view logs: ::

    $ LOGLEVEL=info python3 sqlt3tomongo.py yoursqldb.db --host 'mongodb://yourmongodb:27017'

Testing
-------
::

    $ python3 -m unittest test_sqlt3mongo.py

License
-------
`MIT License <https://github.com/kylepw/twitterpeel/blob/master/LICENSE>`_
