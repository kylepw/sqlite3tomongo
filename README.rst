==============
sqlite3tomongo
==============

Simple script to import a SQLite3 database into MongoDB instance.

Requirements
------------
- Python 3.6+
- Running mongod instance

Installation
------------
::

    $ git clone git@github.com:kylepw/sqlite3tomongo.git && cd sqlite3tomongo
    $ python -m venv venv_sql3m && source venv_sql3m/bin/activate
    (venv_sql3m) $ pip install -r requirements.txt

Usage
-----
::

    (venv_sql3m) $ # Make sure a mongod instance is running.
    (venv_sql3m) $ python3 sqlite3tomongo.py test.db 'mongodb://localhost:27017'

or to view info logs: ::

    (venv_sql3m) $ LOGLEVEL=info python3 sqlite3tomongo.py test.db 'mongodb://localhost:27017'

License
-------
`MIT License <https://github.com/kylepw/twitterpeel/blob/master/LICENSE>`_
