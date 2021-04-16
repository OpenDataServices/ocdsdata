OCDS Data
---------

Trying to make getting OCDS data as easy as possible, in any format you want.

For more informaion see https://ocds-downloads.opendata.coop/ which uses this tool to create all the downloads.

Contains a CLI tool to download OCDS data into a PostgreSQL database and an Airflow pipline to make exports of this database to various formats.


CLI Installation
----------------

Clone this repo.

```
pip install -r requirements.txt
```

A PostgreSQL database is needed with a [connection string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) to it.

Set environment variable DATABASE_URL as the connection string)

```
export DATABASE_URL=postgresql://user:password@localhost/dbname
```

CLI Use
-------

Only one CLI command is needed:

```
python ocdsdata.py import <scraper> <schema>
```

This will scrape data from a <scraper> defined in [Kingfisher Collect Documentation](https://kingfisher-collect.readthedocs.io/en/latest/spiders.html) and import it into a the PostgreSQL <schema>.  For example to import zambia OCDS data into a PostgreSQL schema named `my_zambia_data` run:

```
python ocdsdata.py import zambia my_zambia_data
```

This process may take a long time.


Airflow Installation
--------------------

TODO

Airflow Use
-----------

TODO


Developers
----------
To update requirements run:

pip-compile requirements.in kingfisher-collect/requirements.txt --output-file=requirements.txt
