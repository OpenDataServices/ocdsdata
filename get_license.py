import requests
import os
import sqlalchemy as sa
import json
import tempfile
import subprocess


def get_engine(schema=None, db_uri=None, pool_size=1):

    if not db_uri:
        db_uri = os.environ["DATABASE_URL"]

    connect_args = {}
    if schema:
        connect_args = {"options": f"-csearch_path={schema}"}

    return sa.create_engine(db_uri, pool_size=pool_size, connect_args=connect_args)


def get_stats():
    stats = requests.get('https://ocdsdata.fra1.digitaloceanspaces.com/metadata/stats.json').json()
    for source, value in stats.items():
        print(source)
        dump_url = value.get('pg_dump').get('url')
        if not dump_url:
            continue

        engine = get_engine()

        engine.execute(f'drop schema if exists {source} cascade; create schema {source}')

        try:
            with tempfile.TemporaryDirectory() as tmpdirname:
                subprocess.run(
                    ["wget", dump_url, "-q", "-O", f"{tmpdirname}/dump.pg_dump"],
                    check=True,
                )

                subprocess.run(
                    ["pg_restore", 
                     "-d", os.environ["DATABASE_URL"], 
                     "-n", source, 
                     "-t", "_package_data", 
                     f"{tmpdirname}/dump.pg_dump"],
                    check=True,
                )

                with engine.begin() as connection:

                    extension_result = connection.execute(
                        f"""
                        SELECT
                            extension
                        FROM
                            {source}._package_data, jsonb_array_elements(package_data -> 'extensions') extension
                        WHERE
                            jsonb_typeof(package_data -> 'extensions') = 'array' group by 1;
                    """
                    )
                    extensions = [row.extension for row in extension_result]

                    licence_result = connection.execute(
                        f"""
                        SELECT
                            package_data ->> 'license' as license
                        FROM
                            {source}._package_data
                        WHERE
                            package_data ->> 'license' is not null
                        GROUP BY 1;
                    """
                    )
                    licences = [row.license for row in licence_result]

                    publisher_result = connection.execute(
                        f"""
                        SELECT
                            package_data -> 'publisher' -> 'name' as name,
                            package_data -> 'publisher' -> 'uri' as uri
                        FROM
                            {source}._package_data
                        GROUP BY 1, 2;
                    """
                    )
                    publishers = [[row.name, row.uri] for row in publisher_result]

                    package_info = {"extensions": extensions, "licences": licences, "publishers": publishers}

                    os.makedirs(f'package_info/{source}', exist_ok=True)
                    with open(f'package_info/{source}/package_info.json', 'w+') as f:
                        json.dump(package_info, f)
                    print(package_info)
        finally:
            engine.execute(f'drop schema if exists {source} cascade')

