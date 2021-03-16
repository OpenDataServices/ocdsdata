import click
import os
import tempfile
import functools
import datetime
import traceback
import json
from jsonref import JsonRef
import csv
from collections import Counter
import shutil
from ocdsextensionregistry import ProfileBuilder
import sys

import orjson
from pathlib import Path
import sqlalchemy as sa
from scrapy.utils.project import get_project_settings
from scrapy.spiderloader import SpiderLoader
from scrapy.crawler import CrawlerProcess
from scrapy import signals
from codetiming import Timer
import ocdsmerge


this_path = Path(__file__).parent.absolute()
collect_path = str((this_path / "kingfisher-collect").absolute())


@functools.lru_cache(None)
def get_engine(id=None, db_uri=None, pool_size=1):
    if not db_uri:
        db_uri = os.environ["DATABASE_URL"]

    connect_args = {}
    if id:
        connect_args = {"options": f"-csearch_path=process_job_{id}"}

    return sa.create_engine(db_uri, pool_size=pool_size, connect_args=connect_args)


def create_table(table, id, sql, **params):
    print(f"creating table {table}")
    t = Timer()
    t.start()
    engine = get_engine(id)
    with engine.connect() as con:
        con.execute(
            sa.text(
                f"""drop table if exists {table};
                        create table {table}
                        AS
                        {sql};"""
            ),
            **params,
        )
    t.stop()


def execute(connection, sql, **params):
    connection.execute(sa.text(sql), **params)


def update_process_table(id, **kwargs):
    connection = kwargs.pop("connection", None)
    if not connection:
        connection = get_engine()

    time_now = str(datetime.datetime.utcnow())
    kwargs["last_update"] = time_now

    set_parts = []
    for key in kwargs:
        set_parts.append(f"{key} = :{key}")
    set = ", ".join(set_parts)
    sql = f"update public.process_table set {set} where id=:id"
    connection.execute(sa.text(sql), id=id, **kwargs)


def refresh_database():
    engine = get_engine()

    engine.execute(
        """

    DROP TABLE IF EXISTS process_table;

    CREATE TABLE IF NOT EXISTS process_table(
                       id text,
                       schema text,
                       run_id text,
                       name text,
                       state text,
                       scrape_info JSONB null,
                       time_started timestamp null,
                       last_update timestamp null,
                       time_finished timestamp null
                      );
    """
    )


@click.group()
def cli():
    pass


@cli.command()
def refresh_db():
    refresh_database()
    click.echo("refreshed database")


@cli.command()
def export_scrapers():
    os.chdir(collect_path)
    settings = get_project_settings()
    sl = SpiderLoader.from_settings(settings)
    click.echo(json.dumps(sl.list()))


@cli.command()
@click.argument("name")
def run_all(name):
    scrape(name, name, name)
    create_base_tables(name)
    compile_releases(name)
    release_objects(name)
    schema_analysis(name)


@cli.command('scrape')
@click.argument("name")
@click.argument("id")
@click.argument("run_id")
def _scrape(name, id, run_id):
    scrape()


def scrape(name, id, run_id):
    data_dir = this_path / "data" / id
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_file_path = data_dir / "all.csv"

    engine = get_engine()
    with engine.begin() as connection:
        connection.execute(
            f"""drop schema if exists process_job_{id} cascade;
                create schema process_job_{id};
                create table process_job_{id}.scrape_data(name TEXT, url TEXT, data_type TEXT, file_name TEXT,
                                                          valid BOOLEAN, data JSONB, error_data TEXT);
                create table process_job_{id}.job_info(name TEXT, info JSONB, logs TEXT);
            """
        )
        time_now = str(datetime.datetime.utcnow())
        connection.execute(sa.text("""DELETE FROM process_table where id=:id"""), id=id)
        connection.execute(
            sa.text(
                """INSERT INTO process_table (id, schema, run_id, name, state, time_started, last_update)
                 values (:id, :schema, :run_id, :name, :state, :time_now , :time_now)"""
            ),
            id=id,
            schema=f"process_job_{id}",
            name=name,
            run_id=run_id,
            state="scrape-started",
            time_now=time_now,
        )

    os.environ["FILES_STORE"] = str(data_dir.absolute())

    os.chdir(collect_path)
    settings = get_project_settings()
    settings["LOG_FILE"] = str(data_dir / "all.log")

    with open(str(csv_file_path), "w+", newline="") as csv_file:

        csv_writer = csv.writer(csv_file)

        count_data_types = Counter()

        def save_to_csv(item, spider):
            if "error" in item:
                print(item)
                return
            try:
                if isinstance(item["data"], dict):
                    data = orjson.dumps(item["data"], default=str)
                    valid = "t"
                    error_data = ""
                else:
                    try:
                        orjson.loads(item["data"])
                        data = item["data"]
                        valid = "t"
                        error_data = ""
                    except orjson.JSONDecodeError:
                        valid = "f"
                        data = "{}"
                        error_data = item["data"]

                if isinstance(data, bytes):
                    data = data.decode(item["encoding"])

                count_data_types.update([item["data_type"]])

                csv_writer.writerow(
                    [
                        name,
                        item["url"],
                        item["data_type"],
                        item["file_name"],
                        valid,
                        data,
                        error_data,
                    ]
                )
                print([item["url"], item["data_type"], item["file_name"]])
            except Exception:
                traceback.print_exc()
                raise

        runner = CrawlerProcess(settings)
        crawler = runner.create_crawler(name)

        crawler.signals.connect(save_to_csv, signal=signals.item_scraped)

        runner.crawl(crawler)
        runner.start()

    info = crawler.stats.spider_stats[name]
    info["name"] = name
    info["data_types"] = dict(count_data_types)
    info_file = data_dir / "info.json"
    info_data = json.dumps(info, default=str)
    info_file.write_text(info_data)

    log_text = (data_dir / "all.log").read_text()

    with engine.begin() as connection, open(str(csv_file_path)) as f:
        connection.execute(
            sa.text(f"insert into process_job_{id}.job_info values (:name, :info, :logs)"),
            name=name,
            info=info_data,
            logs=log_text,
        )

        dbapi_conn = connection.connection
        copy_sql = f"COPY process_job_{id}.scrape_data FROM STDIN WITH CSV"
        cur = dbapi_conn.cursor()
        cur.copy_expert(copy_sql, f)

        result = connection.execute(f"select count(*) from process_job_{id}.scrape_data").first()

        print(f"{result.count} files scraped")
        if result.count == 0:
            print("No data scraped!")
            sys.exit(1)

        update_process_table(id, connection=connection, state="scrape-finished", scrape_info=info_data)

    shutil.rmtree(data_dir)


@cli.command('create-base-tables')
@click.argument("id")
def _create_base_tables(id):
    create_base_tables(id)


def create_base_tables(id):
    engine = get_engine(id)

    update_process_table(id, state="create-tables-started")

    package_data_sql = """
       select 
           distinct md5((data - 'releases' - 'records')::text), 
           data - 'releases' - 'records' package_data
       from 
           scrape_data 
       where 
           data_type in ('release_package', 'record_package')
    """
    create_table("package_data", id, package_data_sql)

    engine.execute(
        """
        drop sequence if exists generated_release_id;
        create sequence generated_release_id;
        """
    )

    compiled_releases_sql = f"""
       select
           min(nextval('generated_release_id')) compiled_release_id,
           name, 
           url, 
           data_type, 
           file_name, 
           null package_data,
           coalesce(release ->> 'ocid', gen_random_uuid()::text) ocid,
           jsonb_agg(release) release_list,
           null rest_of_record,
           null compiled_release,
           null compile_error
       from 
           scrape_data,
           jsonb_path_query(data, '$.releases[*]') release
       where 
           data_type in ('release_package')

       group by name, url, data_type, file_name, coalesce(release ->> 'ocid', gen_random_uuid()::text)

       union all

       select
           nextval('generated_release_id'),
           name, 
           url, 
           data_type, 
           file_name, 
           to_jsonb(md5((data - 'releases' - 'records')::text)) package_data,
           record ->> 'ocid',
           record -> 'releases',
           record - 'compiledRelease' rest_of_record,
           record -> 'compiledRelease' compiled_release,
           null compile_error
       from 
           scrape_data,
           jsonb_path_query(data, '$.records[*]') record
       where 
           data_type in ('record_package')
    """

    create_table("compiled_releases", id, compiled_releases_sql)

    update_process_table(id, state="create-tables-finished")


@cli.command('compile-releases')
@click.argument("id")
def _compile_releases(id):
    compile_releases(id)


def compile_releases(id):
    with tempfile.TemporaryDirectory() as tmpdirname:
        csv_file_path = tmpdirname + "/compiled_release.csv"

        engine = get_engine(id)
        update_process_table(id, state="compile-releases-started")

        engine.execute(
            """
            drop table if exists tmp_compiled_releases;
            create table tmp_compiled_releases(compiled_release_id bigint, compiled_release JSONB, compile_error TEXT) 
        """
        )

        merger = ocdsmerge.Merger()
        results = engine.execute(
            f"select compiled_release_id, release_list from process_job_{id}.compiled_releases where compiled_release is null"
        )

        print("Making CSV file")
        with open(str(csv_file_path), "w+", newline="") as csv_file, Timer():
            csv_writer = csv.writer(csv_file)
            for num, result in enumerate(results):
                try:
                    compiled_release = merger.create_compiled_release(result.release_list)
                    error = ""
                except Exception as e:
                    compiled_release = {}
                    error = str(e)
                csv_writer.writerow([result.compiled_release_id, json.dumps(compiled_release), error])

        print("Importing file")
        with engine.begin() as connection, Timer(), open(str(csv_file_path)) as f:
            dbapi_conn = connection.connection
            copy_sql = "COPY tmp_compiled_releases FROM STDIN WITH CSV"
            cur = dbapi_conn.cursor()
            cur.copy_expert(copy_sql, f)

        print("Updating table")
        with engine.begin() as connection, Timer():
            connection.execute(
                """update compiled_releases cr 
                   SET compiled_release = tmp.compiled_release,
                       compile_error = tmp.compile_error
                   FROM tmp_compiled_releases tmp
                   WHERE tmp.compiled_release_id = cr.compiled_release_id"""
            )
            connection.execute(
                """
                drop table if exists tmp_compiled_releases;
            """
            )

        update_process_table(id, state="compile-releases-finished")


EMIT_OBJECT_PATHS = [
    ("planning",),
    ("tender",),
    ("contracts", "implementation"),
    ("buyer",),
    ("tender", "procuringEntity"),
]

PARTIES_PATHS = [
    "buyer",
    "awards.suppliers",
    "tender.procuringEntity",
    "tender.tenderers",
]


def flatten_object(obj, current_path=""):
    for key, value in list(obj.items()):
        if isinstance(value, dict):
            yield from flatten_object(value, f"{current_path}{key}.")
        else:
            yield f"{current_path}{key}", value


def traverse_object(obj, emit_object, full_path=tuple(), no_index_path=tuple()):
    for key, value in list(obj.items()):
        if isinstance(value, list) and value and isinstance(value[0], dict):
            for num, item in enumerate(value):
                if not isinstance(item, dict):
                    item = {"__error": "A non object is in array of objects"}
                yield from traverse_object(item, True, full_path + (key, num), no_index_path + (key,))
            obj.pop(key)
        elif isinstance(value, list):
            if not all(isinstance(item, str) for item in value):
                object[key] = json.dumps(value)
        elif isinstance(value, dict):
            if no_index_path + (key,) in EMIT_OBJECT_PATHS:
                yield from traverse_object(value, True, full_path + (key,), no_index_path + (key,))
                obj.pop(key)
            else:
                yield from traverse_object(value, False, full_path + (key,), no_index_path + (key,))

    if obj and emit_object:
        yield obj, full_path, no_index_path


@functools.lru_cache(1000)
def path_info(full_path, no_index_path):
    all_paths = []
    for num, part in enumerate(full_path):
        if isinstance(part, int):
            all_paths.append(full_path[: num + 1])

    parent_paths = all_paths[:-1]
    path_key = all_paths[-1] if all_paths else []

    object_key = ".".join(str(key) for key in path_key)
    parent_keys_list = [".".join(str(key) for key in parent_path) for parent_path in parent_paths]
    parent_keys_no_index = [
        ".".join(str(key) for key in parent_path if not isinstance(key, int)) for parent_path in parent_paths
    ]
    object_type = ".".join(str(key) for key in no_index_path) or "release"
    parent_keys = (dict(zip(parent_keys_no_index, parent_keys_list)),)
    return object_key, parent_keys_list, parent_keys_no_index, object_type, parent_keys


def create_rows(result):
    rows = []
    awards = {}
    parties = {}
    for object, full_path, no_index_path in traverse_object(result.compiled_release, 1):

        (
            object_key,
            parent_keys_list,
            parent_keys_no_index,
            object_type,
            parent_keys,
        ) = path_info(full_path, no_index_path)

        object["_link"] = f'{result.compiled_release_id}{"." if object_key else ""}{object_key}'
        object["_link-release"] = str(result.compiled_release_id)
        for no_index_path, full_path in zip(parent_keys_no_index, parent_keys_list):
            object[f"_link-{no_index_path}"] = f"{result.compiled_release_id}.{full_path}"

        row = dict(
            compiled_release_id=result.compiled_release_id,
            object_key=object_key,
            parent_keys=parent_keys,
            object_type=object_type,
            object=object,
        )

        rows.append(row)
        if object_type == "awards":
            award_id = object.get("id")
            if award_id:
                awards[award_id] = object
        if object_type == "parties":
            parties_id = object.get("id")
            parties[parties_id] = object

    for row in rows:
        object = row["object"]
        if row["object_type"] in PARTIES_PATHS:
            parties_id = object.get("id")
            if parties_id:
                party = parties.get(parties_id)
                if party:
                    object["_link-party"] = party["_link"]
                    object["_party"] = {key: value for key, value in party.items() if not key.startswith("_")}
        if row["object_type"] == "contracts":
            award_id = object.get("awardID")
            if award_id:
                award = awards.get(award_id)
                if award:
                    object["_link-award"] = award["_link"]
                    object["_award"] = {key: value for key, value in award.items() if not key.startswith("_")}
        row["object"] = orjson.dumps(dict(flatten_object(object))).decode()
        row["parent_keys"] = orjson.dumps(row["parent_keys"]).decode()

    return [list(row.values()) for row in rows]


@cli.command("release-objects")
@click.argument("id")
def _release_objects(id):
    release_objects(id)


def release_objects(id):
    engine = get_engine(id)
    engine.execute(
        """
        drop table if exists release_objects;
        create table release_objects(compiled_release_id bigint,
        object_key TEXT, parent_keys JSONB, object_type TEXT, object JSONB);
        """
    )

    results = engine.execute("select compiled_release_id, compiled_release from compiled_releases")

    update_process_table(id, state="release-object-started")

    with tempfile.TemporaryDirectory() as tmpdirname:
        results = engine.execute("select compiled_release_id, compiled_release from compiled_releases")
        paths_csv_file = tmpdirname + "/paths.csv"

        print("Making CSV file")
        with open(paths_csv_file, "w+", newline="") as csv_file, Timer():
            csv_writer = csv.writer(csv_file)
            for result in results:
                csv_writer.writerows(create_rows(result))

        print("Uploading Data")
        with engine.begin() as connection, open(paths_csv_file) as f, Timer():
            dbapi_conn = connection.connection
            copy_sql = f"COPY process_job_{id}.release_objects FROM STDIN WITH CSV"
            cur = dbapi_conn.cursor()
            cur.copy_expert(copy_sql, f)

            update_process_table(id, connection=connection, state="release-object-finished")


def process_schema_object(path, current_name, flattened, obj):
    """
    Return a dictionary with a flattened representation of the schema. `patternProperties` are skipped as we don't
    want them as field names (a regular expression string) in the database.
    """
    string_path = (".".join(path)) or "release"

    properties = obj.get("properties", {})  # an object may have patternProperties only
    current_object = flattened.get(string_path)

    if current_object is None:
        current_object = {}
        flattened[string_path] = current_object

    for name, prop in list(properties.items()):
        prop_type = prop["type"]
        prop_info = dict(
            schema_type=prop["type"],
            description=prop.get("description"),
        )
        if prop_type == "object":
            if path + (name,) in EMIT_OBJECT_PATHS:
                flattened = process_schema_object(path + (name,), tuple(), flattened, prop)
            else:
                flattened = process_schema_object(path, current_name + (name,), flattened, prop)
        elif prop_type == "array":
            if "object" not in prop["items"]["type"]:
                current_object[".".join(current_name + (name,))] = prop_info
            else:
                flattened = process_schema_object(path + current_name + (name,), tuple(), flattened, prop["items"])
        else:
            current_object[".".join(current_name + (name,))] = prop_info

    return flattened


def link_info(link_name):
    split_name = link_name.split("-")
    if len(split_name) == 1:
        doc = "Link to this row that can be found in other rows"
    else:
        doc = f"Link to the {split_name[1]} row that this row relates to"

    return {"name": link_name, "description": doc, "type": "string"}


@cli.command("schema-analysis")
@click.argument("id")
def _schema_analysis(id):
    schema_analysis(id)


def schema_analysis(id):
    update_process_table(id, state="schema-analysis-started")

    builder = ProfileBuilder("1__1__4", {})
    schema = builder.patched_release_schema()

    create_table(
        "object_type_fields",
        id,
        """select
              object_type,
              each.key,
              case when
                  count(distinct jsonb_typeof(value)) > 1
              then 'string'
              else max(jsonb_typeof(value)) end
              value_type,
              count(*) from release_objects ro, jsonb_each(object) each group by 1,2;
        """,
    )

    schema_info = process_schema_object(tuple(), tuple(), {}, JsonRef.replace_refs(schema))
    for item in schema_info:
        if len(item) > 31:
            print(item)

    with get_engine(id).begin() as connection:
        result = connection.execute(
            """select object_type, jsonb_object_agg(key, value_type) fields from object_type_fields group by 1;"""
        )
        result_dict = {row.object_type: row.fields for row in result}

        object_type_order = ["release"]
        for key in schema_info:
            if key in result_dict:
                object_type_order.append(key)
        for key in result_dict:
            if key not in object_type_order:
                object_type_order.append(key)

        object_details = {}

        for object_type in object_type_order:
            fields = result_dict[object_type]

            details = [link_info("_link"), link_info("_link-release")]
            fields_added = set(["_link", "_link-release"])

            for field in fields:
                if field.startswith("_link-") and field not in fields_added:
                    details.append(link_info(field))
                    fields_added.add(field)

            schema_object_detials = schema_info.get(object_type, {})

            for schema_field, field_info in schema_object_detials.items():
                if schema_field not in fields:
                    continue
                detail = {"name": schema_field, "type": fields[schema_field]}
                detail.update(field_info)
                details.append(detail)
                fields_added.add(schema_field)

            for field in sorted(fields):
                if field in fields_added:
                    continue
                details.append(
                    {
                        "name": field,
                        "description": "No Docs as not in OCDS",
                        "type": fields[field],
                    }
                )

            object_details[object_type] = details

        connection.execute(
            """
            drop table if exists object_details;
            create table object_details(id SERIAL, object_type text, object_details JSONB);
        """
        )

        for object_type, object_details in object_details.items():
            connection.execute(
                sa.text(
                    "insert into object_details(object_type, object_details) values (:object_type, :object_details)"
                ),
                object_type=object_type,
                object_details=json.dumps(object_details),
            )

    update_process_table(id, state="schema-analysis-finished")


def create_avro_schema(object_type, object_details):
    fields = []
    schema = {
      "type": "record",
      "name": object_type,
      "fields": fields
    }
    for item in object_details:
        type = item['type']
        if type == 'number':
            type = 'double'

        field = {"name": item['name'].replace('.', '_').replace('-', '_'), "type": [type, 'null'],
                 "doc": item.get("description")}

        if type == "array":
            field["type"] = [{"type": "array", "items": "string", "default": []}, 'null']

        fields.append(field)

    return schema


def generate_records(result, object_details):

    cast_to_string = set([field['name'] for field in object_details if field['type'] == 'string'])

    for row in result:
        new_object = {}
        for key, value in row.object.items():
            new_object[key.replace('.', '_').replace('-', '_')] = str(value) if key in cast_to_string else value
        yield new_object


@cli.command("load-big-query")
@click.argument("id")
@click.argument("name")
def _load_big_query(id, name):
    load_big_query(id, name)


def load_big_query(id, name):
    from fastavro import writer, parse_schema
    from google.cloud import bigquery

    client = bigquery.Client()

    with tempfile.TemporaryDirectory() as tmpdirname, get_engine(id).begin() as connection:
        dataset_id = f"{client.project}.{name}"
        client.delete_dataset(
            dataset_id, delete_contents=True, not_found_ok=True
        )
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU"
        dataset = client.create_dataset(dataset, timeout=30)

        result = connection.execute(
            "select object_type, object_details from object_details order by id"
        )
        for object_type, object_details in list(result):
            print(f'loading {object_type}')
            result = connection.execute(
                sa.text("select object from release_objects where object_type = :object_type"),
                object_type=object_type
            )
            object_type = object_type.replace('.', '_').replace('-', '_')
            schema = create_avro_schema(object_type, object_details)

            with open(f'{tmpdirname}/{object_type}.avro', 'wb') as out:
                writer(out, parse_schema(schema), generate_records(result, object_details), validator=True)

            table_id = f"{client.project}.{id}.{object_type}"

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.AVRO
            )

            with open(f'{tmpdirname}/{object_type}.avro', "rb") as source_file:
                client.load_table_from_file(source_file, table_id, job_config=job_config, size=None, timeout=5)

if __name__ == "__main__":
    cli()
