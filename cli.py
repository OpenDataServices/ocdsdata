import click
import os
import functools
import datetime
import time
import traceback
from pydecor import intercept
import json
import csv
from collections import Counter
import shutil

import orjson
from pathlib import Path
import sqlalchemy as sa
from scrapy.utils.project import get_project_settings
from scrapy.spiderloader import SpiderLoader
from scrapy.crawler import CrawlerProcess
import concurrent.futures
from testfixtures import LogCapture
from scrapy import signals


this_path = Path(__file__).parent.absolute()
collect_path = str((this_path / 'kingfisher-collect').absolute())

def print_exception(exc):
    traceback.print_tb(exc.__traceback__)
    print(exc)

@functools.lru_cache(None)
def get_engine(db_uri=None):
    if not db_uri:
        db_uri = os.environ['DATABASE_URL']
    return sa.create_engine(db_uri)
    

def refresh_database():
    engine = get_engine()
    
    engine.execute('''

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
    

    --DROP TABLE IF EXISTS process_table;

    --CREATE TABLE IF NOT EXISTS process_table(
    --                   id Serial,
    --                   scraper text,
    --                   state text,
    --                   time_created timestamp,
    --                   time_started timestamp null,
    --                   time_finished timestamp null,
    --                   failed int default 0,
    --                   checkin_time timestamp null,
    --                   logs jsonb
    --                  );

    --create unique index on process_table(state, scraper) where state = 'new'
    ''')

    #metadata = sqlalchemy.MetaData(engine)
    #metadata.reflect()




@click.group()
def cli():
    pass

@cli.command()
def refresh_db():
    refresh_database()
    click.echo('refreshed database')

@cli.command()
def queue_all():
    engine = get_engine()
    os.chdir(collect_path)

    settings = get_project_settings()
    sl = SpiderLoader.from_settings(settings)

    with engine.begin() as connection:
        time_created = str(datetime.datetime.utcnow())
        for spider_name in sl.list():
            connection.execute(sa.text(
                '''insert into process_table (scraper, state, time_created, logs) 
                   values (:spider, 'new', :time_created, '{}') on conflict do nothing '''
            ), spider=spider_name, time_created=time_created)

@cli.command()
def export_scrapers():
    engine = get_engine()
    os.chdir(collect_path)

    settings = get_project_settings()
    sl = SpiderLoader.from_settings(settings)
    click.echo(json.dumps(sl.list()))

@cli.command()
@click.argument('name')
@click.argument('id')
@click.argument('run_id')
def run_spider(name, id, run_id):
    data_dir = this_path / 'data' / id
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_file_path = data_dir / 'all.csv'


    engine = get_engine()
    with engine.begin() as connection:
        connection.execute(
            f'''drop schema if exists process_job_{id} cascade;
                create schema process_job_{id};
                create table process_job_{id}.scrape_data(name TEXT, url TEXT, data_type TEXT, file_name TEXT, valid BOOLEAN, data JSONB, error_data TEXT);
                create table process_job_{id}.job_info(name TEXT, info JSONB, logs TEXT);
            '''
        )
        time_now = str(datetime.datetime.utcnow())
        connection.execute(
            sa.text('''DELETE FROM process_table where id=:id'''), id=id
        )
        connection.execute(
            sa.text('''INSERT INTO process_table (id, schema, run_id, name, state, time_started, last_update)
                 values (:id, :schema, :run_id, :name, :state, :time_now , :time_now)'''),
            id=id, schema = f'process_job_{id}', name=name, run_id=run_id, state='scrape-started', time_now=time_now
        )


    os.environ['FILES_STORE'] = str(data_dir.absolute())

    os.chdir(collect_path)
    settings = get_project_settings()
    settings['LOG_FILE'] = str(data_dir / 'all.log')

    with open(str(csv_file_path), 'w+', newline='') as csv_file:

        csv_writer = csv.writer(csv_file)
            
        count_data_types = Counter()

        def save_to_csv(item, spider):
            if isinstance(item['data'], dict):
                data = orjson.dumps(item['data'], default=str)
                valid = 't'
                error_data = ''
            else:
                try:
                    orjson.loads(item['data'])
                    data = item['data']
                    valid = 't'
                    error_data = ''
                except orjson.JSONDecodeError:
                    valid = 'f'
                    data = '{}'
                    error_data=item['data']

            if isinstance(data, bytes):
                data = data.decode(item['encoding'])
                
            count_data_types.update([item['data_type']])

            csv_writer.writerow([name, item['url'], item['data_type'], item['file_name'], valid, data, error_data])
            print([item['url'], item['data_type'], item['file_name']])


        runner = CrawlerProcess(settings)
        crawler = runner.create_crawler(name)

        crawler.signals.connect(save_to_csv, signal=signals.item_scraped)

        runner.crawl(crawler)
        runner.start()

    info = crawler.stats.spider_stats[name]
    info['name'] = name
    info['data_types'] = dict(count_data_types)
    info_file = data_dir / 'info.json'
    info_data = json.dumps(info, default=str)
    info_file.write_text(info_data)

    log_text = (data_dir / 'all.log').read_text()

    with engine.begin() as connection, open(str(csv_file_path)) as f:
        connection.execute(
            sa.text(f'insert into process_job_{id}.job_info values (:name, :info, :logs)'),
            name=name, info=info_data, logs=log_text
        )

        dbapi_conn = connection.connection
        copy_sql = f'COPY process_job_{id}.scrape_data FROM STDIN WITH CSV'
        cur = dbapi_conn.cursor()
        cur.copy_expert(copy_sql, f)

        time_now = str(datetime.datetime.utcnow())
        connection.execute(
            sa.text('''update process_table set 
                       state=:state, last_update=:time_now, scrape_info =:scrape_info  
                       where id = :id'''),
            id=id, state='scrape-finished', time_now=time_now, scrape_info=info_data
        )

    shutil.rmtree(data_dir)


def update_process_table(connection, id, **kwargs):
    set_parts = []
    for key in kwargs:
        set_parts.append(f'{key} = :{key}')
    set = ", ". join(set_parts)

    sql = f'update process_table set {set} where id=:id'

    connection.execute(sa.text(sql), id=id, **kwargs)


@intercept(catch=Exception, handler=print_exception, reraise=True)
def scrape_job():
    engine = get_engine()
    with engine.begin() as connection:
        row = connection.execute('''select id from process_table where state = 'new' limit 1 for update skip locked ''').first()
        if not row:
            return
        row_id = row.id
        time_started = str(datetime.datetime.utcnow())

        update_process_table(connection, row_id, time_started=time_started, checkin_time=time_started, state='started-scrape')

    time.sleep(random.random() * 2)

    with engine.begin() as connection:
        time_finished = str(datetime.datetime.utcnow())
        update_process_table(connection, row_id, time_finished=time_finished, 
                            checkin_time=time_finished, failed=0, state='finished-scrape')

    print(f'scrape {row_id} finished')


@cli.command()
def run_scrapes():
    os.chdir(collect_path)
    engine = get_engine()

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            with engine.begin() as connection:
                row = connection.execute('''select id from process_table where state = 'new' limit 1 ''').first()
                if row:
                    executor.submit(scrape_job)
                    continue
                time.sleep(10)
                print('sleeping')


if __name__ == '__main__':
    cli()
