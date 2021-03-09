import os
import functools

import sqlalchemy


@functools.lru_cache(None)
def get_engine(db_uri=None):
    if not db_uri:
        db_uri = os.environ['DATABASE_URL']
    return sqlalchemy.create_engine(db_uri)
    

def refresh_db():
    engine = get_engine()
    connection = engine.connect()
    
    connection.execute('''
    DROP TABLE IF EXISTS import_queue;

    CREATE TABLE IF NOT EXISTS import_table(
                       id Serial,
                       scraper text,
                       state text,
                       time_started timestamp null,
                       time_finished timestamp null,
                       last_scraper_checkin timestamp null,
                       logs jsonb
                      )
    ''')

    metadata = sqlalchemy.MetaData(engine)
    metadata.reflect()



if __name__ == '__main__':
    refresh_db()


