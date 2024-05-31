import os
import json
import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import datetime

def get_connection():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'), 
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'), 
        password=os.getenv('POSTGRES_PASSWORD')
        )

def item_exists(conn, id, target_table):
    cur = conn.cursor()

    if target_table == 'dim_track':
        cur.execute(f"SELECT track_id FROM dim_track WHERE track_id='{id}'")
    elif target_table == 'dim_artist':
        cur.execute(f"SELECT artist_id FROM dim_artist WHERE artist_id='{id}'")
    elif target_table == 'dim_album':
        cur.execute(f"SELECT album_id FROM dim_album WHERE album_id='{id}'")
    else:
        print(f'The table {target_table} does not exist')

    return cur.fetchone() is not None

def insert_data(conn, df, table):
    if df.index.name is not None:
        df = df.reset_index()

    cur = conn.cursor()
    df_columns = list(df)
    columns = ', '.join(df_columns)
    values = "VALUES ({})".format(", ".join(["%s" for _ in df_columns]))
    insert_stmt = "INSERT INTO {} ({}) {} ON CONFLICT DO NOTHING".format(table, columns, values)

    psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
    conn.commit()

def insert_scd_source_data(conn, df, table='staging.fact_top_50_stage'):
    cur = conn.cursor()

    truncate_stmt = 'TRUNCATE TABLE {};'.format(table)
    cur.execute(truncate_stmt)
    conn.commit()

    df['valid_from'] = pd.to_datetime(datetime.now())
    df['valid_to'] = pd.to_datetime(datetime.strptime('2099-12-31 00:00:00', '%Y-%m-%d %H:%M:%S'))

    df_columns = list(df)
    columns = ', '.join(df_columns)
    values = "VALUES ({})".format(", ".join(["%s" for _ in df_columns]))
    insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)

    psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
    conn.commit()

def update_fact_scd(conn, table='source.fact_top_50'):
    q = '''
        WITH u AS (
            UPDATE source.fact_top_50 fact
                SET is_current = False,
                    valid_to = src.valid_from
            FROM staging.fact_top_50_stage src
            WHERE
                fact.track_id != src.track_id
                AND fact.is_current = True
                AND fact.rank = src.rank
                AND fact.time_range = src.time_range
        )

        INSERT INTO source.fact_top_50 (
            track_id,
            rank,
            time_range,
            is_current,
            valid_from,
            valid_to)
    	SELECT
    		a.track_id,
    		a.rank,
    		a.time_range,
    		a.is_current,
    		a.valid_from,
    		a.valid_to
    	FROM staging.fact_top_50_stage AS a
    	LEFT JOIN source.fact_top_50 AS b
    	    ON a.track_id = b.track_id
    	    AND a.rank = b.rank
    	    AND a.time_range = b.time_range
    	    AND b.is_current = True
    	WHERE b.track_id IS NULL
        ON CONFLICT DO NOTHING
    '''

    cur = conn.cursor()
    cur.execute(q)
    conn.commit()