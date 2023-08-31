#!/usr/bin/python3
# -*- coding: utf-8 -*-
# coding: utf8

"""
Copyright: (c) 2023, Siarhei Straltsou
2023-08-30
Test task cheaters
"""

import csv
import logging
import sqlite3
from sqlite3 import Error
from datetime import datetime
from memory_profiler import profile

DIFF = 24 * 60 * 60
RESULT_TABLE_NAME = 'results'


def store_to_db(table, cursor, fields, item):
    try:
        ex_str = 'INSERT INTO {} ({}) VALUES ({})'.format(table,
                                                          ','.join(fields),
                                                          ','.join(list(['?' for _ in range(len(fields))])))
        logging.debug(f'EX_Str: {ex_str}')
        logging.debug(f'Item: {item}')
        cursor.execute(ex_str, item)
    except Error as e:
        logging.error(f'{e}, {table}, {fields}, {item}')


@profile
def load_csv_to_db(db, table: str, filename: str, d_filter: str, delimiter: str = ',', skip_head: bool = True,
                   batch_size: int = 5000):
    batch = batch_size
    with open(filename, 'r', encoding='utf-8') as csv_file:
        try:
            csv_lst = csv.reader(csv_file, delimiter=delimiter)
            if skip_head:
                next(csv_lst)
                is_first = False
            else:
                is_first = True
            cursor = db.cursor()
            dt_date_filter = datetime.strptime(d_filter, '%Y-%m-%d')
            for row in csv_lst:
                if is_first:
                    row_names = list(row)
                    logging.debug(row_names)
                    is_first = False
                else:
                    item = list(row)
                    dt_object = datetime.fromtimestamp(int(item[0]))
                    logging.debug(f'Item: {item}')
                    if dt_object.date() == dt_date_filter.date():
                        store_to_db(table=table, cursor=cursor, fields=row_names, item=item)
                        batch -= 1
                        if batch == 0:
                            db.commit()
                            batch = batch_size
            db.commit()
            cursor.close()
        except Exception as e:
            logging.error(f'{e}')


def prepare(db: str = ':memory:', tables: list = None):
    if tables is None:
        tables = list()
    db_connector = sqlite3.connect(db)
    cursor = db_connector.cursor()
    for tbl in tables:
        cursor.execute(f'DROP TABLE IF EXISTS {tbl}')
    db_connector.commit()
    cursor.close()
    return db_connector


@profile
def copy_cheaters(src_db, dst_db, d_filter: str):
    cursor_src = src_db.cursor()
    cursor_dst = dst_db.cursor()
    query = 'SELECT * FROM cheaters'
    cursor_src.execute(query)
    cheaters_rows = cursor_src.fetchall()
    cheaters_rows_filtered = list()

    dt_date_filter = datetime.strptime(d_filter, '%Y-%m-%d')
    ts_date_filter = datetime.timestamp(dt_date_filter)

    for row in cheaters_rows:
        dt_date_time = datetime.strptime(str(row[1]), '%Y-%m-%d %H:%M:%S')
        ts = datetime.timestamp(dt_date_time)
        if int(ts) >= int(ts_date_filter) and (int(ts)-int(ts_date_filter) <= DIFF):
            cheaters_rows_filtered.append((row[0], str(int(ts))))
    insert_records = 'INSERT INTO cheaters (player_id, ban_time) VALUES(?, ?)'
    cursor_dst.executemany(insert_records, cheaters_rows_filtered)
    dst_db.commit()
    cursor_src.close()
    cursor_dst.close()


@profile
def main_task(src_db, dst_db):
    cursor_src = src_db.cursor()
    cursor_dst = dst_db.cursor()
    query = '''SELECT server.event_id, client.player_id,
                      server.timestamp, server.error_id,
                      server.description json_server,
                      client.description json_client
                FROM client, server
                WHERE server.error_id = client.error_id ORDER BY player_id
                AND client.player_id NOT IN (SELECT cheaters.player_id FROM cheaters)'''
    cursor_src.execute(query)
    task_rows = cursor_src.fetchall()
    insert_records = '''INSERT INTO results (event_id, player_id, timestamp, error_id, json_server, json_client)
                        VALUES(?, ?, ?, ?, ?, ?)'''
    cursor_dst.executemany(insert_records, task_rows)
    dst_db.commit()
    cursor_src.close()
    cursor_dst.close()


@profile
def task_add_table(db, table_name: str):
    cursor = db.cursor()
    query = f'''CREATE TABLE {table_name} (timestamp INTEGER, player_id INTEGER, event_id INTEGER,
                                           error_id TEXT, json_server TEXT, json_client TEXT)'''
    cursor.execute(query)
    db.commit()
    cursor.close()


if __name__ == '__main__':

    date_filter = input('Input date for filtering items, format YYYY-MM-DD: ')

    logging.getLogger()
    debug_mode = False
    if debug_mode:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s %(levelname)s - %(message)s',
                        handlers=[logging.StreamHandler()])

    db_mem = prepare(tables=['client', 'server', 'cheaters'])
    db_cheaters = prepare(db='cheaters.db', tables=['results'])

    task_add_table(db=db_cheaters, table_name=RESULT_TABLE_NAME)

    cursor = db_mem.cursor()
    query = 'CREATE TABLE IF NOT EXISTS client (timestamp INTEGER, error_id TEXT, player_id INTEGER, description TEXT)'
    cursor.execute(query)
    query = 'CREATE TABLE IF NOT EXISTS server (timestamp INTEGER, event_id INTEGER, error_id TEXT, description TEXT)'
    cursor.execute(query)
    query = 'CREATE TABLE IF NOT EXISTS cheaters (player_id INTEGER, ban_time INTEGER)'
    cursor.execute(query)
    db_mem.commit()
    cursor.close()

    copy_cheaters(src_db=db_cheaters, dst_db=db_mem, d_filter=date_filter)
    load_csv_to_db(db=db_mem, table='client', filename='client.csv', d_filter=date_filter, skip_head=False)
    load_csv_to_db(db=db_mem, table='server', filename='server.csv', d_filter=date_filter, skip_head=False)

    main_task(src_db=db_mem, dst_db=db_cheaters)

    db_mem.close()
    db_cheaters.close()
