import binascii
import pickle
import random
import contextlib
import time

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

DB_NAME = 'redun_test'
QUEUE_SEND = 'queue_send'
QUEUE_RECV = 'queue_recv'
TEST_Q = 'qsub'
PG_URI = "postgresql://localhost:5432/"


def create_db():
    sql = f"""
    CREATE DATABASE {DB_NAME};
    """

    try:
        with cursor(db=None) as cur:
            cur.execute(sql)
    except psycopg2.errors.DuplicateDatabase:
        pass


def create_queue_table(queue):
    sql = f"""
        CREATE TABLE IF NOT EXISTS {queue} (
            id int not null primary key generated always as identity,
            queue_time	timestamptz default now(),
            payload	text
        );
    """
    with cursor() as cur:
        cur.execute(sql)


@contextlib.contextmanager
def cursor(autocommit=True, db=DB_NAME):
    # Wait for database to accept connections.
    conn = psycopg2.connect(PG_URI + (db or ''))
    if autocommit:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    try:
        yield cur
    finally:
        conn.close()


def init():
    create_db()
    create_queue_table(QUEUE_RECV)
    create_queue_table(QUEUE_SEND)
    create_queue_table(TEST_Q)


def run_worker_until_empty(queue, return_queue=None):
    BATCH_LIMIT = 1
    sql_begin = f"""
        BEGIN;
        DELETE FROM {queue}
        USING (
            SELECT * FROM {queue} LIMIT {BATCH_LIMIT} FOR UPDATE SKIP LOCKED
        ) q
        WHERE q.id = {queue}.id RETURNING {queue}.*;
    """
    sql_wait = f"SELECT * FROM {queue} LIMIT {BATCH_LIMIT} FOR UPDATE;"
    print('looking for tasks...')
    while True:
        with cursor(autocommit=False) as cur:
            cur.execute(sql_begin)
            v = cur.fetchall()
            if v:
                for _pk, _start_q, payload in v:
                    rv = decode_and_run(payload)
                    if return_queue:
                        rv = encode_obj(rv)
                        submit_encoded_tasks(return_queue, [rv])
                cur.execute("COMMIT;");
                continue

        # no result - try later
        return

def decode_and_run(payload):
    obj = decode_obj(payload)
    func = obj['func']
    args = obj.get('args', tuple())
    kw = obj.get('kw', dict())
    return func(*args, **kw)


def encode_run_params(func, *args, **kw):
    obj = {'func': func}
    if args:
        obj['args'] = args
    if kw:
        obj['kw'] = kw
    return encode_obj(obj)


def run_worker(queue):
    import select
    sql_listen = f'LISTEN {queue}_channel;'
    sql_unlisten = f'UNLISTEN {queue}_channel;'
    SELECT_TIMEOUT = 60
    while True:
        run_worker_until_empty(queue)
        with cursor() as cur:
            cur.execute(sql_listen)
            conn = cur.connection
            timeout = int(SELECT_TIMEOUT * (0.5 + random.random()))
            print('hibernating for', timeout, 'sec')
            select.select((conn,), (), (), timeout)


def submit_encoded_tasks(queue, payloads):
    sql_notify = f"NOTIFY {queue}_channel;"
    with cursor() as cur:
        for payload in payloads:
            sql = f"INSERT INTO {queue} (payload) VALUES ('{payload}');"
            cur.execute(sql)
            cur.execute(sql_notify)


def submit_task(queue, func, *args, **kw):
    submit_encoded_tasks(queue, [encode_run_params(func, *args, **kw)])


def encode_obj(obj):
    rv = binascii.b2a_base64(pickle.dumps(obj)).decode('ascii')
    assert isinstance(rv, str)
    return rv


def decode_obj(str_):
    return pickle.loads(binascii.a2b_base64(str_))
