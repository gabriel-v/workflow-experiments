import sys
import traceback
import binascii
import pickle
import random
import contextlib
import time

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


from processify import processify

DB_NAME = 'redun_test_4'
QUEUE_SEND = 'queue_send'
QUEUE_RECV = 'queue_recv'
TEST_Q = 'qsub'
TEST_Q_2 = 'qsub2'
PG_URI = "postgresql://localhost:5432/"


def create_db():
    sql = f"""
    CREATE DATABASE {DB_NAME};
    """

    try:
        with cursor(db=None) as cur:
            cur.execute(sql)
    except (psycopg2.errors.DuplicateDatabase, psycopg2.errors.UniqueViolation):
        pass


def create_queue_table(queue):
    sql = f"""
        CREATE TABLE IF NOT EXISTS {queue} (
            id int not null primary key generated always as identity,
            queue_time	timestamptz default now(),
            payload	text
        );
    """
    try:
        with cursor() as cur:
            cur.execute(sql)
    except (psycopg2.errors.UniqueViolation):
        pass


def pg_now():
    with cursor(db=None) as cur:
        cur.execute("select now();")
        return cur.fetchone()[0]


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
    create_queue_table(TEST_Q_2)


def run_worker_single(queue, result_queue=None):
    """Fetch a batch of tasks from the queue and run them.

    If we have a result queue, result is put on that.

    Return True if we ran something - so we should run more.
    """
    BATCH_LIMIT = 1
    sql_begin = f"""
        BEGIN;
        DELETE FROM {queue}
        USING (
            SELECT * FROM {queue} LIMIT {BATCH_LIMIT} FOR UPDATE SKIP LOCKED
        ) q
        WHERE q.id = {queue}.id RETURNING {queue}.*;
    """

    with cursor(autocommit=False) as cur:
        cur.execute(sql_begin)
        v = cur.fetchall()
        if v:
            for item in v:
                rv = decode_and_run(*item)
                if result_queue:
                    rv = encode_obj(rv)
                    submit_encoded_tasks(result_queue, [rv], existing_cursor=cur)
            cur.execute("COMMIT;");
            return True

    # no result - try later
    return False


@contextlib.contextmanager
def fetch_result(queue):
    sql_begin = f"""
        BEGIN;
        DELETE FROM {queue}
        USING (
            SELECT * FROM {queue} LIMIT 1 FOR UPDATE SKIP LOCKED
        ) q
        WHERE q.id = {queue}.id RETURNING {queue}.*;
    """
    with cursor(autocommit=False) as cur:
        cur.execute(sql_begin)
        v = cur.fetchall()
        if v:
            v = v[0]
            payload = v[2]
            yield decode_obj(payload)
        else:
            yield None
        cur.execute("COMMIT;");


def run_worker_until_empty(queue, result_queue=None):
    # print('looking for tasks...')
    while run_worker_single(queue, result_queue):
        pass


def decode_and_run(_pk, queue_time, payload):
    print('STARTED  task  id =', _pk, '  added =', queue_time, '  size =', len(payload))
    ret_obj = {'_pk': _pk, 'queue_time': queue_time, 'size': len(payload),}
    try:
        obj = decode_obj(payload)
        ret_obj['task_args'] = obj

        func = obj['func']
        args = obj.get('args', tuple())
        kw = obj.get('kw', dict())

        ret_obj['start_time'] = pg_now()
        ret_obj['result'] = processify(func)(*args, **kw)
        ret_obj['end_time'] = pg_now()
        print('SUCCESS  task  id =', _pk, '  added =', queue_time, '  size =', len(payload))
        return ret_obj
    except Exception as e:
        print('ERROR in task  id =', _pk, '  added =', queue_time, ' err = ', str(e))
        traceback.print_exception(*sys.exc_info())
        ret_obj['end_time'] = pg_now()
        ret_obj['error'] = e
        return ret_obj


def encode_run_params(func, args, kw):
    obj = {'func': func}
    obj['args'] = args
    obj['kw'] = kw
    return encode_obj(obj)


def wait_until_notified(queue, timeout=3):
    import select
    chan = f'{queue}_channel'
    with cursor() as cur:
        cur.execute(f'LISTEN {chan};')
        conn = cur.connection
        timeout = int(timeout * (0.5 + random.random()))
        # print('listening on ', chan, 'for', timeout, 'sec')
        select.select((conn,), (), (), timeout)


def run_worker_forever(queue, result_queue=None):
    while True:
        try:
            _run_worker_forever(queue, result_queue)
        except Exception as error:
            print('RUN WORKER FOREVER PROCRUNNER ERROR: ', str(error))
            time.sleep(1.0)

@processify
def _run_worker_forever(queue, result_queue=None):
    while True:
        try:
            run_worker_until_empty(queue, result_queue)
            wait_until_notified(queue)
            run_worker_until_empty(queue, result_queue)
            time.sleep(0.001)
        except Exception as error:
            print('RUN WORKER FOREVER ERROR: ', str(error))
            time.sleep(1.0)


def submit_encoded_tasks(queue, payloads, existing_cursor=None):
    sql_notify = f"NOTIFY {queue}_channel;"

    def _do_it(cur):
        ids = []
        for payload in payloads:
            sql = f"INSERT INTO {queue} (payload) VALUES ('{payload}') RETURNING id;"
            cur.execute(sql)
            ids.append(cur.fetchone()[0])
            cur.execute(sql_notify)
        return ids

    if existing_cursor:
        return _do_it(existing_cursor)
    else:
        with cursor() as cur:
            return _do_it(cur)


def submit_task(queue, func, *args, **kw):
    return submit_encoded_tasks(queue, [encode_run_params(func, args, kw)])[0]


def encode_obj(obj):
    rv = binascii.b2a_base64(pickle.dumps(obj)).decode('ascii')
    assert isinstance(rv, str)
    return rv


def decode_obj(str_):
    return pickle.loads(binascii.a2b_base64(str_))
