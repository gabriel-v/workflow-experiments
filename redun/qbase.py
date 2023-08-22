import sys
import traceback
import pickle
import random
import contextlib
import time
import logging


import psycopg2
from psycopg2 import sql


from processify import processify

DB_NAME = 'redun_test_4'
QUEUE_SEND = 'queue_send'
QUEUE_RECV = 'queue_recv'
TEST_Q = 'qsub'
TEST_Q_2 = 'qsub2'
PG_URI = "postgresql://localhost:5432/"
log = logging.getLogger(__name__)


def create_db():
    _sql = sql.SQL("""
    CREATE DATABASE {DB_NAME};
    """).format(DB_NAME=sql.Identifier(DB_NAME))

    try:
        with get_cursor(db=None) as cur:
            cur.connection.autocommit = True
            cur.execute(_sql)
    except (psycopg2.errors.DuplicateDatabase, psycopg2.errors.UniqueViolation):
        pass


def create_queue_table(queue):
    _sql = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {queue} (
            id int not null primary key generated always as identity,
            payload	bytea
        );
    """).format(queue=sql.Identifier(queue))
    try:
        with get_cursor() as cur:
            cur.connection.autocommit = True
            cur.execute(_sql)
    except (psycopg2.errors.UniqueViolation):
        pass


@contextlib.contextmanager
def get_cursor(db=DB_NAME):
    conn = psycopg2.connect(PG_URI + (db or ''))
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


def run_worker_single(cur, queue, result_queue=None):
    """Fetch a batch of tasks from the queue and run them.

    If we have a result queue, result is put on that.

    Return True if we ran something - so we should run more.
    """
    BATCH_LIMIT = 1
    sql_begin = sql.SQL("""
        BEGIN;
        DELETE FROM {queue}
        USING (
            SELECT * FROM {queue} LIMIT {BATCH_LIMIT} FOR UPDATE SKIP LOCKED
        ) q
        WHERE q.id = {queue}.id RETURNING {queue}.*;
    """).format(BATCH_LIMIT=sql.Literal(BATCH_LIMIT), queue=sql.Identifier(queue))

    cur.execute(sql_begin)
    v = cur.fetchall()
    if v:
        for item in v:
            rv = decode_and_run(*item)
            if result_queue:
                rv = encode_obj(rv)
                submit_encoded_tasks(cur, result_queue, [rv])
        cur.execute("COMMIT;");
        return True

    # no result - try later
    return False


@contextlib.contextmanager
def fetch_results(cur, queue, limit=100):
    sql_begin = sql.SQL("""
        BEGIN;
        DELETE FROM {queue}
        USING (
            SELECT * FROM {queue} LIMIT {limit} FOR UPDATE SKIP LOCKED
        ) q
        WHERE q.id = {queue}.id RETURNING {queue}.*;
    """).format(queue=sql.Identifier(queue), limit=sql.Literal(limit))

    cur.execute(sql_begin)
    rows = cur.fetchall()
    if not rows:
        yield None
    else:
        yield [decode_obj(row[1]) for row in rows]
    cur.execute("COMMIT;");


def run_worker_until_empty(cur, queue, result_queue=None):
    while run_worker_single(cur, queue, result_queue):
        pass


def decode_and_run(_pk, payload):
    ret_obj = {'_pk': _pk, 'size': len(payload),}
    try:
        obj = decode_obj(payload)
        ret_obj['task_args'] = obj

        func = obj['func']
        args = obj.get('args', tuple())
        kw = obj.get('kw', dict())
        ret_obj['result'] = func(*args, **kw)
        return ret_obj
    except Exception as e:
        log.error('ERROR in task  id = %s err = %s', _pk, str(e))
        traceback.print_exception(*sys.exc_info())
        ret_obj['error'] = e
        return ret_obj


def encode_run_params(func, args, kw):
    obj = {'func': func}
    obj['args'] = args
    obj['kw'] = kw
    return encode_obj(obj)


def wait_until_notified(cur, queue, timeout=60, extra_read_fd=None):
    import select
    chan = queue + '_channel'
    sql_listen = sql.SQL('LISTEN {chan}; COMMIT;').format(chan=sql.Identifier(chan))
    sql_unlisten = sql.SQL('UNLISTEN {chan}; COMMIT;').format(chan=sql.Identifier(chan))

    cur.execute(sql_listen)
    conn = cur.connection
    timeout = int(timeout * (0.5 + random.random()))
    if extra_read_fd:
        fds = select.select((conn, extra_read_fd), (), (), timeout)
        if extra_read_fd in fds[0]:
            os.read(extra_read_fd, 1)
    else:
        select.select((conn,), (), (), timeout)
    conn.notifies.clear()
    cur.execute(sql_unlisten)


def run_worker_forever(queue, result_queue=None):
    while True:
        try:
            _run_worker_forever(queue, result_queue)
        except Exception as error:
            log.error('RUN WORKER FOREVER PROCRUNNER ERROR: %s', str(error))
            time.sleep(1.0)

@processify
def _run_worker_forever(queue, result_queue=None):
    with get_cursor() as cur:
        while True:
            try:
                run_worker_until_empty(cur, queue, result_queue)
                wait_until_notified(cur, queue)
            except Exception as error:
                log.error('RUN WORKER FOREVER ERROR: %s', str(error))
                time.sleep(1.0)


def submit_encoded_tasks(cur, queue, payloads):
    chan = queue + '_channel'
    sql_notify = sql.SQL('NOTIFY {chan};').format(chan=sql.Identifier(chan))
    sql_insert = sql.SQL("INSERT INTO {queue} (payload) VALUES ({payloads}) RETURNING id;")
    sql_insert = sql_insert.format(queue=sql.Identifier(queue), payloads=sql.SQL(',').join(sql.Placeholder() * len(payloads)))

    cur.execute(sql_insert, payloads)
    ids = [x[0] for x in cur.fetchall()]
    cur.execute(sql_notify)
    return ids


def submit_task(queue, func, *args, **kw):
    with get_cursor() as cur:
        rv = submit_encoded_tasks(cur, queue, [encode_run_params(func, args, kw)])[0]
        cur.execute('commit')
        return rv


def encode_obj(obj):
    rv = pickle.dumps(obj)
    return rv


def decode_obj(bytes_):
    return pickle.loads(bytes_)
