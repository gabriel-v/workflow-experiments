import contextlib
import logging
import os
import pickle
import random
import sys
import threading
import time
import traceback
import typing

from collections import OrderedDict
from functools import wraps
from multiprocessing import Process, Queue
from typing import Any, Callable, Optional, Tuple, Dict

import psycopg2
from psycopg2 import sql

from redun.executors.base import Executor, load_task_module, register_executor
from redun.scripting import exec_script, get_task_command
from redun.task import get_task_registry


if typing.TYPE_CHECKING:
    from redun.scheduler import Job, Scheduler


log = logging.getLogger(__name__)

encode_obj = pickle.dumps
decode_obj = pickle.loads


def _processify(func):
    '''Decorator to run a function as a process.
    Be sure that every argument and the return value
    is *pickable*.
    The created process is joined, so the code does not
    run in parallel.
    '''
    # stolen from https://gist.github.com/schlamar/2311116

    def process_func(q, *args, **kwargs):
        try:
            ret = func(*args, **kwargs)
        except Exception:
            ex_type, ex_value, tb = sys.exc_info()
            error = ex_type, ex_value, ''.join(traceback.format_tb(tb))
            ret = None
        else:
            error = None

        q.put((ret, error))

    # register original function with different name
    # in sys.modules so it is pickable
    process_func.__name__ = func.__name__ + '__processify_func'
    setattr(sys.modules[__name__], process_func.__name__, process_func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        q = Queue()
        p = Process(target=process_func, args=[q] + list(args), kwargs=kwargs)
        p.start()
        ret, error = q.get()
        p.join()

        if error:
            ex_type, ex_value, tb_str = error
            message = '%s (in subprocess)\n%s' % (ex_value, tb_str)
            raise ex_type(message)

        return ret
    return wrapper


def create_queue_table(opt, table):
    """Create table to be used as message queue for the executor.
    """
    
    _sql = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            id int not null primary key generated always as identity,
            payload	bytea
        );
    """).format(table=sql.Identifier(table))
    try:
        with get_cursor(opt) as cur:
            cur.connection.autocommit = True
            cur.execute(_sql)
    except (psycopg2.errors.UniqueViolation):
        pass


@contextlib.contextmanager
def get_cursor(opt):
    """Context manager to get a database cursor and close it after use.
    
    Cursor autocommit is off. If needed, turn it on yourself with `cursor.connection.autocommit = True`.

    Args:
        opt: The arguments passed to psycopg2.connect(**opt).

    Yields:
        cursor: Database cursor that can be used while the context is active.
    """
    conn = psycopg2.connect(**opt)
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()
        conn.close()


def run_worker_single(cur, queue, result_queue=None):
    """Fetch a batch of tasks from the queue and run them, optionally returning result on different queue.

    The function is fetched, deleted and executed under a single transaction. This means that if the worker crashes, the transaction removing the items from
    the queue will be automatically rolled back, so a different worker may retry and execute it.

    If the function code itself errors out, the error should be returned in place of the result, as part of the result payload. This is handled by `submit_encoded_tasks()`.

    Args:
        cur: database cursor to be used
        queue: name of queue table where the tasks are read from
        result_queue: if set, task results are placed on this second queue.

    Returns:
        bool: True if we ran something - so we should run more. False otherwise.
    """    

    BATCH_LIMIT = 1
    sql_begin = sql.SQL("""
        BEGIN;
        DELETE FROM {queue}
        USING (
            SELECT * FROM {queue} LIMIT {BATCH_LIMIT} FOR UPDATE SKIP LOCKED
        ) q
        WHERE q.id = {queue}.id RETURNING {queue}.*;
    """).format(
            BATCH_LIMIT=sql.Literal(BATCH_LIMIT),
            queue=sql.Identifier(queue),
    )

    cur.execute(sql_begin)
    v = cur.fetchall()
    if v:
        for item in v:
            rv = decode_and_run(*item)
            if result_queue:
                rv = encode_obj(rv)
                submit_encoded_tasks(cur, result_queue, [rv])
        cur.execute("COMMIT;")
        return True

    # no result - try later
    return False


@contextlib.contextmanager
def fetch_results(cur, queue, limit=100):
    """Context manager for fetching results from a queue under transaction.

    The transaction that deletes messages from the queue is finalized when the context exists normally.

    If the code under this context crashes, the transaction is automatically rolled back,
    and the message that caused the error is put back on the queue.

    Args:
        cur: database cursor to be used
        queue: name of queue table where the tasks are read from
        limit (int, optional): Maximum number of messages to fetch. Defaults to 100.

    Yields:
        List[Object]|None: Yields a single list of results, or None if we can't find any.
    """    
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
    cur.execute("COMMIT;")


def run_worker_until_empty(cur, queue, result_queue=None):
    """Continuously run worker until we're out of messages.

    Args:
        cur: database cursor to be used
        queue: name of queue table where the tasks are read from
        result_queue: if set, task results are placed on this second queue.
    """
    while run_worker_single(cur, queue, result_queue):
        pass


def decode_and_run(_pk, payload):
    """Decode a queued payload into function and args, run the function, and return the results or errors.

    Args:
        _pk: Primary key of queue entry row.
        payload (bytes): The encoded contents of the task to run. Expected to be a pickled dict with fields `func`, `args` and `kwargs`.

    Returns:
        Dict: Object containing task metadata (under `task_args`) and function result (under `result`) or error (under `error`)
    """
    ret_obj = {'_pk': _pk, 'size': len(payload), }
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
    """Encode object in the format expected by `decode_and_run`.

    Args:
        func (Callable): function to run
        args (Tuple): Positional arguments to pass fo `func`
        kw (Dict): Keyword arguments to pass to `func`

    Returns:
        bytes: A pickled Dict containing keys `func`, `args`, `kw`
    """    
    obj = {'func': func}
    obj['args'] = args
    obj['kw'] = kw
    return encode_obj(obj)


def wait_until_notified(cur, queue, timeout=60, extra_read_fd=None):
    """Use Postgres-specific commands LISTEN, UNLISTEN to hibernate the process until
    there is new data to be read from the table queue. To achieve this, we use `select`
    on the database connection object, to sleep until there is new data to be read.

    Inspired by https://gist.github.com/kissgyorgy/beccba1291de962702ea9c237a900c79

    Args:
        cur (Cursor): Database cursor we use to run LISTEN/UNLISTEN
        queue: table queue name
        timeout (int, optional): Max seconds to wait. Defaults to 60.
        extra_read_fd (int, optional): FD which, if set, will be passed to `select` alongside the database connection. Can be used to signal early return, so this function can return immediately for worker shutdown.
    """
    import select
    chan = queue + '_channel'
    sql_listen = sql.SQL('LISTEN {chan}; COMMIT;').format(
        chan=sql.Identifier(chan))
    sql_unlisten = sql.SQL('UNLISTEN {chan}; COMMIT;').format(
        chan=sql.Identifier(chan))

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


def run_worker_forever(opt, queue, result_queue=None):
    """Start and restart worker processes, forever.

    Args:
        opt: The arguments passed to psycopg2.connect(**opt).
        queue: name of queue table where the tasks are read from
        result_queue: if set, task results are placed on this second queue.
    """
    while True:
        try:
            _run_worker_forever(opt, queue, result_queue)
        except Exception as error:
            log.error('RUN WORKER FOREVER PROCRUNNER ERROR: %s', str(error))
            time.sleep(1.0)


@_processify
def _run_worker_forever(opt, queue, result_queue=None):
    """Run a single python worker sub-process until it crashes or errors out.

    Args:
        same as `run_worker_forever`
    """
    with get_cursor(opt) as cur:
        while True:
            try:
                run_worker_until_empty(cur, queue, result_queue)
                wait_until_notified(cur, queue)
            except Exception as error:
                log.error('RUN WORKER FOREVER ERROR: %s', str(error))
                time.sleep(1.0)


def submit_encoded_tasks(cur, queue, payloads):
    """Inserts some payloads into the queue, then notifies any listeners of that queue.


    **WARNING**: This function requires the caller to run `cur.execute('commit')` and finish the transaction.
    This is done so the caller can control when their own transaction finishes, without using a sub-transaction.

    Args:
        cur (Cursor): Database cursor we use to run INSERT and NOTIFY
        queue: table queue name
        payloads (List[bytes]): A list of the payloads to enqueue.

    Returns:
        _type_: _description_
    """
    chan = queue + '_channel'
    sql_notify = sql.SQL('NOTIFY {chan};').format(chan=sql.Identifier(chan))
    sql_insert = sql.SQL(
        "INSERT INTO {queue} (payload) VALUES ({payloads}) RETURNING id;")
    sql_insert = sql_insert.format(queue=sql.Identifier(
        queue), payloads=sql.SQL(',').join(sql.Placeholder() * len(payloads)))

    cur.execute(sql_insert, payloads)
    ids = [x[0] for x in cur.fetchall()]
    cur.execute(sql_notify)
    return ids


def submit_task(opt, queue, func, *args, **kw):
    with get_cursor(opt) as cur:
        rv = submit_encoded_tasks(
            cur, queue, [encode_run_params(func, args, kw)])[0]
        cur.execute('commit')
        return rv


def exec_task(job_id: int, module_name: str, task_fullname: str,
              args: Tuple, kwargs: dict, **extra) -> Any:
    """
    Execute a task in the new process.
    """
    # stolen from local_executor.py
    load_task_module(module_name, task_fullname)
    task = get_task_registry().get(task_fullname)
    return task.func(*args, **kwargs)


def exec_script_task(job_id: int, module_name: str, task_fullname: str,
                     args: Tuple, kwargs: dict, **extra) -> bytes:
    """
    Execute a script task from the task registry.
    """
    # stolen from local_executor.py
    load_task_module(module_name, task_fullname)
    task = get_task_registry().get(task_fullname)
    command = get_task_command(task, args, kwargs)
    return exec_script(command)


@register_executor("pg")
class PgExecutor(Executor):
    def __init__(
        self,
        name: str,
        scheduler: Optional["Scheduler"] = None,
        config=None,
    ):
        super().__init__(name, scheduler=scheduler)
        config = config or dict()
        name = name or 'default'
        self._scratch_root = config.get('scratch_root', "/tmp")
        self._queue_send = config.get(
            'queue_send', f'pg_executor_{name}_queue_send')
        self._queue_recv = config.get(
            'queue_recv', f'pg_executor_{name}_queue_recv')
        self._conn_opt = dict(
            (
                (k, v)
                for (k, v) in config.items()
                if k in ['dbname', 'user', 'password', 'host', 'port', 'dsn']
            )
        )
        assert self._conn_opt is not None, 'no psycopg2 connect options given!'

        self._is_running = False
        self._pending_jobs: Dict[str, "Job"] = OrderedDict()
        self._thread: Optional[threading.Thread] = None

        self._thread_signal_read_fd = None
        self._thread_signal_write_fd = None

    def run_worker(self):
        """Create queue tables and start a single worker process for this executor, then wait for it to finish.
        """
        create_queue_table(self._conn_opt, self._queue_send)
        create_queue_table(self._conn_opt, self._queue_recv)
        run_worker_forever(self._conn_opt, self._queue_send, self._queue_recv)

    def stop(self) -> None:
        """
        Stop Executor and monitoring thread.
        """
        self._is_running = False
        os.write(self._thread_signal_write_fd, b'x')

        # Stop monitor thread.
        if (
            self._thread
            and self._thread.is_alive()
            and threading.get_ident() != self._thread.ident
        ):
            self._thread.join()

    def _start(self) -> None:
        """
        Start monitoring thread. Workers need to be started separately, on different processes, using `PgExecutor.run_worker()`.
        """
        if not self._is_running:
            os.makedirs(self._scratch_root, exist_ok=True)
            create_queue_table(self._conn_opt, self._queue_send)
            create_queue_table(self._conn_opt, self._queue_recv)

            (
                self._thread_signal_read_fd,
                self._thread_signal_write_fd,
            ) = os.pipe()
            self._is_running = True
            self._thread = threading.Thread(
                target=self._monitor,
                daemon=False,
            )
            self._thread.start()

    def _monitor(self) -> None:
        """
        Thread for monitoring task ack. Uses single long-running database connection.
        """
        assert self._scheduler

        try:
            with get_cursor(self._conn_opt) as cur:
                while self._is_running:
                    while self._monitor_one(cur):
                        pass
                    if self._is_running:
                        wait_until_notified(
                            cur,
                            self._queue_recv,
                            extra_read_fd=self._thread_signal_read_fd,
                        )

        except Exception as error:
            self._scheduler.reject_job(None, error)

        self.stop()

    def _monitor_one(self, cur):
        """Run a single batch of task monitoring.

        Args:
            cur (Cursor): Database cursor to use for fetching results.

        Returns:
            bool: True if we found something on the queue, meaning the caller should immediately run this function again.
        """
        with fetch_results(cur, self._queue_recv) as results:
            if results is None:
                return False

            for result in results:
                job_id = result['task_args']['kw']['job_id']
                try:
                    job = self._pending_jobs.pop(job_id)
                except Exception:
                    log.error('unknwon job: %s', job_id)
                    continue
                if 'error' in result:
                    self._scheduler.reject_job(job, result['error'])
                elif 'result' in result:
                    self._scheduler.done_job(job, result['result'])
                else:
                    raise RuntimeError(
                        'monitor: unknown object response val: ' + str(result))

            return True

    def _submit(self, exec_func: Callable, job: "Job") -> None:
        self._start()

        args, kwargs = job.args
        self._pending_jobs[job.id] = job
        submit_task(
            self._conn_opt,
            self._queue_send,
            exec_func,
            job_id=job.id,
            module_name=job.task.load_module,
            task_fullname=job.task.fullname,
            args=args,
            kwargs=kwargs,
        )

    def submit(self, job: "Job") -> None:
        assert not job.task.script
        self._submit(exec_task, job)

    def submit_script(self, job: "Job") -> None:
        assert job.task.script
        self._submit(exec_script_task, job)

    def scratch_root(self) -> str:
        return self._scratch_root
