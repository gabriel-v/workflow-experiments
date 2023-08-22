import logging
import os
import threading
import sys
import typing
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import get_context, get_start_method, set_start_method
from typing import Any, Callable, Optional, Tuple

from redun.config import create_config_section
from redun.executors.base import Executor, load_task_module, register_executor
from redun.scripting import exec_script, get_task_command
from redun.task import get_task_registry


if typing.TYPE_CHECKING:
    from redun.scheduler import Job, Scheduler


from qbase import QUEUE_SEND, QUEUE_RECV, submit_task, fetch_results, wait_until_notified, get_cursor


def exec_task(job_id: int, module_name: str, task_fullname: str, args: Tuple, kwargs: dict, **extra) -> Any:
    """
    Execute a task in the new process.
    """
    load_task_module(module_name, task_fullname)
    task = get_task_registry().get(task_fullname)
    return task.func(*args, **kwargs)


def exec_script_task(job_id: int, module_name: str, task_fullname: str, args: Tuple, kwargs: dict, **extra) -> bytes:
    """
    Execute a script task from the task registry.
    """
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

        self._scratch_root = "/tmp/redun"
        self._is_running = False
        self._pending_jobs: Dict[str, "Job"] = OrderedDict()
        self._thread: Optional[threading.Thread] = None

        self._thread_signal_read_fd = None
        self._thread_signal_write_fd = None

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
        Start monitoring thread.
        """
        os.makedirs(self._scratch_root, exist_ok=True)

        if not self._is_running:

            self._thread_signal_read_fd, self._thread_signal_write_fd = os.pipe()
            self._is_running = True
            self._thread = threading.Thread(target=self._monitor, daemon=False)
            self._thread.start()

    def _monitor(self) -> None:
        """
        Thread for monitoring task ack.
        """
        assert self._scheduler

        try:
            with get_cursor() as cur:
                while self._is_running:
                    while self._monitor_one(cur):
                        pass
                    if self._is_running:
                        wait_until_notified(cur, QUEUE_RECV, extra_read_fd=self._thread_signal_read_fd)

        except Exception as error:
            self._scheduler.reject_job(None, error)

        self.stop()

    def _monitor_one(self, cur):
        with fetch_results(cur, QUEUE_RECV) as results:
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
                    raise RuntimeError('monitor: unknown object response val: ' + str(result))

            return True

    def _submit(self, exec_func: Callable, job: "Job") -> None:
        self._start()

        args, kwargs = job.args
        self._pending_jobs[job.id] = job
        submit_task(
            QUEUE_SEND,
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
