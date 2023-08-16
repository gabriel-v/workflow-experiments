import sys
import typing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import get_context, get_start_method, set_start_method
from typing import Any, Callable, Optional, Tuple

from redun.config import create_config_section
from redun.executors.base import Executor, load_task_module, register_executor
from redun.scripting import exec_script, get_task_command
from redun.task import get_task_registry


if typing.TYPE_CHECKING:
    from redun.scheduler import Job, Scheduler


from qbase import QUEUE_SEND, QUEUE_RECV, submit_tasks, run_worker_until_empty


def exec_task(module_name: str, task_fullname: str, args: Tuple, kwargs: dict) -> Any:
    """
    Execute a task in the new process.
    """
    load_task_module(module_name, task_fullname)
    task = get_task_registry().get(task_fullname)
    return task.func(*args, **kwargs)


def exec_script_task(
    module_name: str, task_fullname: str, args: Tuple, kwargs: dict
) -> bytes:
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
        # self._pending_jobs: Dict[str, "Job"] = OrderedDict()
        self._thread: Optional[threading.Thread] = None

    def stop(self) -> None:
        """
        Stop Executor and monitoring thread.
        """
        self._is_running = False

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
            self._is_running = True
            self._thread = threading.Thread(target=self._monitor, daemon=False)
            self._thread.start()

    def _monitor(self) -> None:
        """
        Thread for monitoring task ack.
        """
        assert self._scheduler

        try:
            while self._is_running:
                # TODO get ack, response, and call remaining stuff
                # if ok: self._scheduler.done_job(job, future.result())
                # if err: self._scheduler.reject_job(job, error)
                pass

        except Exception as error:
            self._scheduler.reject_job(None, error)

        self.log("Shutting down executor...", level=logging.DEBUG)
        self.stop()

    def _submit(self, exec_func: Callable, job: "Job") -> None:
        self._start()
        def on_done(future):
            try:
                self._scheduler.done_job(job, future.result())
            except Exception as error:
                self._scheduler.reject_job(job, error)

        assert job.args
        args, kwargs = job.args
        executor.submit(
            exec_func, job.task.load_module, job.task.fullname, args, kwargs
        ).add_done_callback(on_done)

    def submit(self, job: "Job") -> None:
        assert not job.task.script
        self._submit(exec_task, job)

    def submit_script(self, job: "Job") -> None:
        assert job.task.script
        self._submit(exec_script_task, job)

    def scratch_root(self) -> str:
        return self._scratch_root
