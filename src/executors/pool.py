import concurrent.futures
import os
import queue
import threading

from concurrent.futures import Future
from typing import  Callable

from executors import Logging

from executors.executor import Executor, TASK_SENTINEL, RESULT_SENTINEL
from executors.logger   import logger
from executors.value    import Value

class PoolExecutor(Executor):

    event = None

    def __init__(self, max_workers = None):
        super(PoolExecutor, self).__init__()

        self.futures    = []

        self.iworkers   = Value(0)

        self.results    = queue.Queue()
        # self.start()
        if not max_workers:
            if max_workers := os.cpu_count():
                self.max_workers = max_workers
            else:
                self.max_workers = 1
                logger.warning(
                    f"{Logging.info(self.__class__.__name__)}. "
                    f"Max workers set {self.max_workers}."
                )
        else:
            self.max_workers = min(self.MAX_UNITS, max_workers)

    @staticmethod
    def complete_action(future: Future):
        tasks   = None
        results = None
        try:
            results = future.result()
        except Exception as e:
            results = str(e)
            processed = Executor.process_results(future.executor.results, results) # type: ignore
            if processed > 0:
                future.executor.results.put_nowait(RESULT_SENTINEL)
            future.callback_completed.set()
            return
        
        if isinstance(results, tuple):
            tasks   = results[1] if len(results) > 1 else None
            results = results[0] if len(results) > 0 else None

        # Process actions
        if tasks:
            # executor.lock.acquire()
            for task in tasks:
                future.executor.submit(task)
            future.executor.submit(TASK_SENTINEL)

        # Process results
        if results:
            processed = Executor.process_results(future.executor.results, results) # type: ignore
            if processed > 0:
                future.executor.results.put_nowait(RESULT_SENTINEL)

            if hasattr(future, "task"):
                logger.info(
                    f"{Logging.info(PoolExecutor.__name__)}. " # type: ignore
                    f"{future.task} done."
                )
        future.callback_completed.set()

    def start(self, wait = True):
        if self.creator and not super(PoolExecutor, self).start():
            self.executor   = self.creator(self.max_workers)
            self.started = True
            logger.debug(
                f"{Logging.info(self.__class__.__name__)}. "
                f"Start executor{self}."
            )
        return self.started

    def join(self, timeout= None) -> bool:
        if self.in_executor:
            raise RuntimeError("can't join from child.")
        super(PoolExecutor, self).join(timeout)
        result = concurrent.futures.wait(self.futures, timeout, return_when="ALL_COMPLETED")
        # Wait for all 'complete_action' callbacks has be executed
        for future in result.done:
            future.callback_completed.wait()
            self.futures.remove(future)
        return True

    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if self.executor is not None and task is not None:
            logger.info(
                f"{Logging.info(self.__class__.__name__)}. "
                f"{task} scheduled. "
            )
            future = self.executor.submit(task, *args, **kwargs)
            future.callback_completed = self.event()
            setattr(future, "executor", self)
            setattr(future, "task", str(task))
            future.add_done_callback(PoolExecutor.complete_action)
            self.futures.append(future)
            return True
        return False

    def shutdown(self, wait = True, * , cancel = False) -> bool:
        result = super(PoolExecutor, self).shutdown(wait, cancel=cancel)
        if self.executor is not None:
            self.executor.shutdown(wait, cancel_futures=cancel)
            result = True
        else:
            logger.error(f"{Logging.info()}. Shutdown error.")
        return result
