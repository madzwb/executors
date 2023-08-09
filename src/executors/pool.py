import concurrent.futures
import os
import queue

from concurrent.futures import Future
from typing import  Callable

from executors import Logging

from executors.executor import Executor, TASK_SENTINEL, RESULT_SENTINEL
from executors.logger   import logger
from executors.value    import Value

class PoolExecutor(Executor):

    def __init__(self):
        super(PoolExecutor, self).__init__()

        self.futures    = []

        self.iworkers   = Value(0)

        self.results    = queue.Queue()
        # self.start()

    @staticmethod
    def complete_action(future: Future):
        tasks   = None
        results = None
        try:
            results = future.result()
        except Exception as e:
            results = str(e)
            processed = Executor.process_results(future.results, results) # type: ignore
            if processed > 0:
                future.results.put_nowait(RESULT_SENTINEL)
            return
        
        if isinstance(results, tuple):
            tasks   = results[1] if len(results) > 1 else None
            results = results[0] if len(results) > 0 else None
        # Process results
        if      results                     \
            and hasattr(future, "results")  \
            and hasattr(future, "task")         \
            and future.results is not None  \
            and future.results != results   \
        :
            processed = Executor.process_results(future.results, results) # type: ignore
            if processed > 0:
                future.results.put_nowait(RESULT_SENTINEL)
            logger.info(
                f"{Logging.info(PoolExecutor.__class__.__name__)}. " # type: ignore
                f"{future.task} done."
            )
        # Process actions
        if      tasks                     \
            and hasattr(future, "executor") \
            and future.executor is not None \
        :
            for task in tasks:
                future.executor.submit(task)
            future.executor.submit(TASK_SENTINEL)

    def start(self, wait = True):
        if self.creator and not super(PoolExecutor, self).start():
            self.executor   = self.creator(os.cpu_count())
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
        return True

    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if self.executor is not None and task is not None:
            logger.info(
                f"{Logging.info(self.__class__.__name__)}. "
                f"{task} scheduled. "
            )
            future = self.executor.submit(task, *args, **kwargs)
            setattr(future, "executor", self)
            setattr(future, "results", self.results)
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
