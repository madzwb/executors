import os
import queue

from concurrent.futures import Future
from typing import  Callable

from executors.executor import Executor

import executors.logger as Logging
from executors.logger   import logger


class PoolExecutor(Executor):

    def __init__(self):
        super(PoolExecutor, self).__init__()

        self.futures    = []


        self.results    = queue.Queue()
        self.start()

    @staticmethod
    def complete_action(future: Future):
        result = future.result()
        if      result                          \
            and hasattr(future, "parent")       \
            and future.parent is not None       \
            and future.parent.results != result \
        :
            future.parent.results.put_nowait(result) # type: ignore
            logger.info(
                f"{Logging.info(future.parent.__class__.__name__)}. " # type: ignore
                f"{result}"
            )
        else:
            return

    def start(self):
        if self.creator and not self.started:
            self.executor   = self.creator(os.cpu_count())
            self.started = True
        return self.started

    def shutdown(self, wait = True, * , cancel = False) -> bool:
        result = super(PoolExecutor, self).shutdown(wait, cancel=cancel)
        if self.executor is not None:
            self.executor.shutdown(wait, cancel_futures=cancel)
            result = True
        else:
            logger.error(f"{Logging.info()}. Shutdown error.")
        return result

    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if self.executor is not None and task is not None:
            logger.info(
                f"{Logging.info(self.__class__.__name__)}. "
                f"{task} scheduled. "
            )
            future = self.executor.submit(task, *args, **kwargs)
            setattr(future, "parent", self)
            future.add_done_callback(PoolExecutor.complete_action)
            self.futures.append(future)
            return True
        return False
