import multiprocessing
import threading
import queue

from typing import Callable

from executors import logger as Logging
from executors import descriptors

from executors.logger   import logger
from executors.executor import Executor

"""Main thread"""
class MainThreadExecutor(Executor):

    in_parent   = descriptors.InParentProcess() # Check if in process-creator
    in_executor = descriptors.InParentThread()  # Check if in thread-creator

    def __init__(self, parent_pid = multiprocessing.current_process().ident):
        super(MainThreadExecutor, self).__init__(parent_pid)
        self.executor   = threading.current_thread()
        self.results    = queue.Queue()
        self.started    = True

    @classmethod
    def init(cls, /, *args, **kwargs) -> bool:
        return True

    def start(self):
        logger.debug(
            f"{Logging.info(self.__class__.__name__)}. "
            f"Executor started."
        )
        return self.started
    
    def join(self, timeout= None) -> bool:
        # if not self.in_main_thread and self.in_parent:# TODO
        #     raise RuntimeError("can't do self-joining.")
        return True
    
    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if task is not None and (self.in_parent or self.in_executor):
            try:
                task.executor = self
                logger.info(
                    f"{Logging.info(self.__class__.__name__)}. "
                    f"{task} processing."
                )
                result = task(*args, **kwargs)
                logger.info(
                    f"{Logging.info(self.__class__.__name__)}. "
                    f"{task} done."
                )
                if self.results is not None and self.results != result:
                    self.results.put_nowait(result)
            except Exception as e:
                result = e
                if self.results is not None:
                    self.results.put_nowait(str(e))
            return True
        else:
            logger.warning(
                f"{Logging.info(self.__class__.__name__)}. "
                f"{task} not scheduled."
            )
            return False

    # def __bool__(self) -> bool:
    #     return True
