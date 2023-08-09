import multiprocessing
import threading
import queue

from typing import Callable

from executors import Logging
from executors import descriptors

from executors.logger   import logger
from executors.executor import Executor
from executors.value    import Value

"""Main thread"""
class MainThreadExecutor(Executor):

    in_parent   = descriptors.InParentProcess() # Check if in process-creator
    in_executor = descriptors.InParentThread()  # Check if in thread-creator

    def __init__(self, parent_pid = multiprocessing.current_process().ident):
        super(MainThreadExecutor, self).__init__(parent_pid)
        self.executor   = threading.current_thread()
        self.results    = queue.Queue()
        self.iworkers   = Value(0)

        # self.start()

    @classmethod
    def init(cls, /, *args, **kwargs) -> bool:
        return True

    def start(self, wait = True):
        if not super(MainThreadExecutor, self).start(wait):
            self.started = True
        return self.started
    
    def join(self, timeout= None) -> bool:
        if Executor.join(self, timeout):
            self.joined.value = 1
            return True
        return False
    
    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        result = super(MainThreadExecutor, self).submit(task, *args, **kwargs)
        if result and task is not None and (self.in_parent or self.in_executor):
            if not self.started:
                self.start()
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
                self.process_results(self.results, result)
                # if self.results is not None and self.results != result:
                #     self.results.put_nowait(result)
            except Exception as e:
                result = str(e)
                # if self.results is not None:
                self.results.put_nowait(str(e))
            return True
        else:
            logger.warning(
                f"{Logging.info(self.__class__.__name__)}. "
                f"{task} not scheduled."
            )
            return False

    # def shutdown(self, wait = True, * , cancel = False) -> bool:
    #     return super(MainThreadExecutor, self).shutdown(wait, cancel = cancel)

    # def __bool__(self) -> bool:
    #     return True
