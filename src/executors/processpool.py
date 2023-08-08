import multiprocessing
import concurrent.futures

from typing import Callable

from executors import descriptors

from executors.executor     import TASK_SENTINEL, RESULT_SENTINEL
from executors.pool         import PoolExecutor
from executors.descriptors  import InChilds
from executors.value        import Value


class InProcessPool(InChilds):
    def is_in(self, o, ot) -> bool:
        if not issubclass(ot, ProcessPoolExecutor):
            raise   TypeError(
                        f"wrong object({o}) type({type(o)}), "
                        "must be subclass of ProcessPoolExecutor."
                    )
        return multiprocessing.current_process() in o.executor._processes

class ProcessPoolExecutor(PoolExecutor):

    in_parent   = descriptors.InParentProcess()
    in_executor = InProcessPool()

    @classmethod
    def init(cls, /, *args, **kwargs):
        cls.creator = concurrent.futures.ProcessPoolExecutor
        return True
    
    def __init__(self):
        super(ProcessPoolExecutor, self).__init__()
        self.tasks      = multiprocessing.Queue()
        self.results = multiprocessing.Queue()
        self.is_shutdown= Value(0)

        self._results   = []

    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if self.executor is not None:
            if task is not None:
            task.executor = None # Remove binding for pickle
                task.results    = None # Remove binding for pickle
            return super(ProcessPoolExecutor, self).submit(task, *args, **kwargs)
            else:
                # self.is_shutdown = 1
                self.tasks.put_nowait(TASK_SENTINEL)
                self.results.put_nowait(RESULT_SENTINEL)
                return True
        return False

    def join(self, timeout = None):
        while rv := super(ProcessPoolExecutor, self).join():
            if self.is_shutdown.value:
                self.results.put_nowait(RESULT_SENTINEL)
                break
            while rv and (task := self.get_task()):
                rv = self.submit(task)
            else:
                self.is_shutdown.value = 1
        self.get_results()
        self.joined.value = 1
        return True

    # def get_results(self, block = True, timeout = None) -> list[str]:
    #     if not block:
    #         return self._results#super().get_results(block, timeout)
    #     else:
    #         self.results.put_nowait(RESULT_SENTINEL)
    #         while True:
    #             while result := self.results.get():
    #                 if result != RESULT_SENTINEL:
    #                     self._results.append(result)
    #             if self.results.empty() and not self.results.qsize() and result == RESULT_SENTINEL:
    #                 break
    #         return self._results

    def shutdown(self, wait = True, * , cancel = False) -> bool:
        result = super(ProcessPoolExecutor, self).join()
        result = super(ProcessPoolExecutor, self).shutdown(wait, cancel=cancel)
        # self.get_results()
        return result