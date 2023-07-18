import multiprocessing
import concurrent.futures

from typing import Callable

from executors import descriptors

from executors.pool         import PoolExecutor
from executors.descriptors  import InChilds


class InProcessPool(InChilds):
    def is_in(self, o, ot) -> bool:
        if not issubclass(ot, ProcessPoolExecutor):
            raise   TypeError(
                        f"wrong object({o}) type({type(o)}), "
                        "must be subclass of ProcessPoolExecutor."
                    )
        return multiprocessing.current_process() in o.executor._processes

class ProcessPoolExecutor(PoolExecutor):

    in_parent = descriptors.InParentProcess()

    @classmethod
    def init(cls, /, *args, **kwargs):
        cls.creator = concurrent.futures.ProcessPoolExecutor
        return True
    
    def __init__(self):
        super(ProcessPoolExecutor, self).__init__()

    def join(self, timeout = None) -> bool:
        if not self.in_parent:
            raise RuntimeError("join alowed from parent process(creator) only.")
        if not self.started:
            self.start()
        restult = concurrent.futures.wait(self.futures, timeout, return_when="ALL_COMPLETED")
        return True

    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if self.executor is not None and task is not None:
            task.executor = None # Remove binding for pickle
            return super(ProcessPoolExecutor, self).submit(task, *args, **kwargs)
        return False
