import os
import multiprocessing
import threading

from executors import Logging

# from executors  import descriptors

from executors.executor import Executor
from executors.value    import Value
from executors.worker   import Worker
from executors.logger   import logger



class CounterInBounds():

    def __get__(self, o, ot) -> bool:
        if not issubclass(ot, Workers):
            raise   TypeError(
                        f"wrong object({o}) type({type(o)}), "
                        "must be subclass of Workers."
                    )
        return  o.iworkers.value < o.max_workers or  o.iworkers.value == 0



"""Workers"""
class Workers(Worker):

    # MAX_TRIES   = 1
    MAX_UNITS   = 1

    # in_childs   = descriptors.InChilds()
    # in_child_processes    = descriptors.InChildProcesses()

    # executor_creation   = ExecutorCreationAllowed()
    in_bounds = CounterInBounds()

    def __init__(
            self,
            max_workers= None,
            parent_pid = multiprocessing.current_process().ident,
            parent_tid = threading.current_thread().ident
        ):
        super(Workers, self).__init__(parent_pid, parent_tid)
        self.workers    = []
        self.iworkers   = Value(0)
        self.is_shutdown= Value(0)
        self.max_workers= 1

        if not max_workers:
            if max_workers := os.cpu_count():
                self.max_workers = max_workers
            else:
                self.max_workers = 1
        else:
            self.max_workers = min(self.MAX_UNITS, max_workers)

    def join(self, timeout = None) -> bool:
        # self.lock.acquire()
        # if not Executor.join(self, timeout):
        #     return False
        self.executor = None
        # if super(Workers, self).join(timeout):
        # TODO: try_count and timeout is not tested.
        tries = 0
        while(self.workers and tries < Workers.MAX_TRIES):
            remove = []
            for worker in self.workers:
                    self.executor = worker
                    if super(Workers, self).join(timeout):
                        remove.append(self.executor)
            # Remove finished workers
            for worker in remove:
                self.workers.remove(worker)
            if self.workers:
                tries += 1
            # else:
        result = len(self.workers) == 0
        # self.lock.release()
        return result

    # def shutdown(self, wait = True, * , cancel = False) -> bool:
    #     return super(Workers, self).shutdown(wait, cancel = cancel)
    
