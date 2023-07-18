import os

import executors.descriptors as descriptors
from executors.logger import logger
from executors.value import Value
from executors.worker import Worker



"""Workers"""
class Workers(Worker):

    TRY_COUNT_MAX = 3
    max_cpus    = 1

    # in_childs   = descriptors.InChilds()
    # in_child_processes    = descriptors.InChildProcesses()

    # executor_creation   = ExecutorCreationAllowed()
    # executor_counter    = ExecutorCounterInBounds()

    def __init__(self, max_workers = None, /, *args, **kwargs):
        super().__init__()
        self.workers     = []
        self.iworkers    = Value(0)
        self.max_workers = 1

        if max_workers is None:
            if max_workers := os.cpu_count():
                self.max_workers = max_workers
            else:
                self.max_workers = 1
        else:
            self.max_workers = min(self.max_cpus, max_workers)

    def join(self, timeout = None) -> bool:
        if self.in_parent:#_threads or self.in_child_processes:
            raise RuntimeError("can't join from child.")
        if self.iworkers.value >= self.max_workers:
            # All workers created. Join childs.
            # TODO: try_count and timeout is not tested.
            try_count = 0
            while(self.workers and try_count < Workers.TRY_COUNT_MAX):
                remove = []
                for worker in self.workers:
                    info = ""
                    info += f"{self.debug_info(self.__class__.__name__)}. "
                    info += f"Going to wait for {worker}" 
                    info += f" for {timeout}sec" if timeout else "."
                    logger.debug(info)

                    worker.join(timeout)
                    if not worker.is_alive():
                        remove.append(worker)
                    else:
                        logger.debug(
                            f"{self.debug_info(self.__class__.__name__)}. "
                            f"{worker} not complete in {timeout}sec."
                        )
                # Remove finished workers
                for worker in remove:
                    self.workers.remove(worker)
                if timeout is not None and self.workers:
                    logger.debug(
                        f"{self.debug_info(self.__class__.__name__)}. "
                        f"Not all workers complete with "
                        f"{timeout * len(self.workers)}sec. "
                        f"Trying again {try_count}."
                    )
                    try_count += 1
            else:
                if timeout is not None and try_count >= 3:
                    logger.debug(
                        f"{self.debug_info(self.__class__.__name__)}. "
                        f"Not all workers complete with "
                        f"{timeout*len(self.workers)}sec. "
                        f"Try count reached {try_count}. Exiting."
                    )
            return True
        return False
