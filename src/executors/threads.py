import threading
import queue

from typing import Callable

from executors import Logging
from executors import descriptors

from executors.logger   import logger
from executors.workers  import Workers


class InChildThreads(descriptors.InChilds):
    def is_in(self, o, ot) -> bool:
        if not issubclass(ot, ThreadsExecutor):
            raise   TypeError(
                        f"wrong object({o}) type({type(o)}), "
                        "must be subclass of ThreadsExecutor."
                    )
        return threading.current_thread() in o.workers



class ThreadsExecutor(Workers):

    max_cpus    = 32

    in_parent   = descriptors.InParentProcess()
    in_executor = InChildThreads()
    # actives     = ActiveThreads()

    @classmethod
    def init(cls) -> bool:
        cls.creator = threading.Thread
        # cls.current = threading.current_thread
        return True

    def __init__(self, max_workers = None):
        super(ThreadsExecutor, self).__init__(max_workers)
        self.tasks      = queue.Queue()
        self.results    = queue.Queue()
        # self.create     = threading.Event()

    def start(self):
        return self.started

    def join(self, timeout= None) -> bool:
        if not self.in_parent:
            raise RuntimeError("can't join from another process.")
        return super(ThreadsExecutor, self).join(timeout)

    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if task is not None:
            if task is not None and hasattr(task, "executor"):
                task.executor = self
            if      self.in_parent\
                and self.in_bounds\
                and (
                            not self.tasks.empty()
                        or  not self.tasks.qsize()
                        # or      self.actives >= self.iworkers.value + 1 # Add main
                        # or      self.iworkers.value == 0
                    )\
            :
                worker = self.creator(
                            target  =   self.worker,
                            args    =   (self,) + args,
                            kwargs  =   kwargs
                         )
                self.iworkers.value += 1
                self.workers.append(worker)
                worker.start()
                self.started = True
                logger.debug(
                    f"{Logging.info(self.__class__.__name__)}. "
                    f"{worker} started."
                )
            self.tasks.put_nowait(task)
            logger.info(
                f"{Logging.info(self.__class__.__name__)}. "
                f"{task} scheduled."
            )
            return True
        elif self.iworkers.value:
            # Put sentinel into queue
            self.tasks.put_nowait(task)
            logger.info(
                f"{Logging.info(self.__class__.__name__)}. "
                f"{task} - sentinel scheduled."
            )
        else:
            logger.warning(
                f"{Logging.info(self.__class__.__name__)}. "
                f"{task} not scheduled."
            )
            return False
        return True

    # def __bool__(self) -> bool:
    #     return True
