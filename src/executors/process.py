import multiprocessing
import sys

from typing import Callable

from executors import Logging
from executors import descriptors

from executors.config   import config, CONFIG
from executors.executor import DUMMY
from executors.logger   import logger
from executors.thread   import ThreadExecutor
from executors.worker   import Worker, InProcess



"""Single process"""
class ProcessExecutor(ThreadExecutor):
    
    in_parent   = descriptors.InParentProcess()
    in_executor = InProcess()
    # actives     = ActiveProcesses()

    @classmethod
    def init(cls, /, *args, **kwargs) -> bool:
        cls.creator     = multiprocessing.Process
        # cls.current     = multiprocessing.current_process
        return True

    def __init__(
            self,
            max_workers = None,
            parent_pid  = multiprocessing.current_process().ident
        ):
        super(ProcessExecutor, self).__init__(parent_pid)
        self.max_workers= max_workers
        self.tasks      = multiprocessing.JoinableQueue()
        self.results    = multiprocessing.Queue()
        # self.create     = multiprocessing.Event()   # Create new process
        self.iworkers   = multiprocessing.Value("i", 0)

    @staticmethod
    def worker(
            conf,
            tasks,
            results,
            iworkers,
            max_workers,
            parent_pid = multiprocessing.current_process().ident
        ):
        executor = ProcessExecutor(max_workers, parent_pid)
        executor.tasks      = tasks
        executor.results    = results
        # executor.create     = create
        executor.iworkers   = iworkers
        logger.debug(
            f"{Logging.info(executor.__class__.__name__)}. "
            f"Dummy '{executor.__class__.__name__}' created and setuped."
        )
        executor.executor = multiprocessing.current_process()
        Worker.worker(executor, conf)

    def create_executor(self, /, *args, **kwargs):
        # Create configuration for process
        # (copy module environment to dictionary)
        # https://peps.python.org/pep-0713/
        conf =  sys.modules[CONFIG].__call__()  \
                    if CONFIG in sys.modules    \
                    else                        \
                config
        
        self.executor = self.creator(
                            target  = self.worker,
                            args    = (
                                        conf,
                                        self.tasks,
                                        self.results,
                                        self.iworkers,
                                        1,
                                        DUMMY
                                    ) + args,
                            kwargs  = kwargs
                        )
        if self.executor is not None:
            logger.debug(
                f"{Logging.info(self.__class__.__name__)}. "
                f"Executor:{self.executor} created."
            )
        return self.executor

    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if task is not None and hasattr(task, "executor"):
            task.executor = None
        return super(ProcessExecutor, self).submit(task, *args, **kwargs)
    
    # def __bool__(self) -> bool:
    #     return True

    # def shutdown(self, wait = True, * , cancel = False) -> bool:
    #     result = super(ProcessExecutor, self).shutdown(wait, cancel = cancel)
    #     return result
