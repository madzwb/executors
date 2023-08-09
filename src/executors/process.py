import multiprocessing
import sys
import threading

from typing import Callable

from executors import Logging
from executors import descriptors

from executors.config   import config, CONFIG
from executors.executor import DUMMY, RESULT_SENTINEL, TASK_SENTINEL, Executor
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
            max_workers = 1,
            parent_pid  = multiprocessing.current_process().ident
            , * ,
            wait = False
        ):
        super(ProcessExecutor, self).__init__(parent_pid)
        self.max_workers= max_workers
        self.tasks      = multiprocessing.JoinableQueue()
        self.results    = multiprocessing.Queue()
        # self.create     = multiprocessing.Event()   # Create new process
        self.iworkers   = multiprocessing.Value("i", 0)
        self.is_shutdown= multiprocessing.Value("B", 0)
        self.joined     = multiprocessing.Value("B", 0)
        self.lock       = multiprocessing.Lock()

        # self._results   = []
        # self.tasks.cancel_join_thread()

    # def start(self, wait = True) -> bool:
    #     if not self.started:
    #         super(ProcessExecutor, self).start(wait)
    #         def monitor(executor):
    #             return executor.join()
    #         thread = threading.Thread(target=monitor, args=(self,))
    #         thread.start()
    #         self.started = True
    #     return self.started

    # def get_results(self, block = True, timeout = None) -> list[str]:
    #     if not block:
    #         return self._results#super().get_results(block, timeout)
    #     else:
    #         while True:
    #             while result := self.results.get():
    #                 if result != RESULT_SENTINEL:
    #                     self._results.append(result)
    #             if self.results.empty() and not self.results.qsize() and result == RESULT_SENTINEL:
    #                 break
    #         return self._results

    def join(self, timeout= None) -> bool:
        if self.in_executor:
            raise RuntimeError("can't do self-joining.")
        if Executor.join(self, timeout):#super(ProcessExecutor, self).join(timeout):# self.in_parent:
            # self.joined.value = 1
            self.get_results()  # Wait by results' Queue
            return super(ProcessExecutor, self).join(timeout)   # Wait by process
        return False

    @staticmethod
    def worker(
            conf,
            tasks,
            results,
            iworkers,
            is_shutdown,
            joined,

            max_workers,
            parent_pid = multiprocessing.current_process().ident
        ):
        executor = ProcessExecutor(max_workers, parent_pid)
        executor.tasks      = tasks
        executor.results    = results
        # executor.create     = create
        executor.iworkers   = iworkers
        executor.is_shutdown= is_shutdown
        executor.joined     = joined
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
                                        self.is_shutdown,
                                        self.joined,
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
        self.iworkers.value += 1
        return self.executor

    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if task is not None:# and hasattr(task, "executor"):
            task.executor = None
            task.results    = None
        return super(ProcessExecutor, self).submit(task, *args, **kwargs)
    
    # def __bool__(self) -> bool:
    #     return True

    # def shutdown(self, wait = True, * , cancel = False) -> bool:
    #     result = super(ProcessExecutor, self).shutdown(wait, cancel = cancel)
    #     return result
