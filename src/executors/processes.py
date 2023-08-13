import multiprocessing
import sys
import threading

from typing import Callable

from executors import Logging
from executors import descriptors

from executors.config       import config, CONFIG
from executors.executor     import DUMMY, Executor, RESULT_SENTINEL
from executors.logger       import logger
from executors.process      import ProcessExecutor
from executors.worker       import InProcess
from executors.workers      import Workers

class InChilds(descriptors.InChilds):
    def is_in(self, o, ot) -> bool:
        if not issubclass(ot, ProcessesExecutor):
            raise   TypeError(
                        f"wrong object({o}) type({type(o)}), "
                        "must be subclass of ProcessesExecutor."
                    )
        return multiprocessing.current_process() in o.workers

class ProcessesExecutor(Workers):

    MAX_UNITS   = 61

    in_parent   = descriptors.InParentProcess()
    in_parent_thread = descriptors.InParentThread()
    in_executor = InChilds()
    actives     = descriptors.ActiveProcesses()

    @classmethod
    def init(cls, /, *args, **kwargs) -> bool:
        cls.creator = multiprocessing.Process
        # cls.current = multiprocessing.current_process
        return True
    
    def __init__(
            self,
            max_workers = None,
            parent_pid  = multiprocessing.current_process().ident
            , * ,
            wait = False
        ):
        super(ProcessesExecutor, self).__init__(max_workers, parent_pid)
        # self.parent_pid = parent_pid
        # manager     = multiprocessing.Manager
        self.tasks      = multiprocessing.JoinableQueue()
        self.results    = multiprocessing.Queue()
        self.create     = multiprocessing.Event()   # Create new process

        self.iworkers   = multiprocessing.Value("i", 0)
        self.is_shutdown= multiprocessing.Value("B", 0)
        self.joined     = multiprocessing.Value("B", 0)
        self.lock       = multiprocessing.RLock()

        self._results   = []

    def start(self, wait = True) -> bool:
        if not self.started:
            def monitor(executor):
                return executor._join()
            thread = threading.Thread(target=monitor, args=(self,))
            thread.start()
            self.started = True
        return self.started

    # Override Worker.worker to create dummy-pickleable executor object
    # in new process's memory.
    @staticmethod
    def worker(
            conf,
            tasks,
            results,
            create,
            iworkers,
            is_shutdown,
            joined,
            max_workers,
            parent_pid = multiprocessing.current_process().ident,
        ):
        executor = ProcessesExecutor(max_workers, parent_pid)
        executor.tasks      = tasks
        executor.results    = results
        executor.create     = create
        executor.iworkers   = iworkers
        executor.is_shutdown= is_shutdown
        executor.joined     = joined
        logger.debug(
            f"{Logging.info(ProcessesExecutor.__class__.__name__)}. "
            f"Dummy '{executor.__class__.__name__}' created and setuped."
        )
        # executor.executor = multiprocessing.current_process()
        Workers.worker(executor, conf)#, tasks, results, create)


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

    def _join(self, timeout = None):
        while self.in_bounds:
            logger.debug(
                f"{Logging.info(self.__class__.__name__)}. "
                "Going to wait for creation request."
            )
            while   self.create.wait()      \
                and not self.status()       \
                and self.iworkers.value > 0 \
                and (
                            self.tasks.empty()
                        or  not self.tasks.qsize()
                        or  self.actives < self.iworkers.value # type: ignore
                    )\
                :
                # Event raised, but tasks is empty.
                # Skip child process creation request.
                self.create.clear()
                if self.tasks.empty() or not self.tasks.qsize():
                    logger.debug(
                        f"{Logging.info(self.__class__.__name__)}. "
                        f"Skip creation request. "
                        f"Tasks' count={ self.tasks.qsize()}."
                    )
                elif self.actives < self.iworkers.value: # type: ignore
                    logger.debug(
                        f"{Logging.info(self.__class__.__name__)}. "
                        "Skip creation request. "
                        "Process creation is requested already and in progress."
                    )
                else:
                    logger.debug(
                        f"{Logging.info(self.__class__.__name__)}. "
                        "Going to wait for creation request."
                    )
                continue
            else:
                if self.is_shutdown.value:
                    break
                # Create child processes.
                self.create.clear()
                for i in range(min(self.max_workers - self.iworkers.value, self.tasks.qsize())):
                    # Create configuration for process
                    # (copy module environment to dictionary)
                    # https://peps.python.org/pep-0713/
                    conf =  sys.modules[CONFIG].__call__()  \
                                if CONFIG in sys.modules    \
                                else                        \
                            config
                    worker = self.creator(
                                target  =   self.worker,
                                args    =   (
                                                conf,
                                                self.tasks,
                                                self.results,
                                                self.create,
                                                self.iworkers,
                                                self.is_shutdown,
                                                self.joined,
                                                self.max_workers,
                                                DUMMY, # Create dummy
                                            )
                            )
                    self.iworkers.value += 1 # type: ignore
                    self.workers.append(worker)
                    worker.start()
                    logger.debug(
                        f"{Logging.info(self.__class__.__name__)}. "
                        f"{worker} started."
                    )
    
    """

    """
    def join(self, timeout = None) -> bool:
        if Executor.join(self, timeout):
            self.joined.value = 1
            while self.get_results() or self.actives:
                # self.tasks.put_nowait(TASK_SENTINEL)
                self.results.put_nowait(RESULT_SENTINEL)
            else:
                return Workers.join(self, timeout)
        return False
    
    def submit(self, task: Callable|None, /, *args, **kwargs) -> bool:#, tasks, results, create, /, *args, **kwargs) -> bool:
        
        if task is not None:
        # Remove reference to executor befor adding to tasks' queue.
            # if task is not None and hasattr(task, "executor"):
            task.executor = None
            task.results    = None
            self.tasks.put_nowait(task)
            logger.info(
                f"{Logging.info(self.__class__.__name__)}. "
                f"{task} scheduled. "
            )
            if self.in_bounds:
                self.create.set()
                logger.debug(
                    f"{Logging.info(self.__class__.__name__)}. "
                    f"Process creation requested."
                )
        elif self.iworkers.value or not self.tasks.empty() or self.tasks.qsize(): # type: ignore
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

    def status(self):
        logger.debug(
            f"{Logging.info(self.__class__.__name__)}. "
            "<Status "
                f"tasks={self.tasks.qsize()} "
                f"processes={len(multiprocessing.active_children())}, "
                f"threads={threading.active_count()}, "
                f"workers={self.iworkers.value}" # type: ignore
            ">."
        )

    # @staticmethod
    # def info(name = "") -> str:
    #     process = multiprocessing.current_process()
    #     process = Executor._repr_process(process)
    #     return  f"<{name} process={process}>"\
    #                 if config.DEBUG\
    #                 else\
    #             f"{name}"
