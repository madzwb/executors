import multiprocessing
import sys
import threading

from typing import Callable

from executors import descriptors
from executors import logger as Logging

from executors.config       import config, CONFIG
from executors.executor     import DUMMY
from executors.logger       import logger
from executors.workers      import Workers


class InChildProcesses(descriptors.InChilds):
    def is_in(self, o, ot) -> bool:
        if not issubclass(ot, ProcessesExecutor):
            raise   TypeError(
                        f"wrong object({o}) type({type(o)}), "
                        "must be subclass of ProcessesExecutor."
                    )
        return multiprocessing.current_process() in o.workers

class ProcessesExecutor(Workers):

    max_cpus    = 61

    in_parent   = descriptors.InParentProcess()
    in_executor = InChildProcesses()
    actives     = descriptors.ActiveProcesses()

    @classmethod
    def init(cls, /, *args, **kwargs) -> bool:
        cls.creator = multiprocessing.Process
        # cls.current = multiprocessing.current_process
        return True
    
    def __init__(
            self,
            max_workers = None,
            parent_pid = multiprocessing.current_process().ident
        ):
        super(ProcessesExecutor, self).__init__(max_workers, parent_pid)
        # self.parent_pid = parent_pid
        # manager     = multiprocessing.Manager
        self.tasks      = multiprocessing.JoinableQueue()
        self.results    = multiprocessing.Queue()
        self.create     = multiprocessing.Event()   # Create new process

        self.iworkers   = multiprocessing.Value("i", 0)

    def start(self) -> bool:
        if not self.started:
            def monitor(executor):
                return executor.join()
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
            max_workers,
            parent_pid = multiprocessing.current_process().ident
        ):
        executor = ProcessesExecutor(max_workers, parent_pid)
        executor.tasks      = tasks
        executor.results    = results
        executor.create     = create
        executor.iworkers   = iworkers
        logger.debug(
            f"{Logging.info(executor.__class__.__name__)}. "
            f"Dummy '{executor.__class__.__name__}' created and setuped."
        )
        Workers.worker(executor, conf)#, tasks, results, create)

    """

    """
    def join(self, timeout = None) -> bool:
        # If self.parent_pid is real process's pid,
        # checking that it id is equal to creator process.
        if not self.in_parent:
            raise   RuntimeError(\
                        f"join to object({id(self)}) of type {type(self).__name__}', "
                        f"created in process({self.parent_pid}), "
                        f"from process {multiprocessing.current_process().ident} failed."
                        f"Joining allowed for creator process only."
                    )
        # Check if ProcessesExecutor object created in static method -
        # 'worker' as helper - parameters holder.
        elif self.parent_pid is not None and self.parent_pid == 0:
            logger.error(
                f"{Logging.info(self.__class__.__name__)}. "
                "Join to dummy."
            )
            return False
        
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
                if self.actives < self.iworkers.value: # type: ignore
                    logger.debug(
                        f"{Logging.info(self.__class__.__name__)}. "
                        "Skip creation request. "
                        "Process creation is requested already and in progress."
                    )
                logger.debug(
                    f"{Logging.info(self.__class__.__name__)}. "
                    "Going to wait for creation request."
                )
            else:
                # Create child process.
                self.create.clear()
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
                                            self.max_workers,
                                            DUMMY # Create dummy
                                        )
                        )
                self.iworkers.value += 1 # type: ignore
                self.workers.append(worker)
                worker.start()
                logger.debug(
                    f"{Logging.info(self.__class__.__name__)}. "
                    f"{worker} started."
                )
                if super().join(timeout):
                    break
                else:
                    continue
        return True
    
    def submit(self, task: Callable|None, /, *args, **kwargs) -> bool:#, tasks, results, create, /, *args, **kwargs) -> bool:
        # Remove reference to executor befor adding to tasks' queue.
        if task is not None and hasattr(task, "executor"):
            task.executor = None
        
        if task is not None:
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
