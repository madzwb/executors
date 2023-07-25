import multiprocessing
import threading
import queue

from typing import Callable

from executors import Logging
from executors import descriptors

from executors.logger       import logger
from executors.mainthread   import MainThreadExecutor
from executors.value        import Value
from executors.worker       import Worker, InThread



"""Single thread"""
class ThreadExecutor(MainThreadExecutor, Worker):

    in_parent   = descriptors.InParentThread()
    in_executor = InThread()
    # actives     = ActiveThreads()

    @classmethod
    def init(cls, /, *args, **kwargs) -> bool:
        cls.creator = threading.Thread
        # cls.current = threading.current_thread
        return True

    def __init__(self, parent_pid = multiprocessing.current_process().ident):
        super(ThreadExecutor, self).__init__(parent_pid)
        self.tasks      = queue.Queue()
        self.iworkers   = Value(0)
        self.is_shutdown= Value(0)
         # Reset to None to creaet a new thread in 'submit' method
        self.executor = None
        self.started = False

    def join(self, timeout= None) -> bool:
        if self.in_executor:
            raise RuntimeError("can't do self-joining.")
        if self.executor is not None:
            if not self.started:
                self.start()
            info = ""
            info += f"{Logging.info(self.__class__.__name__)}. "
            info += f"Going to wait for executor:{self.executor}"
            info += f" for {timeout}sec" if timeout else "."
            logger.debug(info)
            self.executor.join(timeout)
            return True
        return False

    def create_executor(self, /, *args, **kwargs):
        self.executor   =  self.creator(
                                target  = self.worker,
                                args    = (self,) + args,
                                kwargs  = kwargs
                            )
        if self.executor is not None:
            logger.debug(
                f"{Logging.info(self.__class__.__name__)}. "
                f"Executor:{self.executor} created."
            )
        return self.executor

    def start(self, wait = True):
        if not self.started:
            if self.executor is None:
                self.create_executor()
            if self.executor is not None:
                self.executor.start()
            logger.debug(
                f"{Logging.info(self.__class__.__name__)}. "
                f"Executor:{self.executor} going to start."
            )
            if wait:
                while not self.executor.is_alive():
                    continue

    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if task is not None:
            # From parent - create thread, put task into queue
            if self.executor is None: #self.in_parent and 
                # task.executor = self
                self.tasks.put_nowait(task)
                logger.info(
                    f"{Logging.info(self.__class__.__name__)}. "
                    f"{task} scheduled."
                )
                self.create_executor(*args, **kwargs)
                self.iworkers.value += 1
            # From created thread(self.executor) - 
            # execute task immediately
            elif self.in_executor:
                logger.debug(
                    f"{Logging.info(self.__class__.__name__)}. "
                    f"Immediately call task:{task}."
                )
                super(ThreadExecutor, self).submit(task, *args, **kwargs)
            # From other threads - put into queue
            else:
                self.tasks.put_nowait(task)
                logger.info(
                    f"{Logging.info(self.__class__.__name__)}. "
                    f"{task} scheduled."
                )
            return True
        elif self.iworkers.value or not self.tasks.empty() or self.tasks.qsize():
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

