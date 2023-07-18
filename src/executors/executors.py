from __future__ import annotations

import concurrent.futures
import multiprocessing
import multiprocessing.queues
import os
# import logging
import queue
import sys
import threading
# import time

# from abc import ABC, abstractmethod
# from concurrent.futures import Future
from typing import Callable


import executors.descriptors as descriptors

import executors.logger as Logging
from executors.logger   import logger

from executors.config       import config, CONFIG
from executors.executor     import Executor, DUMMY
from executors.pool         import PoolExecutor
from executors.value        import Value
from executors.worker       import Worker
from executors.workers      import Workers

from registrator import registrator


class ThreadPoolExecutor(PoolExecutor):

    in_parent = descriptors.InParentThread()
    in_childs = descriptors.InThreadPool()
    # in_parent_thread = InParentThread()
    # in_parent_process = InParentProcess()

    @classmethod
    def init(cls, /, *args, **kwargs) -> bool:
        cls.creator = concurrent.futures.ThreadPoolExecutor
        return True

    def __init__(self, parent_pid = threading.current_thread().ident):
        super(ThreadPoolExecutor, self).__init__(parent_pid)

    def join(self, timeout= None) -> bool:
        if self.in_childs:
            raise RuntimeError("can't join from child.")
        if not self.started:
            self.start()
        result = concurrent.futures.wait(self.futures, timeout, return_when="ALL_COMPLETED")
        return True
    
    # def __bool__(self) -> bool:
    #     return True



class ProcessPoolExecutor(PoolExecutor):

    in_parent = descriptors.InParentProcess()

    @classmethod
    def init(cls, /, *args, **kwargs):
        cls.creator = concurrent.futures.ProcessPoolExecutor
        return True
    
    def __init__(self, parent_pid = multiprocessing.current_process().ident):
        super(ProcessPoolExecutor, self).__init__(parent_pid)

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




"""Main thread"""
class MainThreadExecutor(Executor):

    # in_main_thread  = InMainThread()
    in_parent   = descriptors.InParentThread() # Always True
    # in_executor = InThread()

    def __init__(
            self,
            parent_pid = multiprocessing.current_process().ident,
            parent_tid = threading.current_thread().ident,
        ):
        super().__init__()
        self.executor   = threading.current_thread()
        self.parent_pid = parent_pid
        self.results    = queue.Queue()
        self.started    = True

    @classmethod
    def init(cls, /, *args, **kwargs) -> bool:
        # cls.current     = threading.current_thread
        return True

    def start(self):
        logger.debug(
            f"{Logging.info(self.__class__.__name__)}. "
            f"Executor started."
        )
        return self.started
    
    def join(self, timeout= None) -> bool:
        # if not self.in_main_thread and self.in_parent:# TODO
        #     raise RuntimeError("can't do self-joining.")
        return True
    
    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if task is not None and (self.in_parent or self.in_executor):
            try:
                task.executor = self
                logger.info(
                    f"{Logging.info(self.__class__.__name__)}. "
                    f"{task} processing."
                )
                result = task(*args, **kwargs)
                logger.info(
                    f"{Logging.info(self.__class__.__name__)}. "
                    f"{task} done."
                )
                if self.results is not None and self.results != result:
                    self.results.put_nowait(result)
            except Exception as e:
                result = e
                if self.results is not None:
                    self.results.put_nowait(str(e))
            return True
        else:
            logger.warning(
                f"{Logging.info(self.__class__.__name__)}. "
                f"{task} not scheduled."
            )
            return False

    # def __bool__(self) -> bool:
    #     return True



"""Single thread"""
class ThreadExecutor(MainThreadExecutor, Worker):

    # in_executor = InThread()
    in_parent   = descriptors.InParentThread()
    # actives     = ActiveThreads()

    @classmethod
    def init(cls, /, *args, **kwargs) -> bool:
        cls.creator = threading.Thread
        # cls.current = threading.current_thread
        return True

    def __init__(self, parent_pid = threading.current_thread().ident):
        super(ThreadExecutor, self).__init__(parent_pid)
        self.tasks      = queue.Queue()
        self.iworkers   = Value(0)
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

    def start(self):
        if not self.started and self.executor is not None:
            self.executor.start()
            logger.debug(
                f"{Logging.info(self.__class__.__name__)}. "
                f"Executor:{self.executor} going to start."
            )

    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if task is not None:
            # From parent - create thread, put task into queue
            if self.in_parent and self.executor is None:
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



# TODO:
"""Single process"""
class ProcessExecutor(ThreadExecutor):
    
    # in_executor = InProcess()
    in_parent   = descriptors.InParentProcess()
    # actives     = ActiveProcesses()

    @classmethod
    def init(cls, /, *args, **kwargs) -> bool:
        cls.creator     = multiprocessing.Process
        # cls.current     = multiprocessing.current_process
        return True

    def __init__(
            self,
            max_workers = None,
            parent_pid = multiprocessing.current_process().ident
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
    


class ThreadsExecutor(Workers):

    max_cpus    = 32

    # in_parent   = descriptors.InParentThread() # Always True
    # in_childs   = descriptors.InChildThreads()
    # actives     = ActiveThreads()

    @classmethod
    def init(cls) -> bool:
        cls.creator = threading.Thread
        # cls.current = threading.current_thread
        return True

    def __init__(self, max_workers = None, parent_pid = threading.current_thread().ident, /, *args, **kwargs):
        super().__init__(max_workers, *args, **kwargs)
        self.parent_pid = parent_pid
        self.tasks      = queue.Queue()
        self.results    = queue.Queue()
        # self.create     = threading.Event()

    def start(self):
        return self.started

    def join(self, timeout= None) -> bool:
        return super(ThreadsExecutor, self).join(timeout)

    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if task is not None:
            if task is not None and hasattr(task, "executor"):
                task.executor = self
            if      self.in_parent\
                and self.executor_counter\
                and (
                            not self.tasks.empty()
                        or  not self.tasks.qsize()
                        # or      self.actives >= self.iworkers.value + 1 # Add main
                        or      self.iworkers.value == 0
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


class ProcessesExecutor(Workers):

    max_cpus    = 61

    # is_in_parent           = descriptors.InParentProcess()
    # actives             = descriptors.ActiveProcesses()
    in_childs   = descriptors.InChildProcesses()

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
        super(ProcessesExecutor, self).__init__(max_workers)
        self.parent_pid = parent_pid
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
        
        while self.executor_counter:
            logger.debug(
                f"{Logging.info(self.__class__.__name__)}. "
                "Going to wait for creation request."
            )
            while   self.create.wait()      \
                and not self.status()       \
                and self.iworkers.value > 0 \
                and (
                            self.tasks.empty()
                        or  not self.tasks.qsize()# <= 1
                        or  self.actives < self.iworkers.value # Process in starting # type: ignore
                    )\
                :
                # Event raised, but tasks is empty.
                # Skip child process creation request.
                self.create.clear()
                if self.tasks.empty() or self.tasks.qsize() <= 1:
                    logger.debug(
                        f"{Logging.info(self.__class__.__name__)}. "
                        f"Skip creation request. "
                        f"Tasks' count={ self.tasks.qsize()}."
                    )
                if len(multiprocessing.active_children()) < self.iworkers.value: # type: ignore
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
            create = self.executor_creation
            # if create:
            #     self.tasks.put(task)
            # else:
            self.tasks.put_nowait(task)
            logger.info(
                f"{Logging.info(self.__class__.__name__)}. "
                f"{task} scheduled. "
            )
            if create is not None:
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

if __name__ == "__main__":
    class EXECUTORS(registrator.REGISTRATOR): ...

    EXECUTORS.register("Executor", __name__, globals(), Executor)
    registry = EXECUTORS()
    logger.debug(f"Executors registered:{EXECUTORS()}.")

# pass
