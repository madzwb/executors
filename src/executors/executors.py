from __future__ import annotations

import concurrent.futures
import multiprocessing
import multiprocessing.queues
import os
import logging
import queue
import sys
import threading
import time

from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import Any, Callable


from executors.iexecutor import IExecutor

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
if __name__ == "__main__":
    # import config
    import registrator.registrator as registrator
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)
else:
    CONFIG = "config"
    if  \
            CONFIG not in sys.modules\
        or  CONFIG not in globals()\
        or  CONFIG not in locals()\
    :
        class Config:
            DEBUG = True\
                        if hasattr(sys, "gettrace") and sys.gettrace()\
                        else\
                    False
            
        config = Config()
    logger.addHandler(logging.NullHandler())


DUMMY = 0

def is_queue(o: Any) -> bool:
    return      hasattr(o, "put")   \
            or  hasattr(o, "get")   \
            or  hasattr(o, "empty") \
            or  hasattr(o, "full")  \
            or  hasattr(o, "qsize") \
            or  hasattr(o, "get_nowait") \
            or  hasattr(o, "put_nowait")






class InMainProcess():

    def __get__(self, o, ot) -> bool:
        return  not multiprocessing.parent_process()\
                or  multiprocessing.current_process().name == "MainProcess"\



class InMainThread():

    def __get__(self, o, ot) -> bool:
        return  threading.main_thread() == threading.current_thread()



class InParent(ABC):
    @abstractmethod
    def pid(self, o) -> bool: ...

    #   parent_pid == 0 - has special behavior!!!
    def __get__(self, o, ot) -> bool:
        if not issubclass(ot, Executor):
            raise   TypeError(
                        f"wrong object({o}) type({type(o)}), "
                        "must be subclass of Executor."
                    )
        return      hasattr(o,"parent_pid")     \
                and o.parent_pid is not None    \
                and o.parent_pid > 0            \
                and self.pid(o)



class InParentProcess(InParent):
    def pid(self, o) -> bool:
        return multiprocessing.current_process().ident == o.parent_pid



class InParentThread(InParent):
    def pid(self, o) -> bool:
        return True



class InThread():

    def __get__(self, o, ot) -> bool:
        if      not issubclass(ot, ThreadExecutor)\
            and not issubclass(ot, MainThreadExecutor)\
        :
            raise   TypeError(
                        f"wrong object({o}) type({type(o)}), "
                        "must be subclass of ThreadExecutor or MainThreadExecutor."
                    )
        return      o.executor is not None\
                and o.executor == threading.current_thread()



class InProcess():

    def __get__(self, o, ot) -> bool:
        if not issubclass(ot, ProcessExecutor):
            raise   TypeError(
                        f"wrong object({o}) type({type(o)}), "
                        "must be subclass of ProcessExecutor."
                    )
        return      o.executor is not None\
                and o.executor == multiprocessing.current_process()



class InMain():

    def __get__(self, o, ot) -> bool:
        if not issubclass(ot, ProcessesExecutor):
            raise   TypeError(
                        f"wrong object({o}) type({type(o)}), "
                        "must be subclass of ProcessExecutor."
                    )
        return  (not multiprocessing.parent_process()\
                or  multiprocessing.current_process().name == "MainProcess")\
                and threading.main_thread() == threading.current_thread()



class InChildThreads():
    def __get__(self, o, ot) -> bool:
        return threading.current_thread() in o.workers

class InChildProcesses():
    def __get__(self, o, ot) -> bool:
        return multiprocessing.current_process() in o.workers

class ExecutorCounterInBounds():

    def __get__(self, o, ot) -> bool:
        if not issubclass(ot, Workers):
            raise   TypeError(
                        f"wrong object({o}) type({type(o)}), "
                        "must be subclass of Workers."
                    )
        return  o.iworkers.value < o.max_workers or  o.iworkers.value == 0



class ExecutorCreationAllowed():

    def __get__(self, o, ot) -> bool:
        if not issubclass(ot, Workers):
            raise   TypeError(
                        f"wrong object({o}) type({type(o)}), "
                        "must be subclass of Workers."
                    )
        return      o.iworkers.value < o.max_workers\
                and (
                        not o.tasks.empty()
                        or  o.tasks.qsize()
                    )\
                # and o.iworkers.value <= len(multiprocessing.active_children())



class Actives(ABC):
    @classmethod
    @abstractmethod
    def len(cls) -> int: ...

    def __get__(self, o ,ot) -> int:
        return self.len()

class ActiveThreads(Actives):

    @classmethod
    def len(cls) -> int:
        return threading.active_count()
    # len(threading.enumerate())

class ActiveProcesses(Actives):

    @classmethod
    def len(cls) -> int:
        return len(multiprocessing.active_children())


class Executor(IExecutor):

    creator = None
    # current = None
    actives = None

    in_parent   = None
    in_executor = None

    in_main_process = InMainProcess()
    in_main_thread  = InMainThread()
    in_main         = InMain()

    # in_parent_process = InParentProcess()
    # in_parent_thread  = InParentThread()

    @classmethod
    def init(cls, /, *args, **kwargs) -> bool:
        return False

    def __init__(self):
        self.parent     = None
        self.parent_pid = None
        self.parent_tid = None
        self.executor   = None
        self.tasks      = None
        self.results    = None
        self.create     = None
        self.childs     = {} # class Executor
        self.lock       = None # TODO:
        # self.exit       = None
        self.started    = False
    
    # def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
    #     return False

    # def join(self, timeout = None) -> bool:
    #     return False

    # def __bool__(self) -> bool:
    #     return False

    # TODO: Rewrite
    def shutdown(self, wait = True, * , cancel = False) -> bool:
        result = False
        if self.childs:# and not self.is_dummy():
            remove = []
            for alias, child in self.childs.items():
                if r := child.shutdown(wait, cancel = cancel):
                    remove.append(alias)
                else:
                    logger.error(
                        f"{Executor.debug_info(self.__class__.__name__)}. "
                        f"'{alias}' shutdown error."
                    )
                self.process_results(child.results)
            for alias in remove:
                self.childs.pop(alias)
            logger.debug(
                f"{Executor.debug_info(self.__class__.__name__)} "
                "shutted down."
            )
        
        if self.executor is not None and not self.is_dummy():
            if self.tasks:
                self.tasks.put_nowait(None)
            if wait and not self.in_executor and hasattr(self.executor, "join"):
                self.executor.join(wait)
                result = True
            else:   # TODO
                pass#self.executor.daemon = True        return result
        else:
            result = True
        return result
    
    def process_results(self, results) -> int:
        processed = 0
        if self.results is not None and results:
            match type(results).__name__:
                case    "str":
                    self.results.put_nowait(results)
                    processed = 1
                case    "list":
                    for result in results:
                        self.results.put_nowait(result)
                    processed = len(results)
                case    _:
                    if hasattr(results, "get_nowait"):#isqueue(result):
                        try:
                            while result := results.get_nowait():
                                self.results.put_nowait(result)
                                processed += 1
                        except Exception as e:
                            pass
        return processed

    def is_dummy(self) -> bool:
        return self.parent_pid == DUMMY

    @staticmethod
    def _repr_process(process = multiprocessing.current_process()) -> str:
        return "<Process "\
                    f"name='{process.name}' "\
                    f"pid={process.ident} "\
                    f"parent={process._parent_pid}"\
                ">"
    
    @staticmethod
    def _repr_thread(thread  = threading.current_thread()) -> str:
        return "<Thread "\
                    f"name='{thread.name}' "\
                    f"pid={thread.ident}"\
                ">"

    def __repr__(self) -> str:
        process = self._repr_process()
        thread  = self._repr_thread()
        return  f"<{self.__class__.__name__} process={process} thread={thread}>"
    
    @staticmethod
    def debug_info(name = "") -> str:
        process = multiprocessing.current_process()
        thread  = threading.current_thread()
        process = Executor._repr_process(process)
        thread  = Executor._repr_thread(thread)
        return f"<{name} process={process} thread={thread}>"\
                    if config.DEBUG\
                    else\
                f"{name}"
    


class PoolExecutor(Executor):

    def __init__(self, parent_pid = None):
        super(PoolExecutor, self).__init__()

        self.parent_pid = parent_pid
        self.futures    = []


        self.results    = queue.Queue()
        self.start()

    @staticmethod
    def complete_action(future: Future):
        result = future.result()
        if      result                          \
            and hasattr(future, "parent")       \
            and future.parent                   \
            and future.parent.results != result \
        :
            future.parent.results.put_nowait(result)
            logger.info(
                f"{future.parent.debug_info(future.parent.__class__.__name__)}. "
                f"{result}"
            )
        else:
            return

    def start(self):
        if self.creator and not self.started:
            self.executor   = self.creator(os.cpu_count())
            self.started = True
        return self.started

    def shutdown(self, wait = True, * , cancel = False) -> bool:
        result = super(PoolExecutor, self).shutdown(wait, cancel=cancel)
        if self.executor is not None:
            # print(f"Futures size: {len(cls.futures)}.")
            # count = len(cls.futures)
            self.executor.shutdown(wait, cancel_futures=cancel)
            result = True
        else:
            logger.error(f"{self.debug_info()}. Shutdown error.")
        return result

    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if self.executor is not None and task is not None:
            logger.info(
                f"{self.debug_info(self.__class__.__name__)}. "
                f"{task} scheduled. "
            )
            future = self.executor.submit(task, *args, **kwargs)
            setattr(future, "parent", self)
            future.add_done_callback(PoolExecutor.complete_action)
            self.futures.append(future)
            return True
        return False

class ThreadPoolExecutor(PoolExecutor):

    # in_parent_thread = InParentThread()
    # in_parent_process = InParentProcess()

    @classmethod
    def init(cls, /, *args, **kwargs) -> bool:
        cls.creator = concurrent.futures.ThreadPoolExecutor
        return True

    def __init__(self, parent_pid = threading.current_thread().ident):
        super(ThreadPoolExecutor, self).__init__(parent_pid)

    def join(self, timeout= None) -> bool:
        if threading.current_thread() in self.executor._threads:
            raise RuntimeError("can't join from child.")
        if not self.started:
            self.start()
        result = concurrent.futures.wait(self.futures, timeout, return_when="ALL_COMPLETED")
        return True
    
    # def __bool__(self) -> bool:
    #     return True



class ProcessPoolExecutor(PoolExecutor):

    in_parent = InParentProcess()

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



"""Worker"""
class Worker(Executor):
    
    TRIES = 0

    executor_creation   = True
    executor_counter    = True

    def __init__(self):
        super(Worker, self).__init__()

    @staticmethod
    def worker(executor: Executor, conf = None, /, *args, **kwargs):#, tasks, results, create):
        caller = executor.__class__.__name__
        if      executor.tasks      is None \
            or  executor.results    is None \
        :
            raise   RuntimeError(\
                        f"not setuped tasks' queue."
                            if executor.tasks is None
                            else
                        ""
                        f"not setuped results' queue."
                            if executor.tasks is None
                            else
                        ""
                    )
        # Update 'config' module with conf
        if CONFIG in sys.modules and conf is not None:
            sys.modules[CONFIG].__call__(conf)
            # logger_init()
            logger.debug(f"{Executor.debug_info(caller)} logging prepeared.")

        logger.debug(f"{Executor.debug_info(caller)} started.")
        executor.started = True
        
        while True:
            task = None
            logger.debug(
                f"{Worker.debug_info(caller)}. "
                "<Status "
                    f"tasks={executor.tasks.qsize()} "
                    f"results={executor.results.qsize()}"
                ">."
            )
            task = executor.tasks.get()  # Get task or wait for new one

            # Filter out all sentinel
            # and push back one on empty tasks' queue
            if task is None:
                if  isinstance(
                        executor.tasks,
                        multiprocessing.queues.JoinableQueue
                    ):
                    executor.tasks.task_done()
                # Get and mark all sentinel tasks as done
                tries = 0
                while task is None:
                    try:
                        task = executor.tasks.get_nowait()
                        if task is None:
                            if  isinstance(
                                    executor.tasks,
                                    multiprocessing.queues.JoinableQueue
                                ):
                                executor.tasks.task_done()
                        else:
                            break
                    except queue.Empty as e:
                        task = None
                        if tries < Worker.TRIES:
                            tries += 1
                            time.sleep(tries)
                            continue
                        # On empty put back sentinel
                        executor.tasks.put_nowait(None)
                        break
            
            if task is not None:
                # TODO: Move out.
                if executor.create is not None and executor.executor_creation:
                    logger.debug(f"{Worker.debug_info(caller)} creation requested. ")
                    executor.create.set()
                # Call task
                try:
                    # Set executor for subtasks submitting
                    task.executor = executor
                    logger.info(
                        f"{Worker.debug_info(caller)}. "
                        f"{task} processing."
                    )
                    # start = 0
                    # if sys.getprofile() is not None:
                    #     start = time.time()
                    result = task.__call__()#None, tasks, results, create)
                    info = f"{Worker.debug_info(caller)}. {task} done"
                    # if sys.getprofile() is not None:
                    #     end = time.time()
                    #     delta = end - start
                    #     info += f"with str(time)s"
                    info += "."
                    logger.info(info)
                except Exception as e:
                    result = str(e)
                    logger.error(f"{Worker.debug_info(caller)}. {result}.")
                
                if  isinstance(
                        executor.tasks,
                        multiprocessing.queues.JoinableQueue
                    ):
                    executor.tasks.task_done()
                # Process results
                if result and result != executor.results:# and isinstance(result, str):
                    executor.process_results(result)
            else:
                logger.debug(
                    f"{Executor.debug_info(caller)} "
                    f"got sentinel. Exiting."
                )
                break
        # Worker done
        logger.debug(f"{Worker.debug_info(caller)} done.")
        logger.debug(
            f"{Worker.debug_info(caller)}. "
            "<Status "
                f"tasks={executor.tasks.qsize()} "
                f"results={executor.results.qsize()}"
            ">."
        )
        if executor.create is not None:
            logger.debug(f"{Worker.debug_info(caller)} exiting signaled. ")
            executor.create.set()
        executor.shutdown()
        return


class Value():

    def __init__(self,_value: int) -> None:
        self._value = _value

    @property
    def value(self) -> int:
        return self._value
    
    @value.setter
    def value(self, value: int) -> None:
        self._value = value

"""Workers"""
class Workers(Worker):

    TRY_COUNT_MAX = 3
    max_cpus    = 1

    in_threads      = InChildThreads()
    in_processes    = InChildProcesses()

    executor_creation   = ExecutorCreationAllowed()
    executor_counter    = ExecutorCounterInBounds()

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
        if self.in_threads or self.in_processes:
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

"""Main thread"""
class MainThreadExecutor(Executor):

    in_parent   = InParentThread() # Always True
    in_executor = InThread()

    def __init__(self, parent_pid = threading.current_thread().ident):
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
            f"{self.debug_info(self.__class__.__name__)}. "
            f"Executor started."
        )
        return self.started
    
    def join(self, timeout= None) -> bool:
        if not self.in_main_thread and self.in_parent:# TODO
            raise RuntimeError("can't do self-joining.")
        return True
    
    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if task is not None and (self.in_parent or self.in_executor):
            try:
                task.executor = self
                logger.info(
                    f"{self.debug_info(self.__class__.__name__)}. "
                    f"{task} processing."
                )
                result = task(*args, **kwargs)
                logger.info(
                    f"{self.debug_info(self.__class__.__name__)}. "
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
                f"{self.debug_info(self.__class__.__name__)}. "
                f"{task} not scheduled."
            )
            return False

    # def __bool__(self) -> bool:
    #     return True



"""Single thread"""
class ThreadExecutor(MainThreadExecutor, Worker):

    in_executor = InThread()
    in_parent   = InParentThread()
    actives     = ActiveThreads()

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
            info += f"{self.debug_info(self.__class__.__name__)}. "
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
                f"{self.debug_info(self.__class__.__name__)}. "
                f"Executor:{self.executor} created."
            )
        return self.executor

    def start(self):
        if not self.started and self.executor is not None:
            self.executor.start()
            logger.debug(
                f"{self.debug_info(self.__class__.__name__)}. "
                f"Executor:{self.executor} going to start."
            )

    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        if task is not None:
            # From parent - create thread, put task into queue
            if self.in_parent and self.executor is None:
                # task.executor = self
                self.tasks.put_nowait(task)
                logger.info(
                    f"{self.debug_info(self.__class__.__name__)}. "
                    f"{task} scheduled."
                )
                self.create_executor(*args, **kwargs)
                self.iworkers.value += 1
            # From created thread(self.executor) - 
            # execute task immediately
            elif self.in_executor:
                logger.debug(
                    f"{self.debug_info(self.__class__.__name__)}. "
                    f"Immediately call task:{task}."
                )
                super(ThreadExecutor, self).submit(task, *args, **kwargs)
            # From other threads - put into queue
            else:
                self.tasks.put_nowait(task)
                logger.info(
                    f"{self.debug_info(self.__class__.__name__)}. "
                    f"{task} scheduled."
                )
            return True
        elif self.iworkers.value or not self.tasks.empty() or self.tasks.qsize():
            # Put sentinel into queue
            self.tasks.put_nowait(task)
            logger.info(
                f"{self.debug_info(self.__class__.__name__)}. "
                f"{task} - sentinel scheduled."
            )
        else:
            logger.warning(
                f"{self.debug_info(self.__class__.__name__)}. "
                f"{task} not scheduled."
            )
            return False
        return True

    # def __bool__(self) -> bool:
    #     return True



# TODO:
"""Single process"""
class ProcessExecutor(ThreadExecutor):
    
    in_executor = InProcess()
    in_parent   = InParentProcess()
    actives     = ActiveProcesses()

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
            f"{ProcessesExecutor.debug_info(executor.__class__.__name__)}. "
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
                f"{self.debug_info(self.__class__.__name__)}. "
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

    in_parent           = InParentThread() # Always True
    actives             = ActiveThreads()

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
                    f"{self.debug_info(self.__class__.__name__)}. "
                    f"{worker} started."
                )
            self.tasks.put_nowait(task)
            logger.info(
                f"{self.debug_info(self.__class__.__name__)}. "
                f"{task} scheduled."
            )
            return True
        elif self.iworkers.value:
            # Put sentinel into queue
            self.tasks.put_nowait(task)
            logger.info(
                f"{self.debug_info(self.__class__.__name__)}. "
                f"{task} - sentinel scheduled."
            )
        else:
            logger.warning(
                f"{self.debug_info(self.__class__.__name__)}. "
                f"{task} not scheduled."
            )
            return False
        return True

    # def __bool__(self) -> bool:
    #     return True


class ProcessesExecutor(Workers):

    max_cpus    = 61

    in_parent           = InParentProcess()
    actives             = ActiveProcesses()

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

    # def __reduce__(self) -> str | tuple[Any, ...]:
    #     return super().__reduce__()

    # @classmethod
    # def join(cls, worker):
    #     # pid = multiprocessing.current_process()
    #     # parent = multiprocessing.parent_process()

    #     if not multiprocessing.parent_process() or multiprocessing.current_process().name == "MainProcess":
    #     # if threading.current_thread().name == "MainProcess":
    #         worker.join()
    #         # while True:
    #         #     print(f"Process:{multiprocessing.current_process()}. Try to wait on tasks: '{cls.tasks}'.")
    #         #     cls.tasks.join()
    #         #     print(f"All tasks: '{cls.tasks}' done.")
    #         #     active = multiprocessing.active_children()
    #         #     if active:
    #         #         for child in active:
    #         #             print(f"Process:{multiprocessing.current_process()}. Try to wait on child process: '{child}'.")
    #         #             child.join(60)
    #         #             print(f"Process:{multiprocessing.current_process()}. Wait for child process: '{child}' timedout.")
    #         #         continue
    #         #     else:
    #         #         break
    #     return

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
            f"{ProcessesExecutor.debug_info(executor.__class__.__name__)}. "
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
                f"{self.debug_info(self.__class__.__name__)}. "
                "Join to dummy."
            )
            return False
        
        while self.executor_counter:
            logger.debug(
                f"{self.debug_info(self.__class__.__name__)}. "
                "Going to wait for creation request."
            )
            while   self.create.wait()\
                and not self.status()\
                and self.iworkers.value > 0\
                and (
                            self.tasks.empty()
                        or  not self.tasks.qsize()# <= 1
                        or  self.actives < self.iworkers.value # Process in starting
                    )\
                :
                # Event raised, but tasks is empty.
                # Skip child process creation request.
                self.create.clear()
                if self.tasks.empty() or self.tasks.qsize() <= 1:
                    logger.debug(
                        f"{self.debug_info(self.__class__.__name__)}. "
                        f"Skip creation request. "
                        f"Tasks' count={ self.tasks.qsize()}."
                    )
                if len(multiprocessing.active_children()) < self.iworkers.value:
                    logger.debug(
                        f"{self.debug_info(self.__class__.__name__)}. "
                        "Skip creation request. "
                        "Process creation is requested already and in progress."
                    )
                logger.debug(
                    f"{self.debug_info(self.__class__.__name__)}. "
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
                self.iworkers.value += 1
                self.workers.append(worker)
                worker.start()
                logger.debug(
                    f"{self.debug_info(self.__class__.__name__)}. "
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
                f"{self.debug_info(self.__class__.__name__)}. "
                f"{task} scheduled. "
            )
            if create is not None:
                self.create.set()
                logger.debug(
                    f"{self.debug_info(self.__class__.__name__)}. "
                    f"Process creation requested."
                )
        elif self.iworkers.value or not self.tasks.empty() or self.tasks.qsize():
            # Put sentinel into queue
            self.tasks.put_nowait(task)
            logger.info(
                f"{self.debug_info(self.__class__.__name__)}. "
                f"{task} - sentinel scheduled."
            )
        else:
            logger.warning(
                f"{self.debug_info(self.__class__.__name__)}. "
                f"{task} not scheduled."
            )
            return False
        return True
    
    # def __bool__(self) -> bool:
    #     return True

    def status(self):
        logger.debug(
            f"{self.debug_info(self.__class__.__name__)}. "
            "<Status "
                f"tasks={self.tasks.qsize()} "
                f"processes={len(multiprocessing.active_children())}, "
                f"threads={threading.active_count()}, "
                f"workers={self.iworkers.value}"
            ">."
        )

    @staticmethod
    def debug_info(name = "") -> str:
        process = multiprocessing.current_process()
        process = Executor._repr_process(process)
        return  f"<{name} process={process}>"\
                    if config.DEBUG\
                    else\
                f"{name}"

if __name__ == "__main__":
    class EXECUTORS(registrator.REGISTRATOR): ...

    EXECUTORS.register("Executor", __name__, globals(), Executor)
    registry = EXECUTORS()
    logger.debug(f"Executors registered:{EXECUTORS()}.")

# pass
