import logging
import multiprocessing
# import os
import queue
# import sys
import time
import threading

from typing import Callable

from executors.config import config
from executors.logger import logger
from executors.value  import Value

if __name__ == "__main__":
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)
from executors import Logging

import executors.iexecutor  as iexecutor

TASK_SENTINEL   = None
RESULT_SENTINEL = None

DUMMY = 0

class Executor(iexecutor.IExecutor):

    MAX_TRIES = 1

    creator = None
    # current = None
    # actives = None

    in_parent   = None
    in_executor = None

    # in_main_process = InMainProcess()
    # in_main         = InMain()

    # in_parent_process = InParentProcess()
    # in_parent_thread  = InParentThread()

    @classmethod
    def init(cls, /, *args, **kwargs) -> bool:
        return False

    def __init__(
            self,
            parent_pid = multiprocessing.current_process().ident,
            parent_tid = threading.current_thread().ident
        ):
        
        self.parent     : Executor|None         = None
        self.childs     : dict[str,Executor]    = {}

        self.parent_pid = parent_pid
        self.parent_tid = parent_tid
        
        self.executor   = None

        self.tasks      = None
        self.results    = None
        self.lresults   = []
        
        self.started    = False
        self.joined     = Value(0)

        self.create     = None
        self.lock       = None # TODO: locking for started, joined
        # self.exit       = None
    
    #   TODO: 
    def get_task(self, block = True):
        task = self.tasks.get()  # Get task or wait for new one

        # Filter out all sentinel
        # and push back one on empty tasks' queue
        if task is None:
            if  isinstance(
                    self.tasks,
                    multiprocessing.queues.JoinableQueue
                ):
                self.tasks.task_done()
            # Get and mark all sentinel tasks as done
            tries = 0
            while task is None and tries < self.MAX_TRIES:
                try:
                    task = self.tasks.get_nowait()
                    if task is None:
                        if  isinstance(
                                self.tasks,
                                multiprocessing.queues.JoinableQueue
                            ):
                            self.tasks.task_done()
                        tries = 0
                        logger.debug(
                            f"{Logging.info(self.__class__.__name__)}. "
                            f"TASK_SENTINEL skipped."
                        )
                    else:
                        logger.debug(
                            f"{Logging.info(self.__class__.__name__)}. "
                            f"{task} gotten."
                        )
                        break
                except Exception as e: # Fucking shit
                    task = None
                    tries += 1
                    logger.debug(
                        f"{Logging.info(self.__class__.__name__)}. "
                        f"Got exception: {e}."
                    )
                    if tries < self.MAX_TRIES:
                        time.sleep(tries)
                        continue
                    break
        return task

    def get_results(self, block = True, timeout = None) -> int:#list[str]|queue.Queue:
        processed = 0
        if not block:
            # results = []
            try:
                while True:
                    r = self.results.get_nowait()
                    if      r is not None          \
                        or  r != RESULT_SENTINEL   \
                    :
                        self.lresults.append(r)
                    processed += 1
                # self.results.put_nowait(RESULT_SENTINEL)
            except Exception as e:
                pass
        else:
            while True:
                while result := self.results.get():
                    if result != RESULT_SENTINEL:
                        self.lresults.append(result)
                        processed += 1
                if self.results.empty() and not self.results.qsize() and result == RESULT_SENTINEL:
                    break
        return processed#self.lresults
            # return self.results

    def submit(self, task: Callable|None = None, /, *args, **kwargs) -> bool:
        result = True
        for alias, child in self.childs.items():
            if task is TASK_SENTINEL and not child.submit(task, *args, **kwargs):
                result = False
        return result

    def start(self, wait = True):
        message = ""
        if self.started:
            message += f"{Logging.info(self.__class__.__name__)}. "
            message += f"Executor:{self.executor} is started already."  \
                            if self.executor is not None                \
                            else                                        \
                        "Executor is started already."
        else:
            message +=  f"{Logging.info(self.__class__.__name__)}. "
            message +=  f"Executor:{self.executor} going to start." \
                            if self.executor is not None            \
                            else                                    \
                        "Going to start."
        logger.debug(message)
        return self.started

    def join(self, timeout = None) -> bool:
        if self.joined.value:
            return False
        # If self.parent_pid is real process's pid,
        # checking that it id is equal to creator process.
        if not self.in_parent:
            raise   RuntimeError(\
                        f"join to object({id(self)}) of type {type(self).__name__}', "
                        f"created in process({self.parent_pid}), "
                        f"from process({multiprocessing.current_process().ident}) failed."
                        f"Joining allowed for creator process only."
                    )
        # Check if ProcessesExecutor object created in static method -
        # 'worker' as helper - parameters holder.
        elif self.is_dummy():
            logger.error(
                f"{Logging.info(self.__class__.__name__)}. "
                "Join to dummy."
            )
            return False
        if self.executor is not None:
            info = ""
            info += f"{Logging.info(self.__class__.__name__)}. "
            info += f"Going to wait for {self.executor}" 
            info += f" for {timeout}sec" if timeout else "."
            logger.debug(info)
        # if self.parent is None:
        #     self.submit(TASK_SENTINEL)
        return True

    # def __bool__(self) -> bool:
    #     return False

    # TODO: Rewrite
    def shutdown(self, wait = True, * , cancel = False) -> bool:
        result = False
        if self.tasks is not None:
            self.tasks.put_nowait(TASK_SENTINEL)
        if wait:# and not self.joined.value:
            result = self.join()
        else:
            result = True

        if self.lock is not None:
            self.lock.acquire()

        if self.childs:# and not self.is_dummy():
            if self.is_dummy() or self.iworkers.value <= 1:# and self.is_shutdown.value:
                remove = []
                for alias, child in self.childs.items():
                    child.submit(TASK_SENTINEL)
                    if r := child.shutdown(True, cancel = cancel): # Always wait for childs
                        remove.append(alias)
                        # child.results.put_nowait(RESULT_SENTINEL)
                        results = child.get_results()
                        self.process_results(self.results, child.lresults)
                    else:
                        logger.error(
                            f"{Logging.info(self.__class__.__name__)}. "
                            f"'{alias}' shutdown error."
                        )
                        result = False
                for alias in remove:
                    self.childs.pop(alias)
            else:
                logger.debug(
                    f"{Logging.info(self.__class__.__name__)} "
                    "Skip childs shutdown."
                )

        if self.lock is not None:
            self.lock.release()
        
        logger.debug(
            f"{Logging.info(self.__class__.__name__)} "
            "shutting down."
        )
        self.results.put_nowait(RESULT_SENTINEL)
        return result
    
    @staticmethod
    def process_results(results, result: str|list[str]|queue.Queue) -> int:
        processed = 0
        if id(results) == id(result):
            return processed
        if result is not None and result:
            match type(result).__name__:
                case    "str":
                    results.put_nowait(result)
                    processed = 1
                case    "list":
                    for r in result:
                        results.put_nowait(r)
                    processed = len(result)
                case    _:
                    if hasattr(result, "get_nowait"):#isqueue(result):
                        try:
                            while True:
                                r = result.get_nowait()
                                if      r is not None          \
                                    or  r != RESULT_SENTINEL   \
                                :
                                    results.put_nowait(r)
                                processed += 1
                            # self.results.put_nowait(RESULT_SENTINEL)
                        except Exception as e:
                            pass
        return processed

    def is_dummy(self) -> bool:
        return self.parent_pid == DUMMY

    # def __repr__(self) -> str:
    #     process = self._repr_process()
    #     thread  = self._repr_thread()
    #     return  f"<{self.__class__.__name__} process={process} thread={thread}>"
