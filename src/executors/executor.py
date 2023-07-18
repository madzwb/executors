import logging
import multiprocessing
# import os
# import sys
import threading

from executors.config import config
from executors.logger import logger

if __name__ == "__main__":
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

import executors.logger as Logging
import executors.iexecutor  as iexecutor


DUMMY = 0

class Executor(iexecutor.IExecutor):

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
        
        self.started    = False

        self.create     = None
        self.lock       = None # TODO:
        # self.exit       = None
    
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
                        f"{Logging.info(self.__class__.__name__)}. "
                        f"'{alias}' shutdown error."
                    )
                self.process_results(child.results)
            for alias in remove:
                self.childs.pop(alias)
            logger.debug(
                f"{Logging.info(self.__class__.__name__)} "
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

    # def __repr__(self) -> str:
    #     process = self._repr_process()
    #     thread  = self._repr_thread()
    #     return  f"<{self.__class__.__name__} process={process} thread={thread}>"
