import multiprocessing
import threading

from abc    import ABC, abstractmethod
# from typing import Any

from executors.executor import Executor

# def is_queue(o: Any) -> bool:
#     return      hasattr(o, "put")   \
#             or  hasattr(o, "get")   \
#             or  hasattr(o, "empty") \
#             or  hasattr(o, "full")  \
#             or  hasattr(o, "qsize") \
#             or  hasattr(o, "get_nowait") \
#             or  hasattr(o, "put_nowait")



# class InMainProcess():

#     def __get__(self, o, ot) -> bool:
#         return  not multiprocessing.parent_process()\
#                 or  multiprocessing.current_process().name == "MainProcess"\

# class InMainThread():

#     def __get__(self, o, ot) -> bool:
#         return  threading.main_thread() == threading.current_thread()



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



# class InThread():

#     def __get__(self, o, ot) -> bool:
#         if      not issubclass(ot, ThreadExecutor)\
#             and not issubclass(ot, MainThreadExecutor)\
#         :
#             raise   TypeError(
#                         f"wrong object({o}) type({type(o)}), "
#                         "must be subclass of ThreadExecutor or MainThreadExecutor."
#                     )
#         return      o.executor is not None\
#                 and o.executor == threading.current_thread()



# class InProcess():

#     def __get__(self, o, ot) -> bool:
#         if not issubclass(ot, ProcessExecutor):
#             raise   TypeError(
#                         f"wrong object({o}) type({type(o)}), "
#                         "must be subclass of ProcessExecutor."
#                     )
#         return      o.executor is not None\
#                 and o.executor == multiprocessing.current_process()



# class InMain():

#     def __get__(self, o, ot) -> bool:
#         if not issubclass(ot, ProcessesExecutor):
#             raise   TypeError(
#                         f"wrong object({o}) type({type(o)}), "
#                         "must be subclass of ProcessExecutor."
#                     )
#         return  (not multiprocessing.parent_process()\
#                 or  multiprocessing.current_process().name == "MainProcess")\
#                 and threading.main_thread() == threading.current_thread()

class InChilds(ABC):
    @abstractmethod
    def is_in(self, o) -> bool:    ...

    def __get__(self, o, ot) -> bool:
        return self.is_in(o)

class InChildThreads(InChilds):
    def is_in(self, o) -> bool:
        return threading.current_thread() in o.workers

class InChildProcesses(InChilds):
    def is_in(self, o) -> bool:
        return multiprocessing.current_process() in o.workers

class InThreadPool(InChilds):
    def is_in(self, o) -> bool:
        return threading.current_thread() in o.executor._threads

class InProcessPool(InChilds):
    def is_in(self, o) -> bool:
        return multiprocessing.current_process() in o.executor._processes

# class ExecutorCounterInBounds():

#     def __get__(self, o, ot) -> bool:
#         if not issubclass(ot, Workers):
#             raise   TypeError(
#                         f"wrong object({o}) type({type(o)}), "
#                         "must be subclass of Workers."
#                     )
#         return  o.iworkers.value < o.max_workers or  o.iworkers.value == 0



# class ExecutorCreationAllowed():

#     def __get__(self, o, ot) -> bool:
#         if not issubclass(ot, Workers):
#             raise   TypeError(
#                         f"wrong object({o}) type({type(o)}), "
#                         "must be subclass of Workers."
#                     )
#         return      o.iworkers.value < o.max_workers\
#                 and (
#                         not o.tasks.empty()
#                         or  o.tasks.qsize()
#                     )\
#                 # and o.iworkers.value <= len(multiprocessing.active_children())



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
