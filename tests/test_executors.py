from __future__ import annotations

import logging
import multiprocessing
import os
import cProfile
import sys
import time
import threading
import unittest

from abc import ABC, abstractmethod

# current = os.path.dirname(os.path.realpath(__file__))
# parent = os.path.dirname(current)
# sys.path.append(parent)

# import config
# config.SCRIPT = __file__
# config.TEST = False
# config.DEBUG = False
# config.LOG_TO_TERMINAL = False

# # Truncate log file.
# if __name__ == "__main__" and config.LOG_TO_FILE and config.TEST:
#     path = os.path.dirname(config.SCRIPT)
#     filename =  os.path.splitext(os.path.basename(config.SCRIPT))[0] + ".log"
#     full = (path + "/" + filename)
#     with open(full,"w") as file:
#         pass

# sys.path.append(os.path.dirname(__file__) + "/../")
# sys.path.append(os.path.dirname(__file__) + "/../src/")
# from src.executors.executors import IExecutor, Executor, logger
from executors.executors import IExecutor, Executor, logger
import registrator.registrator as registrator

class EXECUTERS(registrator.REGISTRATOR):
    pass
# EXECUTERS.register("Executor", "src.executors.executors", vars(sys.modules["src.executors.executors"]), IExecutor)
EXECUTERS.register("Executor", "executors.executors", vars(sys.modules["executors.executors"]), IExecutor)
registry = EXECUTERS()

# from logger import logger, formatter, init as logger_init
# logger_init()

# formatter = logging.Formatter("%(asctime)s [%(levelname)-8s] - %(message)s")
# formatter_result = logging.Formatter("%(message)s")
# formatter.default_msec_format = '%s.%03d'

# logger = logging.getLogger()
# stream_handler  = logging.StreamHandler()
# stream_handler.setFormatter(formatter)
# logger.addHandler(stream_handler)

# logger.setLevel(logging.DEBUG)

PROFILING = False
TIMEOUT = 0.3
TASKS   = 33


class IAction(ABC):
    
    @abstractmethod
    def __call__(self, *args, **kwargs): ...

class ITask(IAction):
    
    def __init__(self, executor: Executor|None = None) -> None:
        super().__init__()
        self.executor: Executor|None = executor

class Task(ITask):
    
    def __init__(self,i: int,  executor: Executor | None = None, timeout = None) -> None:
        super().__init__(executor)
        self.i = i
        self.timeout = timeout

    def __call__(self, *args, **kwargs):
        if self.timeout is not None:
            time.sleep(self.timeout)
        # if self.executor is not None:
        #     self.executor.submit(None)
        result = f"Task: {self.i} complete."
        # print(result, flush = True)
        return result

    def __str__(self):
        return f"Task: {self.i}"

# if __name__ == "__main__":

def submit_tasks(executor, timeout = None):
    for i in range(TASKS):
        executor.submit(Task(i, executor, timeout))
    executor.submit(None)
    while not executor.join():
        pass
    else:
        executor.shutdown()

def create_monitoring(executor, timeout = None):
    thread = threading.Thread(target=submit_tasks, args=(executor, timeout))
    thread.start()
    return thread

def get_results(results_):
    _results = []
    try:
        while result := results_.get_nowait():
            _results.append(result)
    except Exception as e:
        pass
    return _results

def active_threads(thread) -> int:
    count = 0
    while thread.is_alive():
        if (actives := threading.active_count()) > count:
            count = actives
    return count

def active_processes(process) -> int:
    count = 0
    while process.is_alive():
        if (actives := len(multiprocessing.active_children())) > count:
            count = actives
    return count

class ExecutorsTestCase(unittest.TestCase):

    def setUp(self) -> None:
        super().setUp()
        self.results = []
        for i in range(TASKS):
            result = Task(i)()
            self.results.append(result)
        if PROFILING:
            self.profile = cProfile.Profile()
            self.profile.enable()

    def tearDown(self):
        if PROFILING:
            self.profile.disable()
            self.profile.print_stats(sort="ncalls")

    def test_MainThreadExecutor(self):
        name = "mainthread"
        logger.info(f"Testing '{name}' start.")
        executor = registry[name]()
        submit_tasks(executor, TIMEOUT)
        results = get_results(executor.results)

        self.assertEqual(len(results), TASKS)
        self.assertEqual(sorted(self.results), sorted(results))
        logger.info(f"Testing '{name}' end.")

    def test_ThreadExecutor(self):
        name = "thread"
        logger.info(f"Testing '{name}' start.")
        executor = registry[name]()
        monitoring = create_monitoring(executor, TIMEOUT)
        actives = active_threads(monitoring)
        results = get_results(executor.results)

        self.assertEqual(executor.iworkers.value, 1)
        self.assertEqual(len(results), TASKS)
        self.assertEqual(sorted(self.results), sorted(results))
        logger.info(f"Testing '{name}' end.")

    def test_ProcessExecutor(self):
        name = "process"
        logger.info(f"Testing '{name}' start.")
        executor = registry[name]()
        monitoring = create_monitoring(executor, TIMEOUT)
        monitoring.join()

        # actives = active_processes(monitoring)
        results = get_results(executor.results)

        # self.assertEqual(actives, 1)
        self.assertEqual(len(results), TASKS)
        self.assertEqual(sorted(self.results), sorted(results))
        logger.info(f"Testing '{name}' end.")

    def test_ThreadsExecutor(self):
        name = "threads"
        logger.info(f"Testing '{name}' start.")
        executor = registry[name]()
        monitoring = create_monitoring(executor, TIMEOUT)
        actives = active_threads(monitoring)
        results = get_results(executor.results)

        self.assertEqual(executor.iworkers.value, 4)
        self.assertEqual(len(results), TASKS)
        self.assertEqual(sorted(self.results), sorted(results))
        logger.info(f"Testing '{name}' end.")

    def test_ProcessesExecutor(self):
        name = "processes"
        logger.info(f"Testing '{name}' start.")
        executor = registry[name]()
        monitoring = create_monitoring(executor, TIMEOUT)
        monitoring.join()
        # actives = active_processes(monitoring)
        results = get_results(executor.results)

        # self.assertEqual(actives, os.cpu_count())
        self.assertEqual(len(results), TASKS)
        self.assertEqual(sorted(self.results), sorted(results))
        logger.info(f"Testing '{name}' end.")
        return

    def test_ThreadPoolExecutor(self):
        name = "threadpool"
        logger.info(f"Testing '{name}' start.")
        executor = registry[name]()
        monitoring = create_monitoring(executor, TIMEOUT)
        monitoring.join()
        # actives = active_threads(monitoring)
        results = get_results(executor.results)

        # self.assertEqual(actives, os.cpu_count())
        self.assertEqual(len(results), TASKS)
        self.assertEqual(sorted(self.results), sorted(results))
        logger.info(f"Testing '{name}' end.")

    def test_ProcessPoolExecutor(self):
        name = "processpool"
        logger.info(f"Testing '{name}' start.")
        executor = registry[name]()
        monitoring = create_monitoring(executor, TIMEOUT)
        monitoring.join()
        # actives = active_processes(monitoring)
        results = get_results(executor.results)

        # self.assertEqual(actives, os.cpu_count())
        self.assertEqual(len(results), TASKS)
        self.assertEqual(sorted(self.results), sorted(results))
        logger.info(f"Testing '{name}' end.")

if __name__ == "__main__":
    unittest.main()
