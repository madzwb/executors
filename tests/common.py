from __future__ import annotations

import logging
import multiprocessing
# import os
import cProfile
import sys
import time
import threading
import unittest

# unittest.TestLoader.sortTestMethodsUsing = None

from abc import ABC, abstractmethod

from logger     import *
from executors  import *

import registrator.registrator as registrator

class EXECUTERS(registrator.REGISTRATOR):
    pass
EXECUTERS.register("Executor", "executors", vars(sys.modules["executors"]), IExecutor)
registry = EXECUTERS()

PROFILING   = False
TIMEOUT     = 0.3
TASKS       = 33



class IAction(ABC):
    
    @abstractmethod
    def __call__(self, *args, **kwargs): ...

class ITask(IAction):
    
    def __init__(self, executor = None) -> None:
        super().__init__()
        self.executor = executor

class Task(ITask):
    
    def __init__(self,i: int,  executor = None, timeout = None) -> None:
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



class CommonTestCase(unittest.TestCase):
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
