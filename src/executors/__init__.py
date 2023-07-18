# from __future__ import annotations

# import concurrent.futures
# import multiprocessing
# import multiprocessing.queues
# import os
# # import logging
# import queue
# import sys
# import threading
# # import time

# from abc import ABC, abstractmethod
# from concurrent.futures import Future
# from typing import Callable


# import executors.descriptors as descriptors
# from executors.descriptors import *

# import executors.logger as Logging
from executors.logger   import logger

# from executors.config       import config, CONFIG
from executors.executor     import Executor
from executors.mainthread   import MainThreadExecutor
from executors.thread       import ThreadExecutor
from executors.process      import ProcessExecutor
from executors.threadpool   import ThreadPoolExecutor
from executors.processpool  import ProcessPoolExecutor
from executors.threads      import ThreadsExecutor
from executors.processes    import ProcessesExecutor

# from registrator import registrator

# class EXECUTORS(registrator.REGISTRATOR): ...

# EXECUTORS.register("Executor", "executors.mainthread", globals(), Executor)

# registry = EXECUTORS()
# logger.debug(f"Executors registered:{EXECUTORS()}.")

# if __name__ == "__main__":
#     pass