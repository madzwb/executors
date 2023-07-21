import logging
import multiprocessing
import os
import sys
import threading

logger = None

# if "logger.logger" in sys.modules:
try:
    from logger.logger import *
except ImportError:
    pass

if logger is None:

    from executors.config import config

    formatter = logging.Formatter("%(asctime)s [%(levelname)-8s] - %(message)s")
    formatter_result = logging.Formatter("%(message)s")
    formatter.default_msec_format = '%s.%03d'

    logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
    logger.addHandler(logging.NullHandler())
    level = logging.DEBUG if config.DEBUG else logging.INFO
    logger.setLevel(level)

def _repr_process(process = multiprocessing.current_process()) -> str:
    return "<Process "\
                f"name='{process.name}' "\
                f"pid={process.ident} "\
                f"parent={process._parent_pid}"\
            ">"

def _repr_thread(thread  = threading.current_thread()) -> str:
    return "<Thread "\
                f"name='{thread.name}' "\
                f"pid={thread.ident}"\
            ">"

def info(name = "") -> str:
    process = multiprocessing   .current_process()
    thread  = threading         .current_thread()
    process = _repr_process(process)
    thread  = _repr_thread(thread)
    return f"<{name} process={process} thread={thread}>"\
                if config.DEBUG\
                else\
            f"{name}"
