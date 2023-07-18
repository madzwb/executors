import logging
import multiprocessing
import os
import threading

from executors.config import config

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.addHandler(logging.NullHandler())

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

@staticmethod
def info(name = "") -> str:
    process = multiprocessing   .current_process()
    thread  = threading         .current_thread()
    process = _repr_process(process)
    thread  = _repr_thread(thread)
    return f"<{name} process={process} thread={thread}>"\
                if config.DEBUG\
                else\
            f"{name}"
