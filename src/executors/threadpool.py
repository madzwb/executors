import concurrent.futures
import threading

from executors import descriptors
from executors.pool import PoolExecutor


class InThreadPool(descriptors.InChilds):
    def is_in(self, o, ot) -> bool:
        if not issubclass(ot, ThreadPoolExecutor):
            raise   TypeError(
                        f"wrong object({o}) type({type(o)}), "
                        "must be subclass of ThreadPoolExecutor."
                    )
        return threading.current_thread() in o.executor._threads

class ThreadPoolExecutor(PoolExecutor):

    in_parent   = descriptors.InParentProcess()
    in_executor = InThreadPool()
    # in_parent_thread = InParentThread()
    # in_parent_process = InParentProcess()

    @classmethod
    def init(cls, /, *args, **kwargs) -> bool:
        cls.creator = concurrent.futures.ThreadPoolExecutor
        return True

    def __init__(self):
        super(ThreadPoolExecutor, self).__init__()

    # def __bool__(self) -> bool:
    #     return True
