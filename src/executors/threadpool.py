import threading
import concurrent.futures

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

    in_parent   = descriptors.InParentThread()
    in_executor = InThreadPool()
    # in_parent_thread = InParentThread()
    # in_parent_process = InParentProcess()

    @classmethod
    def init(cls, /, *args, **kwargs) -> bool:
        cls.creator = concurrent.futures.ThreadPoolExecutor
        return True

    def __init__(self):
        super(ThreadPoolExecutor, self).__init__()

    def join(self, timeout= None) -> bool:
        if not self.in_parent:
            raise RuntimeError("can't join from another process.")
        if self.in_executor:
            raise RuntimeError("can't join from child.")
        if not self.started:
            self.start()
        result = concurrent.futures.wait(self.futures, timeout, return_when="ALL_COMPLETED")
        return True
    
    # def __bool__(self) -> bool:
    #     return True
