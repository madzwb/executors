import multiprocessing
import multiprocessing.queues
import sys
import time
import threading

from executors import Logging
from executors import descriptors

from executors.config   import CONFIG
from executors.executor import Executor, TASK_SENTINEL, RESULT_SENTINEL
from executors.logger   import logger



class InThread(descriptors.InChilds):
    
    def is_in(self, o, ot):
        return threading.current_thread().ident == o.executor.ident

    def __get__(self, o, ot):
        if not issubclass(ot, Worker):
            raise   TypeError(
                        f"wrong object({o}) type({type(o)}), "
                        "must be subclass of ThreadExecutor."
                    )
        return self.is_in(o, ot)



class InProcess(descriptors.InChilds):
    
    def is_in(self, o, ot):
        return multiprocessing.current_process().ident == o.executor.ident

    def __get__(self, o, ot):
        if not issubclass(ot, Worker):
            raise   TypeError(
                        f"wrong object({o}) type({type(o)}), "
                        "must be subclass of ThreadExecutor."
                    )
        return self.is_in(o, ot)



# class ExecutorCreationAllowed():

#     def __get__(self, o, ot) -> bool:
#         if not issubclass(ot, Worker):
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


class Worker(Executor):
    
    MAX_TRIES = 1
    MAX_UNITS = 1

    # executor_creation   = ExecutorCreationAllowed
    # executor_counter    = True

    def __init__(
            self,
            parent_pid = multiprocessing.current_process().ident,
            parent_tid = threading.current_thread().ident
        ):
        super(Worker, self).__init__(parent_pid, parent_tid)

    def join(self, timeout = None) -> bool:
        #super(Worker, self).join(timeout)
        if      not self.in_executor                \
            and self.executor is not None           \
        :
            tries = 0
            while tries < Worker.MAX_TRIES:
                info = ""
                info += f"{Logging.info(self.__class__.__name__)}. "
                info += f"Going to wait for {self.executor}" 
                info += f" for {timeout}sec" if timeout else "."
                logger.debug(info)

                # if timeout is None:
                #     self.joined.value = 1

                self.executor.join(timeout)
                if self.executor.is_alive():
                    logger.debug(
                        f"{Logging.info(self.__class__.__name__)}. "
                        f"{self.executor} not complete in {timeout}sec."
                    )
                    if timeout is not None:
                        tries += 1
                    else:
                        return False
                else:
                    self.joined.value = 1
                    return True
            else:
                if timeout is not None and tries >= 3:
                    logger.debug(
                        f"{Logging.info(self.__class__.__name__)}. "
                        f"{self.executor} not complete with "
                        f"{timeout*tries}sec. "
                        f"Try count reached {tries}. Exiting."
                    )
        return False
    
    @staticmethod
    def worker(executor: Executor, conf = None, /, *args, **kwargs):
        caller = executor.__class__.__name__
        if      executor.tasks      is None \
            or  executor.results    is None \
        :
            raise   RuntimeError(\
                        f"not setuped tasks' queue."
                            if executor.tasks is None
                            else
                        ""
                        f"not setuped results' queue."
                            if executor.tasks is None
                            else
                        ""
                    )
        # Update 'config' module with conf
        if CONFIG in sys.modules and conf is not None:
            sys.modules[CONFIG].__call__(conf)
            Logging.init() # Reinit logger
            logger.debug(f"{Logging.info(caller)} logging prepeared.")

        logger.debug(f"{Logging.info(caller)} started.")
        executor.started = True
        if conf is not None and hasattr(conf, "DEBUG") and conf.DEBUG:
            processed = 0
        while True:
            task = None
            logger.debug(
                f"{Logging.info(caller)}. "
                "<Status "
                    f"workers={executor.iworkers.value} "
                    f"tasks={executor.tasks.qsize()} "
                    f"results={executor.results.qsize()}"
                ">."
            )
            task = executor.get_task()

            # On empty put back sentinel for other workers
            if task is None and executor.iworkers.value > 0:
                executor.tasks.put_nowait(TASK_SENTINEL)
                logger.info(
                    f"{Logging.info(caller)}. "
                    f"{task} - sentinel putted back."
                )
            elif task is None:
                pass
            
            if task is not None:
                # # TODO: Move out.
                # if executor.create is not None and executor.executor_creation:
                #     logger.debug(f"{Logging.info(caller)} creation requested. ")
                #     executor.create.set()
                # Call task
                try:
                    # Set executor for subtasks submitting
                    task.executor = executor
                    logger.info(
                        f"{Logging.info(caller)}. "
                        f"{task} processing."
                    )
                    # start = 0
                    # if sys.getprofile() is not None:
                    #     start = time.time()
                    result = task.__call__()#None, tasks, results, create)
                    info = f"{Logging.info(caller)}. {task} done"
                    # if sys.getprofile() is not None:
                    #     end = time.time()
                    #     delta = end - start
                    #     info += f"with str(time)s"
                    info += "."
                    logger.info(info)
                    if conf is not None and hasattr(conf, "DEBUG") and conf.DEBUG:
                        processed += 1
                except Exception as e:
                    result = str(e)
                    logger.error(f"{Logging.info(caller)}. {result}.")
                
                if  isinstance(
                        executor.tasks,
                        multiprocessing.queues.JoinableQueue
                    ):
                    executor.tasks.task_done()
                # Process results
                # if result and result != executor.results:
                Executor.process_results(executor.results, result)
            else:
                logger.debug(
                    f"{Logging.info(caller)} "
                    f"got sentinel. Exiting."
                )
                break
        # Worker done
        logger.debug(f"{Logging.info(caller)} done.")
        logger.debug(
            f"{Logging.info(caller)}. "
            "<Status "
                f"workers={executor.iworkers.value} "
                f"tasks={executor.tasks.qsize()} "
                f"results={executor.results.qsize()}"
            ">."
        )
        executor.is_shutdown.value = 1
        if executor.create is not None:
            logger.debug(f"{Logging.info(caller)} exiting signaled. ")
            executor.create.set()

        executor.shutdown(False)
        executor.iworkers.value -= 1
        # executor.results.put_nowait(RESULT_SENTINEL)
        return

    # def shutdown(self, wait = True, * , cancel = False) -> bool:
    #     if super().shutdown(wait, cancel=cancel):
    #         if self.executor is not None:
    #             self.executor.close()
    #             return True
    #     return False
