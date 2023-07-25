import multiprocessing
import multiprocessing.queues
import sys
import time
import threading

from executors import Logging
from executors import descriptors

from executors.config   import CONFIG
from executors.executor import Executor
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
    
    MAX_TRIES = 0
    MAX_UNITS = 1

    # executor_creation   = ExecutorCreationAllowed
    # executor_counter    = True

    def __init__(
            self,
            parent_pid = multiprocessing.current_process().ident,
            parent_tid = threading.current_thread().ident
        ):
        super(Worker, self).__init__(parent_pid, parent_tid)

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
        # executor.started = True
        if conf is not None and hasattr(conf, "DEBUG") and conf.DEBUG:
            processed = 0
        while True:
            task = None
            logger.debug(
                f"{Logging.info(caller)}. "
                "<Status "
                    f"tasks={executor.tasks.qsize()} "
                    f"results={executor.results.qsize()}"
                ">."
            )
            task = executor.tasks.get()  # Get task or wait for new one

            # Filter out all sentinel
            # and push back one on empty tasks' queue
            if task is None:
                if  isinstance(
                        executor.tasks,
                        multiprocessing.queues.JoinableQueue
                    ):
                    executor.tasks.task_done()
                # Get and mark all sentinel tasks as done
                tries = 0
                while task is None:
                    try:
                        task = executor.tasks.get_nowait()
                        if task is None:
                            if  isinstance(
                                    executor.tasks,
                                    multiprocessing.queues.JoinableQueue
                                ):
                                executor.tasks.task_done()
                        else:
                            break
                    except Exception as e: # Fucking shit
                        task = None
                        if tries < Worker.MAX_TRIES:
                            tries += 1
                            time.sleep(tries)
                            continue
                        # On empty put back sentinel for other processes
                        if executor.iworkers.value > 1:
                            executor.tasks.put_nowait(None)
                        break
            
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
                if result and result != executor.results:
                    executor.process_results(result)
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
                f"tasks={executor.tasks.qsize()} "
                f"results={executor.results.qsize()}"
            ">."
        )
        # executor.iworkers.value -= 1
        # executor.shutdown()
        executor.is_shutdown.value = 1
        executor.iworkers.value -= 1
        if executor.create is not None:
            logger.debug(f"{Logging.info(caller)} exiting signaled. ")
            executor.create.set()
        return
