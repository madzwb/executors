import multiprocessing
import multiprocessing.queues
import sys
import time

from executors.config import CONFIG
from executors.executor import Executor
from executors.logger import logger



class Worker(Executor):
    
    TRIES = 0

    executor_creation   = True
    executor_counter    = True

    def __init__(self):
        super(Worker, self).__init__()

    @staticmethod
    def worker(executor: Executor, conf = None, /, *args, **kwargs):#, tasks, results, create):
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
            # logger_init()
            logger.debug(f"{Executor.debug_info(caller)} logging prepeared.")

        logger.debug(f"{Executor.debug_info(caller)} started.")
        executor.started = True
        
        while True:
            task = None
            logger.debug(
                f"{Worker.debug_info(caller)}. "
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
                        if tries < Worker.TRIES:
                            tries += 1
                            time.sleep(tries)
                            continue
                        # On empty put back sentinel
                        executor.tasks.put_nowait(None)
                        break
            
            if task is not None:
                # TODO: Move out.
                if executor.create is not None and executor.executor_creation:
                    logger.debug(f"{Worker.debug_info(caller)} creation requested. ")
                    executor.create.set()
                # Call task
                try:
                    # Set executor for subtasks submitting
                    task.executor = executor
                    logger.info(
                        f"{Worker.debug_info(caller)}. "
                        f"{task} processing."
                    )
                    # start = 0
                    # if sys.getprofile() is not None:
                    #     start = time.time()
                    result = task.__call__()#None, tasks, results, create)
                    info = f"{Worker.debug_info(caller)}. {task} done"
                    # if sys.getprofile() is not None:
                    #     end = time.time()
                    #     delta = end - start
                    #     info += f"with str(time)s"
                    info += "."
                    logger.info(info)
                except Exception as e:
                    result = str(e)
                    logger.error(f"{Worker.debug_info(caller)}. {result}.")
                
                if  isinstance(
                        executor.tasks,
                        multiprocessing.queues.JoinableQueue
                    ):
                    executor.tasks.task_done()
                # Process results
                if result and result != executor.results:# and isinstance(result, str):
                    executor.process_results(result)
            else:
                logger.debug(
                    f"{Executor.debug_info(caller)} "
                    f"got sentinel. Exiting."
                )
                break
        # Worker done
        logger.debug(f"{Worker.debug_info(caller)} done.")
        logger.debug(
            f"{Worker.debug_info(caller)}. "
            "<Status "
                f"tasks={executor.tasks.qsize()} "
                f"results={executor.results.qsize()}"
            ">."
        )
        if executor.create is not None:
            logger.debug(f"{Worker.debug_info(caller)} exiting signaled. ")
            executor.create.set()
        executor.shutdown()
        return
