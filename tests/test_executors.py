from common import *

config.config.SCRIPT = __file__
if "logger.config" in sys.modules:
    Logging.init()
    Logging.truncate()
else:
    stream_logger = logging.StreamHandler()
    stream_logger.setFormatter(Logging.formatter)
    logger.addHandler(stream_logger)



class MainThread(CommonTestCase):

    def test(self):
        name = "mainthread"
        logger.info(f"Testing '{name}' start.")
        executor = registry[name]()
        submit_tasks(executor, TIMEOUT)
        results = get_results(executor.results)

        self.assertEqual(len(results), TASKS)
        self.assertEqual(sorted(self.results), sorted(results))
        logger.info(f"Testing '{name}' end.")



class Thread(CommonTestCase):

    def test(self):
        name = "thread"
        logger.info(f"Testing '{name}' start.")
        executor = registry[name]()
        monitoring = create_monitoring(executor, TIMEOUT)
        monitoring.join()
        # actives = active_threads(monitoring)
        results = get_results(executor.results)

        # self.assertEqual(executor.iworkers.value, 1)
        self.assertEqual(len(results), TASKS)
        self.assertEqual(sorted(self.results), sorted(results))
        logger.info(f"Testing '{name}' end.")



class Process(CommonTestCase):

    def test(self):
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



class Threads(CommonTestCase):

    def test(self):
        name = "threads"
        logger.info(f"Testing '{name}' start.")
        executor = registry[name]()
        monitoring = create_monitoring(executor, TIMEOUT)
        monitoring.join()
        # actives = active_threads(monitoring)
        results = get_results(executor.results)

        # self.assertEqual(actives, os.cpu_count())
        # self.assertEqual(executor.iworkers.value, os.cpu_count())
        self.assertEqual(len(results), TASKS)
        self.assertEqual(sorted(self.results), sorted(results))
        logger.info(f"Testing '{name}' end.")



class Processes(CommonTestCase):

    def test(self):
        name = "processes"
        logger.info(f"Testing '{name}' start.")
        executor = registry[name]()
        monitoring = create_monitoring(executor, TIMEOUT)
        monitoring.join()
        # actives = active_processes(monitoring)
        results = get_results(executor.results)

        # self.assertEqual(actives, os.cpu_count())
        # self.assertEqual(executor.iworkers.value, os.cpu_count())
        self.assertEqual(len(results), TASKS)
        self.assertEqual(sorted(self.results), sorted(results))
        logger.info(f"Testing '{name}' end.")
        return



class ThreadPool(CommonTestCase):

    def test(self):
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



class ProcessPool(CommonTestCase):

    def test(self):
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
