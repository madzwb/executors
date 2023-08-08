from common import *

print(__name__)
if "logger.config" in sys.modules:
    if      __name__ == "__main__"          \
        or  __name__ == "test_executors"    \
        :
        config.config.SCRIPT = __file__
        config.config.LOG_FILE_TRUNCATE = True
        Logging.truncate()
        Logging.init()
else:
    stream_logger = logging.StreamHandler()
    stream_logger.setFormatter(Logging.formatter)
    logger.addHandler(stream_logger)



class MainThread(CommonTestCase):
    # name = "mainthread"

    def test(self):
        self._test()


class Thread(CommonTestCase):

    def test(self):
        self._test()
    # def test(self):
    #     name = "thread"
    #     logger.info(f"Testing '{name}' start.")
    #     executor = registry[name]()
    #     monitoring = create_monitoring(executor, TIMEOUT)
    #     monitoring.join()
    #     # actives = active_threads(monitoring)
    #     results = get_results(executor.results)

    #     # self.assertEqual(executor.iworkers.value, 1)
    #     self.assertEqual(len(results), TASKS)
    #     self.assertEqual(sorted(self.results), sorted(results))
    #     logger.info(f"Testing '{name}' end.")



class Process(CommonTestCase):
    pass
    # def test(self):
    #     name = "process"
    #     logger.info(f"Testing '{name}' start.")
    #     executor = registry[name]()
    #     monitoring = create_monitoring(executor, TIMEOUT)
    #     monitoring.join()

    #     # actives = active_processes(monitoring)
    #     results = get_results(executor.results)

    #     # self.assertEqual(actives, 1)
    #     self.assertEqual(len(results), TASKS)
    #     self.assertEqual(sorted(self.results), sorted(results))
    #     logger.info(f"Testing '{name}' end.")



class Threads(CommonTestCase):

    def test(self):
        self._test()
    # def test(self):
    #     name = "threads"
    #     logger.info(f"Testing '{name}' start.")
    #     executor = registry[name]()
    #     monitoring = create_monitoring(executor, TIMEOUT)
    #     monitoring.join()
    #     # actives = active_threads(monitoring)
    #     results = get_results(executor.results)

    #     # self.assertEqual(actives, os.cpu_count())
    #     # self.assertEqual(executor.iworkers.value, os.cpu_count())
    #     self.assertEqual(len(results), TASKS)
    #     self.assertEqual(sorted(self.results), sorted(results))
    #     logger.info(f"Testing '{name}' end.")



class Processes(CommonTestCase):

    def test(self):
        self._test()
    # def test(self):
    #     name = "processes"
    #     logger.info(f"Testing '{name}' start.")
    #     executor = registry[name]()
    #     monitoring = create_monitoring(executor, TIMEOUT)
    #     monitoring.join()
    #     # actives = active_processes(monitoring)
    #     results = get_results(executor.results)

    #     # self.assertEqual(actives, os.cpu_count())
    #     # self.assertEqual(executor.iworkers.value, os.cpu_count())
    #     self.assertEqual(len(results), TASKS)
    #     self.assertEqual(sorted(self.results), sorted(results))
    #     logger.info(f"Testing '{name}' end.")
    #     return



class ThreadPool(CommonTestCase):

    def test(self):
        self._test()
    # def test(self):
    #     name = "threadpool"
    #     logger.info(f"Testing '{name}' start.")
    #     executor = registry[name]()
    #     monitoring = create_monitoring(executor, TIMEOUT)
    #     monitoring.join()
    #     # actives = active_threads(monitoring)
    #     results = get_results(executor.results)

    #     # self.assertEqual(actives, os.cpu_count())
    #     self.assertEqual(len(results), TASKS)
    #     self.assertEqual(sorted(self.results), sorted(results))
    #     logger.info(f"Testing '{name}' end.")



class ProcessPool(CommonTestCase):

    def test(self):
        self._test()
    # def test(self):
    #     name = "processpool"
    #     logger.info(f"Testing '{name}' start.")
    #     executor = registry[name]()
    #     monitoring = create_monitoring(executor, TIMEOUT)
    #     monitoring.join()
    #     # actives = active_processes(monitoring)
    #     results = get_results(executor.results)

    #     # self.assertEqual(actives, os.cpu_count())
    #     self.assertEqual(len(results), TASKS)
    #     self.assertEqual(sorted(self.results), sorted(results))
    #     logger.info(f"Testing '{name}' end.")



if __name__ == "__main__":
    unittest.main()
