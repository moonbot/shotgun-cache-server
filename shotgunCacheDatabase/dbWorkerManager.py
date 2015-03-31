
import multiprocessing
import dbWorker

class DBWorkerManager(object):
    def __init__(self, queue, workerConfig, workerCount=None):
        super(DBWorkerManager, self).__init__()
        self.queue = multiprocessing.Queue()
        self.workerConfig = workerConfig
        if workerCount is None:
            workerCount = multiprocessing.cpu_count()
        self.workerCount = workerCount
        self.workers = []

    def postWork(self, work):
        self.queue.put(work)

    def start(self):
        for i in range(self.workerCount):
            self.addWorker()

    def addWorker(self):
        worker = dbWorker.DBWorker(self.queue, self.workerConfig)
        worker.start()
        self.workers.append(worker)
