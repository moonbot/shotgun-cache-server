import time
import logging
import multiprocessing
import Queue

"""
Types of validation

Counts
    - Quick validation that just ensures the correct # of each entity type is stored in the cache
    -



"""

__all__ = [
    'CountValidator'
]

LOG = logging.getLogger(__name__)


class CountValidator(object):
    def __init__(self, shotgunConnector, elastic, entityConfigs, processCount=8):
        super(CountValidator, self).__init__()
        self.shotgunConnector = shotgunConnector
        self.elastic = elastic
        self.entityConfigs = entityConfigs
        self.processCount = min(len(entityConfigs), processCount)

        self.sg = None
        self.queue = multiprocessing.JoinableQueue()
        self.processes = []

    def start(self):
        self.sg = self.shotgunConnector.getInstance()
        self.launchWorkers()
        self.run()
        self.terminateWorkers()

    def launchWorkers(self):
        for n in range(self.processCount):
            worker = ValidateCountWorker(self.queue, self.shotgunConnector, self.elastic, self.entityConfigs)
            proc = multiprocessing.Process(target=worker.run)
            proc.start()
            self.processes.append(proc)

    def terminateWorkers(self):
        for proc in self.processes:
            proc.terminate()

    def run(self):
        for config in self.entityConfigs:
            data = {
                'filters': self.filters,
                'configType': config.type,
            }
            self.queue.put(data)
        self.queue.join()


class ValidateCountWorker(object):
    def __init__(self, queue, shotgunConnector, elasticConnector, elasticEntityIndexTemplate, entityConfigs):
        super(ValidateCountWorker, self).__init__()
        self.queue = queue
        self.shotgunConnector = shotgunConnector
        self.elasticConnector = elasticConnector
        self.elasticEntityIndexTemplate = elasticEntityIndexTemplate
        self.entityConfigs = dict([(c.type, c) for c in entityConfigs])

    def start(self):
        self.sg = self.shotgunConnector.getInstance()
        self.elastic = self.elasticConnector.getInstance()
        self.run()

    def run(self):
        while True:
            try:
                work = self.queue.get()
            except Queue.Emtpy:
                continue
                time.sleep(0.1)

            LOG.debug("work: {0}".format(work)) # TESTING
            entityConfig = self.entityConfigs[work['configType']]

            # Make sure we are filtering based how the cached entity
            # is setup, then add the additional filters
            filters = entityConfig.get('filters', [])
            filters.extend(work.get('filters', []))

            sgResult = self.sg.summarize(entityConfig.type, summary_fields=[{'id', 'count'}])
            print("sgResult: {0}".format(sgResult)) # TESTING

            # TODO (bchapman) Need more standardized way to get this
            elasticIndex = self.elasticEntityIndexTemplate.format(type=configType.type).lower()
            elasticResult = self.elastic.search(index=elasticIndex, type=entityConfig.type.lower())
            print("elasticResult: {0}".format(elasticResult)) # TESTING

            self.queue.task_done()
