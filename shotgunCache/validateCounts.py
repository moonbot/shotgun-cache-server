import os
import time
import logging
import datetime
import multiprocessing
import Queue

import rethinkdb

__all__ = [
    'CountValidator'
]

LOG = logging.getLogger(__name__)


class CountValidator(object):
    def __init__(self, config, entityConfigs):
        super(CountValidator, self).__init__()
        self.config = config
        self.entityConfigs = entityConfigs

        self.workQueue = multiprocessing.JoinableQueue()
        self.resultQueue = multiprocessing.Queue()
        self.processes = []
        self.results = []

    def start(self, raiseExc=True):
        LOG.info("Starting Validate Counts")
        self.launchWorkers()
        self.run()
        self.terminateWorkers()

        if raiseExc:
            failed = []
            for result in self.results:
                if result['failed']:
                    failed.append(result)

            if len(failed):
                raise RuntimeError("Validation Failed, {0} cached entity type(s) do not match".format(len(failed)))

        return self.results

    def launchWorkers(self):
        processCount = min(len(self.entityConfigs), self.config['validate_counts.processes'])
        LOG.debug("Launching {0} validate workers".format(processCount))
        for n in range(processCount):
            worker = CountValidateWorker(self.workQueue, self.resultQueue, self.config, self.entityConfigs)
            proc = multiprocessing.Process(target=worker.start)
            proc.start()
            self.processes.append(proc)

    def run(self):
        LOG.debug("Adding items to validate queue")
        for config in self.entityConfigs:
            data = {'configType': config.type}
            self.workQueue.put(data)
        self.workQueue.join()

        results = []
        while True:
            try:
                result = self.resultQueue.get(False)
            except Queue.Empty:
                break
            else:
                if result:
                    results.append(result)
        self.results = results

    def terminateWorkers(self):
        LOG.debug("Terminating validate workers")
        for proc in self.processes:
            proc.terminate()
        self.processes = []


class CountValidateWorker(object):
    def __init__(self, workQueue, resultQueue, config, entityConfigs):
        super(CountValidateWorker, self).__init__()
        self.workQueue = workQueue
        self.resultQueue = resultQueue
        self.config = config
        self.entityConfigs = dict([(c.type, c) for c in entityConfigs])

        self.sg = None
        self.rethink = None

    def start(self):
        self.sg = self.config.createShotgunConnection(convert_datetimes_to_utc=False)
        self.rethink = self.config.createRethinkConnection()
        self.run()

    def run(self):
        workerPID = os.getpid()
        LOG.debug("Validate Worker Running: {0}".format(workerPID))
        while True:
            try:
                work = self.workQueue.get()
            except Queue.Emtpy:
                continue
                time.sleep(0.1)

            entityConfig = self.entityConfigs[work['configType']]

            LOG.debug("Getting Shotgun counts for type: '{0}'".format(work['configType']))
            sgResult = self.sg.summarize(entityConfig.type, [], summary_fields=[{'field': 'id', 'type': 'count'}])

            sgCount = sgResult['summaries']['id']

            cacheCount = 0
            try:
                LOG.debug("Getting cache counts for type: '{0}'".format(work['configType']))
                cacheSearchTime = datetime.datetime.utcnow()
                cacheCount = rethinkdb.table(entityConfig['table']).count().run(self.rethink)
            except rethinkdb.errors.RqlRuntimeError:
                cacheCount = 0

            # Find the diff of events that have happened in Shotgun, but not been saved to the cache yet
            # Searches all event log entries for this entity type that are New, Retired, or Revive occurring in the past fetch_interval
            # including a small amount of processing padding for the cache
            self.config.history.load()
            latestCachedEventID = self.config.history['latest_event_log_entry']['id']
            minTime = cacheSearchTime - datetime.timedelta(seconds=self.config['monitor.fetch_interval'] + 0.05)
            maxTime = cacheSearchTime
            eventTypes = ['Shotgun_{entityType}_{changeType}'.format(entityType=entityConfig.type, changeType=t) for t in ['New', 'Retirement', 'Revival']]
            eventLogFilters = [
                ['event_type', 'in', eventTypes],
                ['created_at', 'between', [minTime, maxTime]],
                ['id', 'greater_than', latestCachedEventID]
            ]

            LOG.debug("Getting Pending Event Log Entries for type: '{0}'".format(work['configType']))
            eventLogEntries = self.sg.find('EventLogEntry', eventLogFilters, ['event_type', 'id'])

            additions = len([e for e in eventLogEntries if 'New' in e['event_type'] or 'Revival' in e['event_type']])
            removals = len([e for e in eventLogEntries if 'Retirement' in e['event_type']])
            pendingDiff = additions - removals

            failed = sgCount - pendingDiff != cacheCount

            if failed:
                LOG.debug("'{0}' counts don't match, SG: {1} Cache: {2}".format(entityConfig.type, sgCount, cacheCount))
            else:
                LOG.debug("'{0}' counts match, SG: {1} Cache: {2}".format(entityConfig.type, sgCount, cacheCount))

            result = {
                'work': work,
                'entityType': work['configType'],
                'failed': failed,
                'sgCount': sgCount,
                'pendingEvents': len(eventLogEntries),
                'pendingDiff': pendingDiff,
                'cacheCount': cacheCount,
            }
            self.resultQueue.put(result)

            self.workQueue.task_done()
