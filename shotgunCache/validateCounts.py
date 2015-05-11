import logging
import datetime
import gevent

import rethinkdb as r
import utils

__all__ = [
    'CountValidator'
]

LOG = logging.getLogger(__name__)


class CountValidator(object):
    def __init__(self, config, entityConfigs):
        super(CountValidator, self).__init__()
        self.config = config
        self.entityConfigs = entityConfigs

        self.shotgunPool = utils.ShotgunConnectionPool(config, config['import']['max_shotgun_connections'])
        self.rethinkPool = utils.RethinkConnectionPool(config, config['import']['max_rethink_connections'])

        self.processes = []
        self.results = []

    def start(self, raiseExc=True):
        return self.run()

    def run(self):
        LOG.debug("Adding items to validate queue")
        greenlets = [gevent.spawn(self.get_count_for_config, c) for c in self.entityConfigs]
        gevent.joinall(greenlets)
        result = [g.value for g in greenlets]
        return result

    def get_count_for_config(self, entityConfig):
        LOG.debug("Getting Shotgun counts for type: '{0}'".format(entityConfig.type))
        with self.shotgunPool.get() as sg:
            sgResult = sg.find(entityConfig.type, [], ['id'])
        sgCount = len(sgResult)

        cacheCount = 0
        LOG.debug("Getting cache counts for type: '{0}'".format(entityConfig.type))
        cacheSearchTime = datetime.datetime.utcnow()
        with self.rethinkPool.get() as conn:
            try:
                cacheCount = r.table(entityConfig['table']).count().run(conn)
            except r.errors.RqlRuntimeError:
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

        LOG.debug("Getting Pending Event Log Entries for type: '{0}'".format(entityConfig.type))
        with self.shotgunPool.get() as sg:
            eventLogEntries = sg.find('EventLogEntry', eventLogFilters, ['event_type', 'id'])

        additions = len([e for e in eventLogEntries if 'New' in e['event_type'] or 'Revival' in e['event_type']])
        removals = len([e for e in eventLogEntries if 'Retirement' in e['event_type']])
        pendingDiff = additions - removals

        failed = sgCount - pendingDiff != cacheCount

        if failed:
            LOG.debug("'{0}' counts don't match, SG: {1} Cache: {2}".format(entityConfig.type, sgCount, cacheCount))
        else:
            LOG.debug("'{0}' counts match, SG: {1} Cache: {2}".format(entityConfig.type, sgCount, cacheCount))

        result = {
            'entityType': entityConfig.type,
            'failed': failed,
            'sgCount': sgCount,
            'pendingEvents': len(eventLogEntries),
            'pendingDiff': pendingDiff,
            'cacheCount': cacheCount,
        }
        return result
