import time
import datetime
import socket
import logging

import zmq
import shotgun_api3 as sg

__all__ = [
    'ShotgunEventMonitor',
]

LOG = logging.getLogger(__name__)


class ShotgunEventMonitor(object):
    """
    Stripped down version of the Shotgun Event Daemon
    Records when EventLogEntry's have changed in shotgun
    and sends the changes to the DatabaseController through zmq
    """
    eventSubTypes = ['New', 'Change', 'Retirement', 'Revival']

    def __init__(self, config, latestEventLogEntry=None):
        super(ShotgunEventMonitor, self).__init__()
        self.config = config
        self.latestEventLogEntry = latestEventLogEntry

        self.entityTypes = []

        self.sg = None
        self.context = None
        self.socket = None

        self._latestEventID = None
        self._latestEventIDPath = None
        self._loopStartTime = None

    def start(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.connect(self.config['zmqListenUrl'])
        self.loadInitialEventID()
        self.buildBaseFilters()
        self.run()

    def run(self):
        LOG.info("Monitoring Shotgun")
        totalEventsInLoop = 0
        reset = False
        self.socket.send_pyobj({'type': 'monitorStarted'})
        while True:
            if reset:
                totalEventsInLoop = 0
                timeToPost = 0
                self._loopStartTime = time.time()

            events = self.getNewEvents()

            if len(events):
                LOG.debug("{0} new EventLogEntrys".format(len(events)))

                postStartTime = time.time()
                body = {
                    'type': 'eventLogEntries',
                    'data': {
                        'entities': events,
                    },
                }

                self.socket.send_pyobj(body)
                timeToPost = time.time() - postStartTime

                self.setLatestEventLogEntry(events[-1])
                totalEventsInLoop += len(events)
                reset = False
            else:
                self.fetchDelay()
                reset = True

                # Only report status for loops with events
                if totalEventsInLoop:
                    statData = {
                        'type': 'stat',
                        'data': {
                            'type': 'shotgun_event_update',
                            'fetch_interval': self.config['monitor.fetch_interval'],
                            'total_events': totalEventsInLoop,
                            # might not need this, most of the time below 1 ms
                            'duration': round(timeToPost * 1000, 3),
                            'created_at': datetime.datetime.utcnow().isoformat(),
                        },
                    }
                    self.socket.send_pyobj(statData)

    def getNewEvents(self):
        """
        Fetch the new EventLogEntry entities from Shotgun
        Loops until successful
        """
        filters = [
            ['id', 'greater_than', self.latestEventLogEntry['id']]
        ]
        filters.extend(self.baseFilters)
        fields = [
            'id',
            'event_type',
            'attribute_name',
            'meta',
            'entity',
            'user',
            'project',
            'session_uuid',
            'created_at'
        ]
        order = [
            {'column': 'id', 'direction': 'asc'}
        ]

        conn_attempts = 0
        while True:
            try:
                _sg = self.connect()
                result = _sg.find("EventLogEntry", filters, fields, order, limit=self.config['monitor.max_event_batch_size'])
                break
            except (sg.ProtocolError, sg.ResponseError, socket.error):
                self.connect(force=True)
                LOG.warning("Unable to connect to Shotgun (attempt {0} of {1})".format(conn_attempts + 1, self.config['monitor.max_conn_retries']))
                conn_attempts += 1

            if conn_attempts >= self.config['monitor.max_conn_retries']:
                LOG.warning("Unable to connect to Shotgun after max attempts, retrying in {0} seconds".format(self.config['monitor.conn_retry_sleep']))
                time.sleep(self.config['monitor.conn_retry_sleep'])

        return result

    def prepareEntityEvents(self, events):
        result = []
        for event in events:
            result.append({
                'type': 'entityUpdate',
                'data': event
            })
        return result

    def loadInitialEventID(self):
        if self.latestEventLogEntry is None:
            LOG.debug("Loading initial EventLogEntry id")

            while True:
                conn_attempts = 0
                result = None
                order = [{'column': 'id', 'direction': 'desc'}]
                try:
                    _sg = self.connect()
                    result = _sg.find_one("EventLogEntry", filters=[], fields=['id', 'created_at'], order=order)
                    break
                except (sg.ProtocolError, sg.ResponseError, socket.error):
                    self.connect(force=True)
                    LOG.warning("Unable to connect to Shotgun (attempt {0} of {1})".format(conn_attempts + 1, self.config['monitor.max_conn_retries']))
                    conn_attempts += 1

                if conn_attempts >= self.config['monitor.max_conn_retries']:
                    LOG.warning("Unable to connect to Shotgun after max attempts, retrying in {0} seconds".format(self.config['monitor.conn_retry_sleep']))
                    time.sleep(self.config['monitor.conn_retry_sleep'])

            self.setLatestEventLogEntry(result)

    def setLatestEventLogEntry(self, entity):
        _entity = dict([(k, v) for k, v in entity.items() if k in ['id', 'created_at']])
        self.socket.send_pyobj({
            'type': 'latestEventLogEntry',
            'data': {
                'entity': _entity
            }
        })
        self.latestEventLogEntry = _entity

    def connect(self, force=False):
        if force or self.sg is None:
            LOG.debug("Connecting to Shotgun")
            self.sg = self.config.createShotgunConnection()
        return self.sg

    def fetchDelay(self):
        diff = 0
        if self._loopStartTime is not None:
            diff = time.time() - self._loopStartTime
        sleepTime = max(self.config['monitor.fetch_interval'] - diff, 0)
        if sleepTime:
            time.sleep(sleepTime)

    def setEntityTypes(self, entityTypes):
        self.entityTypes = entityTypes

    def buildBaseFilters(self):
        filters = []
        filters.extend(self.buildEntityTypeFilters())
        self.baseFilters = filters

    def buildEntityTypeFilters(self):
        result = {
            "filter_operator": "any",
            "filters": []
        }
        for entityType in self.entityTypes:
            for subType in self.eventSubTypes:
                eventType = 'Shotgun_{entityType}_{subType}'.format(
                    entityType=entityType,
                    subType=subType
                )
                result['filters'].append(['event_type', 'is', eventType])
        return [result]
