import socket
import time
import zmq

import logging

import shotgun_api3 as sg

__all__ = [
    'ShotgunEventMonitor',
]

LOG = logging.getLogger(__name__)
LOG.level = 10


class ShotgunEventMonitor(object):
    """
    Stripped down version of the Shotgun Event Daemon
    Records when EventLogEntry's have changed in shotgun
    """
    eventSubTypes = ['New', 'Change', 'Retirement', 'Revival']

    def __init__(self, zmqPostUrl, shotgunConnector, latestEventID=None,
                 maxConnRetries=5, connRetrySleep=60, maxEventBatchSize=500,
                 fetchInterval=1, enableStats=True):
        super(ShotgunEventMonitor, self).__init__()
        self.zmqPostUrl = zmqPostUrl
        self.shotgunConnector = shotgunConnector
        self.latestEventID = latestEventID
        self.maxConnRetries = maxConnRetries
        self.connRetrySleep = connRetrySleep
        self.maxEventBatchSize = maxEventBatchSize
        self.fetchInterval = fetchInterval
        self.enableStats = enableStats

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
        self.socket.connect(self.zmqPostUrl)
        self.loadInitialEventID()
        self.buildBaseFilters()
        self.run()

    def run(self):
        totalEventsInLoop = 0
        reset = False
        while True:
            if reset:
                totalEventsInLoop = 0
                timeToPost = 0
                self._loopStartTime = time.time()

            events = self.getNewEvents()

            if len(events):
                LOG.debug("{0} new EventLogEntrys".format(len(events)))

                postStartTime = time.time()
                entityEvents = self.prepareEntityEvents(events)
                self.postItems(entityEvents)
                timeToPost = time.time() - postStartTime

                self.updateLatestEventID(events)
                totalEventsInLoop += len(events)
                reset = False
            else:
                self.fetchDelay()
                reset = True

                # Only report status for loops with events
                if totalEventsInLoop:
                    self.postStat({
                        'statType': 'monitor_post',
                        'fetchInterval': self.fetchInterval,
                        'totalEvents': totalEventsInLoop,
                        'timeToPost': timeToPost,
                    })

    def prepareEntityEvents(self, events):
        result = []
        for event in events:
            result.append({
                'type': 'entityUpdate',
                'data': event
            })
        return result

    def loadInitialEventID(self):
        if self.latestEventID is None:
            LOG.debug("Loading initial EventLogEntry id")

            while True:
                conn_attempts = 0
                result = None
                order = [{'column': 'id', 'direction': 'desc'}]
                try:
                    _sg = self.connect()
                    result = _sg.find_one("EventLogEntry", filters=[], fields=['id'], order=order)
                    break
                except (sg.ProtocolError, sg.ResponseError, socket.error):
                    self.connect(force=True)
                    LOG.warning("Unable to connect to Shotgun (attempt {0} of {1})".format(conn_attempts + 1, self.maxConnRetries))
                    conn_attempts += 1

                if conn_attempts >= self.maxConnRetries:
                    LOG.warning("Unable to connect to Shotgun after max attempts, retrying in {0} seconds".format(self.connRetrySleep))
                    time.sleep(self.connRetrySleep)

            self.setLatestEventID(result['id'])

    def updateLatestEventID(self, events):
        # Extract latest event ID
        ids = [e['id'] for e in events]
        eventID = max(ids)
        self.setLatestEventID(eventID)

    def setLatestEventID(self, eventID):
        self.postItems([{'type': 'latestEventID', 'data': {'eventID': eventID}}])
        self.latestEventID = eventID

    def connect(self, force=False):
        if force or self.sg is None:
            LOG.debug("Connecting to Shotgun")
            self.sg = self.shotgunConnector.getInstance()
        return self.sg

    def postItems(self, items):
        LOG.debug("Posting {0} items".format(len(items)))
        for item in items:
            self.socket.send_pyobj(item)

    def fetchDelay(self):
        diff = 0
        if self._loopStartTime is not None:
            diff = time.time() - self._loopStartTime
        sleepTime = max(self.fetchInterval - diff, 0)
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

    def getNewEvents(self):
        """
        Fetch the new EventLogEntry entities from Shotgun
        Loops until successful
        """
        filters = [
            ['id', 'greater_than', self.latestEventID]
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
                result = _sg.find("EventLogEntry", filters, fields, order, limit=self.maxEventBatchSize)
                break
            except (sg.ProtocolError, sg.ResponseError, socket.error):
                self.connect(force=True)
                LOG.warning("Unable to connect to Shotgun (attempt {0} of {1})".format(conn_attempts + 1, self.maxConnRetries))
                conn_attempts += 1

            if conn_attempts >= self.maxConnRetries:
                LOG.warning("Unable to connect to Shotgun after max attempts, retrying in {0} seconds".format(self.connRetrySleep))
                time.sleep(self.connRetrySleep)

        return result

    def postStat(self, statDict):
        if self.enableStats:
            self.postItems([{
                'type': 'stat',
                'data': statDict
            }])
