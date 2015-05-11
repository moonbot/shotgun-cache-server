import time
import datetime
import socket
import logging

import shotgun_api3 as sg

__all__ = [
    'ShotgunEventMonitor',
]

LOG = logging.getLogger(__name__)

processStartTime = time.time()


class ShotgunEventMonitor(object):
    """
    Stripped down version of the Shotgun Event Daemon
    Records when EventLogEntry's have changed in shotgun
    and sends the changes to the DatabaseController through zmq
    """
    eventSubTypes = ['New', 'Change', 'Retirement', 'Revival']

    def __init__(self, config, workQueue):
        super(ShotgunEventMonitor, self).__init__()
        self.config = config
        self.workQueue = workQueue

        self.latestEventLogEntry = self.config.history.get('latest_event_log_entry', None)

        self.entityTypes = []

        self.sg = None

        self._latestEventID = None
        self._latestEventIDPath = None
        self._loopStartTime = None

    def set_entity_types(self, entityTypes):
        self.entityTypes = entityTypes

    def start(self):
        self.load_initial_eventID()
        self.build_base_filters()
        self.run()

    def run(self):
        LOG.info("Monitoring Shotgun")
        totalEventsInLoop = 0
        reset = False
        heartbeatTime = None
        self.workQueue.put({'type': 'monitor_started'})
        while True:
            LOG.debug("Checking for new events")
            if reset:
                totalEventsInLoop = 0
                timeToPost = 0
                self._loopStartTime = time.time()

            events = self.get_new_events()

            if len(events):
                LOG.debug("Received {0} new events".format(len(events)))

                postStartTime = time.time()
                work = {
                    'type': 'event_log_entries',
                    'data': {
                        'entities': events,
                    },
                }

                self.workQueue.put(work)
                timeToPost = time.time() - postStartTime

                self.set_latest_event_log_entry(events[-1])
                totalEventsInLoop += len(events)
                reset = False
            else:
                self.fetch_delay()
                reset = True

                # Only report status for loops with events
                if totalEventsInLoop:
                    work = {
                        'type': 'stat',
                        'data': {
                            'type': 'shotgun_event_update',
                            'total_events': totalEventsInLoop,
                            # might not need this, most of the time below 1 ms
                            'duration': round(timeToPost * 1000, 3),  # ms
                            'created_at': datetime.datetime.utcnow().isoformat(),
                        },
                    }
                    self.workQueue.put(work)

            # Track monitor status so we can graph when it goes down
            currTime = time.time()
            if heartbeatTime is None or currTime - heartbeatTime > self.config['monitor']['heartbeat_interval']:
                work = {
                    'type': 'stat',
                    'data': {
                        'type': 'monitor_status',
                        'created_at': datetime.datetime.utcnow().isoformat(),
                        'uptime': time.time() - processStartTime,  # seconds
                    },
                }
                self.workQueue.put(work)
                heartbeatTime = currTime

    def get_new_events(self):
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
                LOG.warning("Unable to connect to Shotgun (attempt {0} of {1})".format(conn_attempts + 1, self.config['monitor.max_conn_retries']))
                conn_attempts += 1
            except Exception:
                LOG.warning("Unable to connect to Shotgun (attempt {0} of {1})".format(conn_attempts + 1, self.config['monitor.max_conn_retries']))
                conn_attempts += 1

            if conn_attempts >= self.config['monitor.max_conn_retries']:
                LOG.warning("Unable to connect to Shotgun after max attempts, retrying in {0} seconds".format(self.config['monitor.conn_retry_sleep']))
                time.sleep(self.config['monitor.conn_retry_sleep'])

        return result

    def load_initial_eventID(self):
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

            self.set_latest_event_log_entry(result)

    def set_latest_event_log_entry(self, entity):
        _entity = dict([(k, v) for k, v in entity.items() if k in ['id', 'created_at']])
        work = {
            'type': 'latest_event_log_entry',
            'data': {
                'entity': _entity
            }
        }
        self.workQueue.put(work)
        self.latestEventLogEntry = _entity

    def connect(self, force=False):
        if force or self.sg is None:
            LOG.debug("Connecting to Shotgun")
            self.sg = self.config.create_shotgun_connection()
        return self.sg

    def fetch_delay(self):
        diff = 0
        if self._loopStartTime is not None:
            diff = time.time() - self._loopStartTime
        sleepTime = max(self.config['monitor.fetch_interval'] - diff, 0)
        if sleepTime:
            time.sleep(sleepTime)

    def build_base_filters(self):
        filters = []
        filters.extend(self.build_entity_type_filters())
        self.baseFilters = filters

    def build_entity_type_filters(self):
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
