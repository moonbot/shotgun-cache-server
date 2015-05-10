import logging
import datetime
import gevent
import gevent.queue
from dateutil.parser import parse as dtparse

import rethinkdb as r

import entityConfig
import entityImporter
import monitor
import utils

__all__ = [
    'ShotgunCacheServer',
]

LOG = logging.getLogger(__name__)


class ShotgunCacheServer(object):
    """
    Main controller
    Launches all other processes and coordinates communication
    """
    def __init__(self, config):
        super(ShotgunCacheServer, self).__init__()
        self.config = config

        self.rethink = self.config.create_rethink_connection()
        self.sg = self.config.create_shotgun_connection()

        self.workQueue = gevent.queue.Queue()

        self.entityConfigManager = entityConfig.EntityConfigManager(config=self.config)
        self.monitor = monitor.ShotgunEventMonitor(self.config, self.workQueue)
        self.monitorGreenlet = None
        self.entityConfigWatcherGreenlet = None

        self.importer = entityImporter.EntityImporter(
            config=self.config,
            controller=self,
        )
        self.eventLogHandler = EventLogHandler(
            config=self.config,
            workQueue=self.workQueue,
            controller=self,
        )

    def start(self):
        LOG.info("Starting Up Cache")
        self.entityConfigManager.load()
        if not len(self.entityConfigManager.configs):
            raise IOError("No entity configs found to cache in {0}".format(self.config['entity_config_folder']))

        # Create the database
        dbName = self.rethink.db
        if self.rethink.db not in r.db_list().run(self.rethink):
            LOG.info("Creating rethink database: {0}".format(dbName))
            r.db_create(dbName).run(self.rethink)

        self.monitor.set_entity_types(self.entityConfigManager.get_entity_types())
        self.monitorGreenlet = gevent.spawn(self.monitor.start)

        self.entityConfigWatcherGreenlet = gevent.spawn(
            utils.watch_folder_for_changes,
            self.config.entityConfigFolderPath,
            self.importer.auto_import_entities,
            interval=self.config['history_check_interval']
        )

        self.run()

    def run(self):
        LOG.info("Starting Main Event Loop")
        while True:
            work = self.workQueue.get()

            if not isinstance(work, dict):
                raise TypeError("Invalid work item, expected dict: {0}".format(work))

            if LOG.getEffectiveLevel() < 10:
                # Only if we really want it
                LOG.debug("Work: \n" + utils.pretty_json(work))

            meth_name = 'handle_{0}'.format(work['type'])
            if hasattr(self, meth_name):
                getattr(self, meth_name)(work)
            else:
                raise ValueError("Unhandled work type: {0}".format(work['type']))

            gevent.sleep(0)

    def handle_monitor_started(self, work):
        # We wait until the monitor signals that it's running
        # this way we don't miss any events that occur while importing
        self.handle_import_entities(work)

    def handle_import_entities(self, work):
        LOG.info("Importing Cached Entities")
        self.importer.auto_import_entities()

    def handle_event_log_entries(self, work):
        eventLogEntries = work['data']['entities']
        LOG.info("Posting {0} Events".format(len(eventLogEntries)))
        self.eventLogHandler.process_entries(eventLogEntries)

    def handle_latest_event_log_entry(self, work):
        entity = work['data']['entity']
        LOG.debug("Setting Latest Event Log Entry ID to {0}".format(entity['id']))
        self.config.history.load()
        self.config.history['latest_event_log_entry'] = entity
        self.config.history.save()
        # TODO - need to ignore this save when importing entities?

    def handle_stat(self, work):
        return
        self.post_stat(work['data'])


class EventLogHandler(object):
    def __init__(self, controller, config, workQueue):
        super(EventLogHandler, self).__init__()
        self.controller = controller
        self.config = config
        self.sg = config.create_shotgun_connection()
        self.workQueue = workQueue
        self.rethink = config.create_rethink_connection()

    def process_entries(self, eventLogEntries):
        if LOG.getEffectiveLevel() < 10:
            LOG.debug("--- Event Log Entries ---\n{0}\n".format(utils.pretty_json(eventLogEntries)))

        eventLogEntries = self.filter_incoming_entries(eventLogEntries)

        if not len(eventLogEntries):
            return

        for entry in eventLogEntries:
            meta = entry['meta']
            entityType, changeType = entry['event_type'].split('_', 3)[1:]

            entityType = meta['entity_type']

            if entityType not in self.controller.entityConfigManager:
                # This also excludes special entities such as Version_sg_linked_versions_Connection
                LOG.debug("Ignoring uncached entity type: {0}".format(entityType))
                continue

            entityConfig = self.controller.entityConfigManager.get_config_for_type(entityType)

            meth_name = 'post_entry_{0}'.format(changeType)
            if hasattr(self, meth_name):
                getattr(self, meth_name)(entry, entityConfig)
            else:
                raise ValueError("Unhandled change type: {0}".format(changeType))

        submitFinishTime = datetime.datetime.utcnow()

        if self.config['enable_stats']:
            # Post the min/max/avg delay in milliseconds
            delays = []
            for entry in eventLogEntries:
                createdTime = entry['created_at']
                createdTime = utils.convert_str_to_datetime(createdTime)
                diff = submitFinishTime - createdTime
                diff_ms = diff.microseconds / float(1000)
                delays.append(diff_ms)

            avg = reduce(lambda x, y: x + y, delays) / len(delays)
            stat = {
                'type': 'post_to_cache',
                'min_shotgun_to_cache_delay': min(delays),
                'max_shotgun_to_cache_delay': max(delays),
                'avg_shotgun_to_cache_delay': avg,
                'created_at': submitFinishTime.isoformat(),
            }
            self.post_stat(stat)

    def filter_incoming_entries(self, eventLogEntries):
        result = []
        for entry in eventLogEntries:
            entityType, changeType = entry['event_type'].split('_', 3)[1:]
            importTimestamp = self.config.history.get('cached_entities', {}).get(entityType, {}).get('import-timestamp', None)
            if importTimestamp is None:
                continue

            importTimestamp = utils.convert_str_to_datetime(importTimestamp)

            # Ignore old event log entries
            entryTimestamp = utils.convert_str_to_datetime(entry['created_at'])
            if entryTimestamp < importTimestamp:
                LOG.debug("Ignoring EventLogEntry occurring before import of '{0}': {1}".format(entityType, entry['id']))
                if LOG.getEffectiveLevel() < 10:
                    LOG.debug("Old Entry: \n" + utils.pretty_json(entry))
                continue

            result.append(entry)
        return result

    def post_entry_Change(self, entry, entityConfig):
        meta = entry['meta']

        LOG.debug("Posting change to entity: {meta[entity_type]}:{meta[entity_id]}".format(meta=meta))

        field = entry['attribute_name']
        if field not in entityConfig['fields']:
            LOG.debug("Untracked field updated: {0}".format(field))
            return

        table = r.table(entityConfig['table'])
        entity = table.get(meta['entity_id'])

        fieldDataType = meta.get('field_data_type', '')
        if fieldDataType in ['multi_entity', 'entity']:
            if 'added' in meta and meta['added']:
                val = [utils.get_base_entity(e) for e in meta['added']]
                result = entity.update(lambda e: {field: e[field].splice_at(-1, val)}).run(self.rethink)
                if result['errors']:
                    raise IOError(result['first_error'])
            elif 'removed' in meta and meta['removed']:
                val = [utils.get_base_entity(e) for e in meta['removed']]
                result = entity.update(lambda e: {field: e[field].difference(val)}).run(self.rethink)
                if result['errors']:
                    raise IOError(result['first_error'])
            elif 'new_value' in meta:
                val = meta['new_value']
                if val is not None and isinstance(val, dict):
                    val = utils.get_base_entity(val)
                result = entity.update({field: val}).run(self.rethink)
                if result['errors']:
                    raise IOError(result['first_error'])
        elif fieldDataType in ['image']:
            if meta['new_value'] is not None:
                sgResult = self.sg.find_one(meta['entity_type'], [['id', 'is', meta['entity_id']]], [meta['attribute_name']])
                url = sgResult['image']
            else:
                url = None
            updateDict = utils.download_file_for_field(self.config, {'id': meta['entity_id'], 'type': meta['entity_type']}, meta['attribute_name'], url)
            result = entity.update(updateDict)
        else:
            if meta['entity_type'] not in self.controller.entityConfigManager.configs:
                LOG.debug("Ignoring entry for non-cached entity type: {0}".format(meta['entity_type']))
                return

            val = meta['new_value']

            if meta['field_data_type'] == 'date_time' and val is not None:
                # Parse the date time value
                # Can be in these 2 formats:
                #  - Fri Dec 10 06:00:00 UTC 2010
                #  - 2014-05-29 22:00:00 UTC
                val = dtparse(val)
                val = val.strftime("%Y-%m-%dT%H:%M:%SZ")

            result = entity.update({field: val}).run(self.rethink)
            if result['errors']:
                raise IOError(result['first_error'])

        # Check if we need to update the cached_display_name
        cachedDisplayNameLinks = entityConfig['fields'].get('cached_display_name', {}).get('dependent_fields', [])
        if field in cachedDisplayNameLinks:
            LOG.debug("Loading 'cached_display_name' for dependent field update '{0}' on entity type '{1}'".format(field, entityConfig.type))
            sgResult = self.sg.find(entityConfig.type, [['id', 'is', meta['entity_id']]], ['cached_display_name'])
            result = entity.update({'cached_display_name': val}).run(self.rethink)
            if result['errors']:
                raise IOError(result['first_error'])

        if 'updated_at' in entityConfig['fields']:
            result = entity.update({'updated_at': entry['created_at']}).run(self.rethink)
            if result['errors']:
                raise IOError(result['first_error'])

        if 'updated_by' in entityConfig['fields']:
            result = entity.update({'updated_by': utils.get_base_entity(entry['user'])}).run(self.rethink)
            if result['errors']:
                raise IOError(result['first_error'])

    def post_entry_New(self, entry, entityConfig):
        meta = entry['meta']
        entityType, changeType = entry['event_type'].split('_', 3)[1:]

        LOG.debug("Posting new entity: {meta[entity_type]}:{meta[entity_id]}".format(meta=meta))

        body = {'type': meta['entity_type'], 'id': meta['entity_id']}

        # Load the default values for each field
        for field in entityConfig['fields']:
            fieldSchema = self.controller.entityConfigManager.schema[entityType][field]

            if 'data_type' not in fieldSchema:
                # No data_type for visible field
                fieldDefault = None
            else:
                fieldType = fieldSchema['data_type']['value']
                fieldDefault = self.config['shotgun_field_type_defaults'].get(fieldType, None)

            body.setdefault(field, fieldDefault)

        # We don't get an update for the project field, so
        # we need to add that here from the event log entry
        body['project'] = entry['project']

        if 'created_at' in entityConfig['fields']:
            body['created_at'] = entry['created_at']
        if 'created_by' in entityConfig['fields']:
            body['created_by'] = utils.get_base_entity(entry['user'])

        if 'updated_at' in entityConfig['fields']:
            body['updated_at'] = entry['created_at']
        if 'updated_by' in entityConfig['fields']:
            body['updated_by'] = utils.get_base_entity(entry['user'])

        result = r.table(entityConfig['table']).insert(body).run(self.rethink)
        if result['errors']:
            raise IOError(result['first_error'])

    def post_entry_Retirement(self, entry, entityConfig):
        meta = entry['meta']
        LOG.debug("Deleting entity: {meta[entity_type]}:{meta[entity_id]}".format(meta=meta))

        result = r.table(entityConfig['table']).get(meta['entity_id']).delete().run(self.rethink)
        if result['errors']:
            raise IOError(result['first_error'])

    def post_entry_Revival(self, entry, entityConfig):
        meta = entry['meta']
        entityType, changeType = entry['event_type'].split('_', 3)[1:]

        LOG.debug("Reviving entity: {meta[entity_type]}:{meta[entity_id]}".format(meta=meta))

        # This is one of the few times we have to go and retrieve the information from shotgun
        filters = [['id', 'is', entry['entity']['id']]]
        body = self.sg.find_one(entityType, filters, entityConfig['fields'].keys())

        # Trim to base entities for nested entities
        for field in body:
            fieldSchema = self.controller.entityConfigManager.schema[entityType][field]

            if fieldSchema['data_type']['value'] in ['multi_entity', 'entity']:
                val = body[field]
                if isinstance(val, (list, tuple)):
                    val = [utils.get_base_entity(e) for e in val]
                else:
                    val = utils.get_base_entity()
                body[field] = val

        if 'updated_at' in entityConfig['fields']:
            body['updated_at'] = entry['created_at']
        if 'updated_by' in entityConfig['fields']:
            body['updated_by'] = utils.get_base_entity(entry['user'])

        result = r.table(entityConfig['table']).insert(body).run(self.rethink)
        if result['errors']:
            raise IOError(result['first_error'])

    def post_stat(self, statDict):
        if not self.config['enable_stats']:
            return

        LOG.debug("Posting stat: {0}".format(statDict['type']))
        statTable = self.config['rethink_stat_table_prefix'] + str(statDict['type'])
        if statTable not in r.table_list().run(self.rethink):
            r.table_create(statTable).run(self.rethink)

        r.table(statTable).insert(statDict).run(self.rethink)
