import logging
import datetime
import multiprocessing
import fnmatch

import zmq
import rethinkdb

import entityConfig
import entityImporter
import monitor
import utils

__all__ = [
    'DatabaseController',
]

LOG = logging.getLogger(__name__)


class DatabaseController(object):
    """
    Main Database controller
    Launches all other processes and coordinates communication
    """
    def __init__(self, config):
        super(DatabaseController, self).__init__()
        self.config = config

        self.rethink = self.config.createRethinkConnection()
        self.sg = self.config.createShotgunConnection()
        self.firstEventPostSinceImport = False

        self.entityConfigManager = entityConfig.EntityConfigManager(config=self.config)
        self._init_monitor()
        self._init_entityImportManager()

    def _init_monitor(self):
        self.monitorContext = zmq.Context()
        self.monitorSocket = self.monitorContext.socket(zmq.PULL)

        self.monitor = monitor.ShotgunEventMonitor(config=self.config)
        self.monitorProcess = multiprocessing.Process(target=self.monitor.start)
        self.monitorProcess.daemon = True

    def _init_entityImportManager(self):
        self.entityImportManager = entityImporter.ImportManager(
            config=self.config,
            controller=self,
        )

    def start(self):
        LOG.info("Starting Up Cache")
        self.entityConfigManager.load()
        if not len(self.entityConfigManager.configs):
            raise IOError("No entity configs found to cache in {0}".format(self.config['entity_config_folder']))

        self.monitor.setEntityTypes(self.entityConfigManager.getEntityTypes())
        self.monitorSocket.bind(self.config['zmq_controller_work_url'])
        self.monitorProcess.start()

        # Create the database
        dbName = self.rethink.db
        if self.rethink.db not in rethinkdb.db_list().run(self.rethink):
            LOG.info("Creating rethink database: {0}".format(dbName))
            rethinkdb.db_create(dbName).run(self.rethink)

        self.run()

    def run(self):
        LOG.info("Starting Main Event Loop")
        while True:
            work = self.monitorSocket.recv_pyobj()

            if not isinstance(work, dict):
                raise TypeError("Invalid work item, expected dict: {0}".format(work))

            if LOG.getEffectiveLevel() < 10:
                # Only if we really want it
                LOG.debug("Work: \n" + utils.prettyJson(work))

            meth_name = 'handle_{0}'.format(work['type'])
            if hasattr(self, meth_name):
                getattr(self, meth_name)(work)
            else:
                raise ValueError("Unhandled work type: {0}".format(work['type']))

    def handle_monitorStarted(self, work):
        # We wait until the monitor signals that it's running
        # this way we don't miss any events that occur while importing
        self.importEntities()

    def handle_stat(self, work):
        self.post_stat(work['data'])

    def handle_eventLogEntries(self, work):
        eventLogEntries = work['data']['entities']
        LOG.info("Posting {0} Events".format(len(eventLogEntries)))
        self.post_eventLogEntryChanges(eventLogEntries)

    def handle_reloadChangedConfigs(self, work):
        LOG.info("Reloading changed configs")
        self.config.history.load()
        self.importEntities()

    def handle_latestEventLogEntry(self, work):
        LOG.debug("Setting Latest Event Log Entry: {0}".format(work['data']['entity']))
        self.config.history['latest_event_log_entry'] = work['data']['entity']
        self.config.history.save()

    def importEntities(self):
        configs = self.entityConfigManager.allConfigs()

        def checkConfigNeedsUpdate(cacheConfig):
            if cacheConfig.hash is None:
                return True
            elif cacheConfig.hash != self.config.history.get('config_hashes', {}).get(cacheConfig.type, None):
                return True
            return False

        configsToLoad = []
        configsToLoad.extend(filter(checkConfigNeedsUpdate, configs))
        configsToLoad.extend(filter(lambda c: c.type not in self.config.history.get('cached_entity_types', {}), configs))

        # Deduplicate
        configsToLoad = dict([(c.type, c) for c in configsToLoad]).values()

        self.deleteUntrackedFromCache(configs)

        if not len(configsToLoad):
            LOG.info("All entity types have been imported.")
            return

        LOG.info("Importing {0} entity types into the cache".format(len(configsToLoad)))
        self.entityImportManager.importEntities(configsToLoad)
        LOG.info("Import complete!")
        self.firstEventPostSinceImport = True

    def deleteUntrackedFromCache(self, configs):
        """
        Delete data from cache for entities that are no longer cached
        """
        if not self.config['delete_cache_for_untracked_entities']:
            return

        # Get the list of cached entity types
        tableTemplate = self.config['rethink_entity_table_template']
        existingTables = rethinkdb.table_list().run(self.rethink)

        existingCacheTables = []
        tablePattern = tableTemplate.format(type="*")
        for table in existingTables:
            if fnmatch.fnmatch(table, tablePattern):
                existingCacheTables.append(table)

        usedCacheTables = [c['table'] for c in configs]
        unusedCacheTables = [t for t in existingCacheTables if t not in usedCacheTables]
        LOG.debug("Unusesd cache tables: {0}".format(unusedCacheTables))

        LOG.info("Deleting {0} cache tables".format(len(unusedCacheTables)))
        for table in unusedCacheTables:
            LOG.info("Deleting table: {0}".format(table))
            rethinkdb.table_drop(table).run(self.rethink)

    def post_entities(self, entityConfig, entities):
        LOG.debug("Posting entities")

        tableName = entityConfig['table']

        if tableName not in rethinkdb.table_list().run(self.rethink):
            LOG.debug("Creating table for type: {0}".format(entityConfig.type))
            rethinkdb.table_create(tableName).run(self.rethink)

        for entity in entities:
            # Get rid of extra data found in sub-entities
            # We don't have a way to reliably keep these up to date except
            # for the type and id
            entitySchema = self.entityConfigManager.schema[entityConfig.type]
            for field, val in entity.items():
                if field not in entitySchema:
                    continue
                if field in ['type', 'id']:
                    continue
                fieldDataType = entitySchema[field].get('data_type', {}).get('value', None)
                if fieldDataType == 'multi_entity':
                    val = [utils.getBaseEntity(e) for e in val]
                    entity[field] = val
                elif fieldDataType == 'entity':
                    val = utils.getBaseEntity(val)
                    entity[field] = val

        # TODO
        # Since we aren't deleting the existing table
        # Need to delete any extra entities at the end
        result = rethinkdb.table(tableName).insert(entities, conflict="replace").run(self.rethink)
        if result['errors']:
            raise IOError(result['first_error'])

        if result['errors']:
            # TODO
            # Better error descriptions?
            raise IOError("Errors occurred creating entities: {0}".format(result))

    def post_eventLogEntryChanges(self, eventLogEntries):
        if LOG.getEffectiveLevel() < 10:
            LOG.debug("--- Event Log Entries ---\n{0}\n".format(utils.prettyJson(eventLogEntries)))

        eventLogEntries = self.filterEventsBeforeImport(eventLogEntries)

        if not len(eventLogEntries):
            return

        for entry in eventLogEntries:
            meta = entry['meta']
            entityType, changeType = entry['event_type'].split('_', 3)[1:]

            entityType = meta['entity_type']
            # entityID = meta['entity_id']

            if entityType not in self.entityConfigManager:
                # This also excludes special entities such as Version_sg_linked_versions_Connection
                LOG.debug("Ignoring uncached entity type: {0}".format(entityType))
                continue

            entityConfig = self.entityConfigManager.getConfigForType(entityType)

            meth_name = 'post_eventlog_{0}'.format(changeType)
            if hasattr(self, meth_name):
                getattr(self, meth_name)(entry, entityConfig)
            else:
                raise ValueError("Unhandled change type: {0}".format(changeType))

        submitFinishTime = datetime.datetime.utcnow()

        # if LOG.getEffectiveLevel() < 10:
        #     LOG.debug("--- Responses ---\n{0}\n".format(utils.prettyJson(responses)))

        # # It's possible to run into a few errors here when shotgun events occurred while importing
        # # Someone could modify and then delete an entity before the import is finished
        # # while importing, we capture this change
        # # then after importing, we apply the modification from the events that occured while importing
        # # however, we can't apply the modification because the entity no longer exists
        # # So on the first event post after importing we ignore DocumentMissingException's
        # if responses['errors']:
        #     ignoreError = False
        #     for request, response in zip(requests, responses['items']):
        #         for responseEntry in response.values():
        #             if responseEntry.get('error', None):
        #                 errStr = responseEntry['error']
        #                 if errStr.startswith('DocumentMissingException') and self.firstEventPostSinceImport:
        #                     LOG.warning("Got DocumentMissingException error, but ignoring because it was during import")
        #                     ignoreError = True
        #                 else:
        #                     LOG.error(errStr)
        #                     LOG.debug("Request:\n{0}".format(utils.prettyJson(request)))
        #                     LOG.debug("Response:\n{0}".format(utils.prettyJson(response)))
        #     if not ignoreError:
        #         raise IOError("Errors occurred creating entities")

        if self.firstEventPostSinceImport:
            self.firstEventPostSinceImport = False

        if self.config['enable_stats']:
            # Post the min/max/avg delay in milliseconds
            delays = []
            for entry in eventLogEntries:
                createdTime = entry['created_at']
                createdTime = utils.convertStrToDatetime(createdTime)
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

    def post_stat(self, statDict):
        if not self.config['enable_stats']:
            return

        LOG.debug("Posting stat: {0}".format(statDict['type']))
        statTable = self.config['rethink_stat_table_template'].format(type=statDict['type'])
        if statTable not in rethinkdb.table_list().run(self.rethink):
            rethinkdb.table_create(statTable).run(self.rethink)

        rethinkdb.table(statTable).insert(statDict).run(self.rethink)

    def filterEventsBeforeImport(self, eventLogEntries):
        result = []
        for entry in eventLogEntries:
            entityType, changeType = entry['event_type'].split('_', 3)[1:]
            importTimestamps = self.config.history['cached_entity_types'][entityType]
            importTimestamp = utils.convertStrToDatetime(importTimestamps['startImportTimestamp'])

            # Ignore old event log entries
            entryTimestamp = utils.convertStrToDatetime(entry['created_at'])
            if entryTimestamp < importTimestamp:
                LOG.debug("Ignoring EventLogEntry occurring before import of '{0}': {1}".format(entityType, entry['id']))
                if LOG.getEffectiveLevel() < 10:
                    LOG.debug("Old Entry: \n" + utils.prettyJson(entry))
                continue

            result.append(entry)
        return result

    def post_eventlog_Change(self, entry, entityConfig):
        meta = entry['meta']

        LOG.debug("Posting change to entity: {meta[entity_type]}:{meta[entity_id]}".format(meta=meta))

        attrName = entry['attribute_name']
        if attrName not in entityConfig['fields']:
            LOG.debug("Untracked field updated: {0}".format(attrName))
            return

        table = rethinkdb.table(meta['entity_type'])
        entity = table.get(meta['entity_id'])

        if meta.get('field_data_type', '') in ['multi_entity', 'entity']:
            if 'added' in meta and meta['added']:
                val = [utils.getBaseEntity(e) for e in meta['added']]
                result = entity.update(lambda e: {attrName: e[attrName].splice_at(-1, val)}).run(self.rethink)
                if result['errors']:
                    raise IOError(result['first_error'])
            elif 'removed' in meta and meta['removed']:
                val = [utils.getBaseEntity(e) for e in meta['removed']]
                result = entity.update(lambda e: {attrName: e[attrName].difference(val)}).run(self.rethink)
                if result['errors']:
                    raise IOError(result['first_error'])
            elif 'new_value' in meta:
                val = meta['new_value']
                if val is not None and isinstance(val, dict):
                    val = utils.getBaseEntity(val)
                result = entity.update({attrName: val}).run(self.rethink)
                if result['errors']:
                    raise IOError(result['first_error'])

        else:
            if meta['entity_type'] not in self.entityConfigManager.configs:
                LOG.debug("Ignoring entry for non-cached entity type: {0}".format(meta['entity_type']))
                return

            val = meta['new_value']
            result = entity.update({attrName: val}).run(self.rethink)
            if result['errors']:
                raise IOError(result['first_error'])

        if 'updated_at' in entityConfig['fields']:
            result = entity.update({'updated_at': entry['created_at']}).run(self.rethink)
            if result['errors']:
                raise IOError(result['first_error'])

        if 'updated_by' in entityConfig['fields']:
            result = entity.update({'updated_by': utils.getBaseEntity(entry['user'])}).run(self.rethink)
            if result['errors']:
                raise IOError(result['first_error'])

    def post_eventlog_New(self, entry, entityConfig):
        meta = entry['meta']
        entityType, changeType = entry['event_type'].split('_', 3)[1:]

        LOG.debug("Posting new entity: {meta[entity_type]}:{meta[entity_id]}".format(meta=meta))

        body = {'type': meta['entity_type'], 'id': meta['entity_id']}

        # Load the default values for each field
        for field in entityConfig['fields']:
            fieldSchema = self.entityConfigManager.schema[entityType][field]

            if 'data_type' not in fieldSchema:
                # No data_type for visible field
                fieldDefault = None
            else:
                fieldType = fieldSchema['data_type']['value']
                fieldDefault = self.config['shotgun_field_type_defaults'].get(fieldType, None)

            body.setdefault(field, fieldDefault)

        if 'created_at' in entityConfig['fields']:
            body['created_at'] = entry['created_at']
        if 'created_by' in entityConfig['fields']:
            body['created_by'] = utils.getBaseEntity(entry['user'])

        if 'updated_at' in entityConfig['fields']:
            body['updated_at'] = entry['created_at']
        if 'updated_by' in entityConfig['fields']:
            body['updated_by'] = utils.getBaseEntity(entry['user'])

        result = rethinkdb.table(entityConfig['table']).insert(body).run(self.rethink)
        if result['errors']:
            raise IOError(result['first_error'])

    def post_eventlog_Retirement(self, entry, entityConfig):
        meta = entry['meta']
        LOG.debug("Deleting entity: {meta[entity_type]}:{meta[entity_id]}".format(meta=meta))

        result = rethinkdb.table(entityConfig['table']).get(meta['entity_id']).delete().run(self.rethink)
        if result['errors']:
            raise IOError(result['first_error'])

    def post_eventlog_Revival(self, entry, entityConfig):
        meta = entry['meta']
        entityType, changeType = entry['event_type'].split('_', 3)[1:]

        LOG.debug("Reviving entity: {meta[entity_type]}:{meta[entity_id]}".format(meta=meta))

        # This is one of the few times we have to go and retrieve the information from shotgun
        filters = [['id', 'is', entry['entity']['id']]]
        body = self.sg.find_one(entityType, filters, entityConfig['fields'].keys())

        # Trim to base entities for nested entities
        for field in body:
            fieldSchema = self.entityConfigManager.schema[entityType][field]

            if fieldSchema['data_type']['value'] in ['multi_entity', 'entity']:
                val = body[field]
                if isinstance(val, (list, tuple)):
                    val = [utils.getBaseEntity(e) for e in val]
                else:
                    val = utils.getBaseEntity()
                body[field] = val

        if 'updated_at' in entityConfig['fields']:
            body['updated_at'] = entry['created_at']
        if 'updated_by' in entityConfig['fields']:
            body['updated_by'] = utils.getBaseEntity(entry['user'])

        result = rethinkdb.table(meta['entity_type']).insert(body).run(self.rethink)
        if result['errors']:
            raise IOError(result['first_error'])
