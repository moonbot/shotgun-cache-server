import logging
import datetime
import multiprocessing
import fnmatch

import zmq

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

        self.elastic = self.config.createElasticConnection()
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
        entityIndexTemplate = self.config['elastic_entity_index_template']
        existingIndices = self.elastic.indices.status()['indices'].keys()

        existingCacheIndices = []
        pattern = entityIndexTemplate.format(type="*")
        for index in existingIndices:
            if fnmatch.fnmatch(index, pattern):
                existingCacheIndices.append(index)

        usedCachedIndices = [c['index'] for c in configs]
        unusedCacheIndices = [i for i in existingCacheIndices if i not in usedCachedIndices]
        LOG.debug("Unusesd cache indices: {0}".format(unusedCacheIndices))

        LOG.info("Deleting {0} cache indexes".format(len(unusedCacheIndices)))
        for index in unusedCacheIndices:
            LOG.info("Deleting index: {0}".format(index))
            self.elastic.indices.delete(index=index)

    def post_entities(self, entityConfig, entities):
        requests = []

        LOG.debug("Posting entities")
        if not self.elastic.indices.exists(index=entityConfig['index']):
            mappings = self.buildElasticFieldMappings(entityConfig)

            body = {'mappings': mappings}
            LOG.debug("Creating elastic index for type: {0}".format(entityConfig.type))
            self.elastic.indices.create(index=entityConfig['index'], body=body)

        for entity in entities:
            header = {
                'index': {
                    '_index': entityConfig['index'],
                    '_type': entityConfig['doc_type'],
                    '_id': entity['id'],
                }
            }

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

            body = entity
            requests.extend([header, body])

        responses = self.elastic.bulk(body=requests)

        if responses['errors']:
            for response in responses['items']:
                if 'error' not in response.get('index', {}):
                    continue
                if response['index']['error']:
                    LOG.exception(response['index']['error'])
            raise IOError("Errors occurred creating entities")

    def post_eventLogEntryChanges(self, eventLogEntries):
        if LOG.getEffectiveLevel() < 10:
            LOG.debug("--- Event Log Entries ---\n{0}\n".format(utils.prettyJson(eventLogEntries)))

        eventLogEntries = self.filterEventsBeforeImport(eventLogEntries)

        requests = []
        for entry in eventLogEntries:
            meta = entry['meta']
            entityType, changeType = entry['event_type'].split('_', 3)[1:]

            entityType = meta['entity_type']
            entityID = meta['entity_id']
            # TODO
            # Cleanup
            # if 'entity_type' in meta:
            # else:
            #     entityID = entry['entity']['id']

            if entityType not in self.entityConfigManager:
                # This also excludes special entities such as Version_sg_linked_versions_Connection
                LOG.debug("Ignoring uncached entity type: {0}".format(entityType))
                continue

            entityConfig = self.entityConfigManager.getConfigForType(entityType)

            headerInfo = {
                '_index': entityConfig['index'],
                '_type': entityConfig['doc_type'],
                '_id': entityID,
            }

            meth_name = 'generateRequests_{0}'.format(changeType)
            if hasattr(self, meth_name):
                newRequests = getattr(self, meth_name)(entry, entityConfig, headerInfo)
                if newRequests:
                    requests.extend(newRequests)
            else:
                raise ValueError("Unhandled change type: {0}".format(changeType))

        if LOG.getEffectiveLevel() < 10:
            LOG.debug("--- Requests ---\n{0}\n".format(utils.prettyJson(requests)))

        if not len(requests):
            LOG.debug("No requests for elastic")
            return

        responses = self.elastic.bulk(body=requests)
        submitFinishTime = datetime.datetime.utcnow()

        if LOG.getEffectiveLevel() < 10:
            LOG.debug("--- Responses ---\n{0}\n".format(utils.prettyJson(responses)))

        # It's possible to run into a few errors here when shotgun events occurred while importing
        # Someone could modify and then delete an entity before the import is finished
        # while importing, we capture this change
        # then after importing, we apply the modification from the events that occured while importing
        # however, we can't apply the modification because the entity no longer exists
        # So on the first event post after importing we ignore DocumentMissingException's
        if responses['errors']:
            ignoreError = False
            for request, response in zip(requests, responses['items']):
                for responseEntry in response.values():
                    if responseEntry.get('error', None):
                        errStr = responseEntry['error']
                        if errStr.startswith('DocumentMissingException') and self.firstEventPostSinceImport:
                            LOG.warning("Got DocumentMissingException error, but ignoring because it was during import")
                            ignoreError = True
                        else:
                            LOG.error(errStr)
                            LOG.debug("Request:\n{0}".format(utils.prettyJson(request)))
                            LOG.debug("Response:\n{0}".format(utils.prettyJson(response)))
            if not ignoreError:
                raise IOError("Errors occurred creating entities")

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
                'elastic_requests': len(requests),
                'elastic_bulk_took': responses['took'],
                'created_at': submitFinishTime.isoformat(),
            }
            self.post_stat(stat)

    def post_stat(self, statDict):
        if not self.config['enable_stats']:
            return

        LOG.debug("Posting stat: {0}".format(statDict['type']))
        statIndex = self.config['elastic_stat_index_template'].format(type=statDict['type'])
        if not self.elastic.indices.exists(index=statIndex):
            self.elastic.indices.create(index=statIndex, body={})
        self.elastic.index(index=statIndex, doc_type=statDict['type'], body=statDict)

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

    def generateRequests_Change(self, entry, entityConfig, headerInfo):
        requests = []

        header = {'update': headerInfo}
        meta = entry['meta']

        attrName = entry['attribute_name']
        if attrName not in entityConfig['fields']:
            LOG.debug("Untracked field updated: {0}".format(attrName))
            return

        if meta.get('field_data_type', '') in ['multi_entity', 'entity']:
            if 'added' in meta and meta['added']:
                val = [utils.getBaseEntity(e) for e in meta['added']]
                body = {
                    "script": "ctx._source.{0} += item".format(entry['attribute_name']),
                    "params": {
                        "item": val,
                    }
                }
            elif 'removed' in meta and meta['removed']:
                val = [utils.getBaseEntity(e) for e in meta['removed']]
                body = {
                    "script": "ctx._source.{0} -= item".format(entry['attribute_name']),
                    "params": {
                        "item": val,
                    }
                }
            elif 'new_value' in meta:
                val = meta['new_value']
                if val is not None and isinstance(val, dict):
                    val = utils.getBaseEntity(val)
                body = {"doc": {attrName: val}}

        else:
            if meta['entity_type'] not in self.entityConfigManager.configs:
                LOG.debug("Ignoring entry for non-cached entity type: {0}".format(meta['entity_type']))
                return

            val = meta['new_value']
            body = {"doc": {attrName: val}}

        requests.extend([header, body])

        if 'updated_at' in entityConfig['fields']:
            body = {"doc": {'updated_at': entry['created_at']}}
            requests.extend([header, body])

        if 'updated_at' in entityConfig['fields']:
            body = {"doc": {'updated_by': utils.getBaseEntity(entry['user'])}}
            requests.extend([header, body])

        return requests

    def generateRequests_New(self, entry, entityConfig, headerInfo):
        requests = []
        meta = entry['meta']
        entityType, changeType = entry['event_type'].split('_', 3)[1:]

        header = {'index': headerInfo}
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

        requests.extend([header, body])
        return requests

    def generateRequests_Retirement(self, entry, entityConfig, headerInfo):
        requests = []
        header = {'delete': headerInfo}
        requests.extend([header])
        return requests

    def generateRequests_Revival(self, entry, entityConfig, headerInfo):
        requests = []
        entityType, changeType = entry['event_type'].split('_', 3)[1:]
        # This is one of the few times
        # we have to go and retrieve the information from shotgun
        header = {'index': headerInfo}
        filters = [['id', 'is', entry['entity']['id']]]
        filters.extend(entityConfig.get('filters', []))
        # TODO
        # I could batch these for multiple revives per batch...
        body = self.sg.find_one(entityType, filters, entityConfig['fields'].keys())
        requests.extend([header, body])
        return requests

    def buildElasticFieldMappings(self, entityConfig):
        typeMappings = {}
        typeMappings.setdefault('dynamic_templates', entityConfig.get('dynamic_templates', []))

        for field, settings in entityConfig.get('fields', {}).items():
            if 'mapping' in settings:
                prop = typeMappings.setdefault('properties', {})
                prop[field] = settings['mapping']

        result = {
            entityConfig['doc_type']: typeMappings,
        }

        return result
