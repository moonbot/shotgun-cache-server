import logging
import datetime
import multiprocessing

import zmq

import entityConfig
import initialImport
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
        self.history = utils.History(self.config['historyFile'])

        self._init_entityConfigManager()
        self._init_monitor()
        self._init_initialImportManager()

    def _init_entityConfigManager(self):
        self.entityConfigManager = entityConfig.EntityConfigManager(
            config=self.config,
            previousHashes=self.history.get('configHashes', {}),
        )

    def _init_monitor(self):
        self.monitorContext = zmq.Context()
        self.monitorSocket = self.monitorContext.socket(zmq.PULL)
        self.monitorSocket.bind(self.config['zmqListenUrl'])

        self.monitor = monitor.ShotgunEventMonitor(
            config=self.config,
            latestEventLogEntry=self.history.get('latestEventLogEntry', None),
        )
        self.monitorProcess = multiprocessing.Process(target=self.monitor.start)
        self.monitorProcess.daemon = True

    def _init_initialImportManager(self):
        self.initialImportManager = initialImport.InitialImportManager(
            config=self.config,
            controller=self,
        )

    def start(self):
        LOG.info("Starting Up Cache")
        self.entityConfigManager.load()
        if not len(self.entityConfigManager.configs):
            raise IOError("No entity configs found to cache in {0}".format(self.config['entityConfigFolder']))

        self.monitor.setEntityTypes(self.entityConfigManager.getEntityTypes())
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
        self.performInitialImport()

    def handle_stat(self, work):
        if self.config['enableStats']:
            LOG.debug("Posting Stat: {0}".format(work['data']['type']))

    def handle_eventLogEntries(self, work):
        eventLogEntries = work['data']['entities']
        LOG.info("Posting {0} Events".format(len(eventLogEntries)))
        self.post_eventLogEntries(eventLogEntries)

    def handle_reloadEntities(self, work):
        entityTypes = work['data'].get('entityTypes', [])
        if entityTypes:
            LOG.info("Performing initial import again for {0} entity types".format(len(entityTypes)))
            # Reload only the supplied entity types
            configs = [self.entityConfigManager.getConfigForType(t) for t in entityTypes]
            self.initialImportManager.importEntities(configs)
        else:
            LOG.info("Performing initial import again".format(len(entityTypes)))
            # Reload entities that are out not loaded or config has changed)
            self.performInitialImport()

    def handle_latestEventLogEntry(self, work):
        LOG.debug("Setting Latest Event Log Entry: {0}".format(work['data']['entity']))
        self.history['latestEventLogEntry'] = work['data']['entity']
        self.history.save()

    def performInitialImport(self):
        configs = self.entityConfigManager.configs.values()

        configsToLoad = []
        configsToLoad.extend(filter(lambda c: c.needsUpdate(), configs))
        configsToLoad.extend(filter(lambda c: c.type not in self.history.get('cachedEntityTypes', {}), configs))

        if not len(configsToLoad):
            LOG.info("All entity types have been imported.")
            return

        LOG.info("Importing {0} entity types into the cache".format(len(configsToLoad)))
        self.initialImportManager.importEntities(configsToLoad)
        LOG.info("Initial entity import complete!")
        self.firstEventPostSinceImport = True

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
                    '_type': entityConfig.type.lower(),
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

    def post_eventLogEntries(self, eventLogEntries):

        # print '-- eventLogEntries --'
        # print json.dumps(eventLogEntries, sort_keys=True, indent=4, separators=(',', ': '))
        # print

        eventLogEntries = self.filterEventsBeforeImport(eventLogEntries)

        requests = []
        for entry in eventLogEntries:
            entityType, changeType = entry['event_type'].split('_', 3)[1:]
            entityConfig = self.entityConfigManager.getConfigForType(entityType)

            meta = entry['meta']

            headerInfo = {
                '_index': entityConfig['index'],
                '_type': entityType.lower(),
                '_id': entry['entity']['id'] if entry['entity'] else meta['entity_id'],
            }

            meth_name = 'generateRequests_{0}'.format(changeType)
            if hasattr(self, meth_name):
                getattr(self, meth_name)(entry, headerInfo)
            else:
                raise ValueError("Unhandled change type: {0}".format(changeType))

        # print '-- requests --'
        # print json.dumps(requests, sort_keys=True, indent=4, separators=(',', ': '))
        # print

        if not len(requests):
            LOG.debug("No requests for elastic")
            return

        responses = self.elastic.bulk(body=requests)
        submitFinishTime = datetime.datetime.utcnow()

        # print '-- responses --'
        # print json.dumps(responses, sort_keys=True, indent=4, separators=(',', ': '))
        # print

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
            if not ignoreError:
                raise IOError("Errors occurred creating entities")

        if self.firstEventPostSinceImport:
            self.firstEventPostSinceImport = False

        if self.config['enableStats']:
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
                'type': 'post_to_elastic',
                'min_shotgun_to_cache_delay': min(delays),
                'max_shotgun_to_cache_delay': max(delays),
                'avg_shotgun_to_cache_delay': avg,
                'elastic_requests': len(requests),
                'elastic_bulk_took': responses['took'],
                'created_at': submitFinishTime.isoformat(),
            }
            self.post_stat(stat)

    def post_stat(self, statDict):
        if not self.config['enableStats']:
            return

        LOG.debug("Posting stat: {0}".format(statDict['type']))
        statIndex = self.config['elasticStatIndexTemplate'].format(type=statDict['type'])
        if not self.elastic.indices.exists(index=statIndex):
            self.elastic.indices.create(index=statIndex, body={})
        self.elastic.index(index=statIndex, doc_type=statDict['type'], body=statDict)

    def filterEventsBeforeImport(self, eventLogEntries):
        result = []
        for entry in eventLogEntries:
            entityType, changeType = entry['event_type'].split('_', 3)[1:]
            importTimestamps = self.history['cachedEntityTypes'][entityType]
            initialImportTimestamp = utils.convertStrToDatetime(importTimestamps['startImportTimestamp'])

            # Ignore old event log entries
            entryTimestamp = utils.convertStrToDatetime(entry['created_at'])
            if entryTimestamp < initialImportTimestamp:
                LOG.debug("Ignoring EventLogEntry occurring before initial import of '{0}': {1}".format(entityType, entry['id']))
                if LOG.getEffectiveLevel() < 10:
                    LOG.debug("Old Entry: \n" + utils.prettyJson(entry))
                continue

            result.append(entry)
        return result

    def generateRequests_Change(self, entry, headerInfo):
        requests = []

        header = {'update': headerInfo}
        meta = entry['meta']

        attrName = entry['attribute_name']
        if attrName not in entityConfig['fields']:
            LOG.debug("Untracked field updated: {0}".format(attrName))
            return

        if meta.get('field_data_type', '') in ['multi_entity', 'entity']:
            if 'added' in meta:
                val = [utils.getBaseEntity(e) for e in meta['added']]
                body = {
                    "script": "ctx._source.{0} += item".format(entry['attribute_name']),
                    "params": {
                        "item": val,
                    }
                }
            elif 'new_value' in meta:
                val = meta['new_value']
                if val is not None and isinstance(val, dict):
                    val = utils.getBaseEntity(val)
                if isinstance(val, dict):
                    body = {"doc": val}
                else:
                    body = {'doc': {attrName: val}}
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

    def generateRequests_New(self, entry, headerInfo):
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
                fieldDefault = self.config['shotgunFieldTypeDefaults'].get(fieldType, None)

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

    def generateRequests_Retirement(self, entry, headerInfo):
        requests = []
        header = {'delete': headerInfo}
        requests.extend([header])
        return requests

    def generateRequests_Revival(self, entry, headerInfo):
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
            entityConfig.type.lower(): typeMappings,
        }

        return result
