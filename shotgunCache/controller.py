import os
import zmq
import multiprocessing
import yaml
import time
import itertools
import re
import datetime
import json

import logging

import mbotenv  # For paths, TODO

import entityConfig
import elasticsearch
import entityImporter
import monitor
import utils

__all__ = [
    'DatabaseController',
]


LOG = logging.getLogger(__name__)
LOG.level = 10

# FUTURE
# Create a utility to diff the cache with shotgun
# Create a utility that can signal a rebuild of
#   specific entity types while the controller is running


class DatabaseController(object):
    def __init__(self, configPath=None):
        super(DatabaseController, self).__init__()
        self.config = self.read_config(configPath)

        self._load_history()
        self._init_shotgunConnector()
        self._init_entityConfigManager()
        self._init_elastic()
        self._init_monitor()
        self._init_entityImportManager()

        self.sg = self.shotgunConnector.getInstance()

    def _init_shotgunConnector(self):
        connector = utils.ShotgunConnector(
            self.config['shotgun']
        )
        self.shotgunConnector = connector

    def _load_history(self):
        result = {}
        historyPath = self.config['historyFile']
        historyPath = os.path.expanduser(historyPath)
        historyPath = os.path.abspath(historyPath)
        self.historyPath = historyPath

        if os.path.exists(historyPath):
            with open(historyPath, 'r') as f:
                result = yaml.load(f)
                if result is None:
                    result = {}

        self.history = result

    def read_config(self, path):
        result = yaml.load(open(path, 'r').read())
        return result

    def _init_entityConfigManager(self):
        self.entityConfigManager = entityConfig.EntityConfigManager(
            configFolder=self.config['entityConfigFolder'],
            previousHashes=self.history.get('configHashes', {}),
            shotgunConnector=self.shotgunConnector,
        )

    def _init_monitor(self):
        self.monitorContext = zmq.Context()
        self.monitorSocket = self.monitorContext.socket(zmq.PULL)
        self.monitorSocket.bind(self.config['zmqListenUrl'])

        self.monitor = monitor.ShotgunEventMonitor(
            latestEventLogEntry=self.history.get('latestEventLogEntry', None),
            shotgunConnector=self.shotgunConnector,
            zmqPostUrl=self.config['zmqListenUrl'],
            **self.config['monitor']
        )
        self.monitorProcess = multiprocessing.Process(target=self.monitor.start)
        self.monitorProcess.daemon = True

    def _init_entityImportManager(self):
        importConfig = self.config['importConfig']
        self.entityImportManager = entityImporter.EntityImportManager(
            controller=self,
            zmqPullUrl=importConfig['zmqPullUrl'],
            zmqPostUrl=importConfig['zmqPostUrl'],
            processes=importConfig['processes'],
            batchSize=importConfig['batchSize'],
        )

    def _init_elastic(self):
        self.elastic = elasticsearch.Elasticsearch(**self.config['elasticsearch_connection'])

    def start(self):
        self.entityConfigManager.load()
        if not len(self.entityConfigManager.configs):
            raise IOError("No entity configs found to cache in {0}".format(self.config['entityConfigFolder']))

        self.monitor.setEntityTypes(self.entityConfigManager.getEntityTypes())
        self.monitorProcess.start()
        self.importChangedEntities()
        self.run()

    def getOutOfDateConfigs(self):
        result = []
        for c in self.entityConfigManager.configs.values():
            if c.needsUpdate() or c.type not in self.history.get('loaded', []):
                result.append(c)
                LOG.info("{0} Config Updated".format(c.type))
        return result

    def importChangedEntities(self):
        LOG.debug("Importing Changed Entities")

        configsToImport = self.getOutOfDateConfigs()
        if not len(configsToImport):
            LOG.info("No entity imports required")
            return

        self.entityImportManager.importEntities(configsToImport)

    def postStatToElastic(self, statDict):
        if not self.config['enableStats']:
            return

        LOG.debug("Posting stat: {0}".format(statDict['type']))
        statIndex = self.config['elasticStatIndexTemplate'].format(type=statDict['type'])
        if not self.elastic.indices.exists(index=statIndex):
            # TODO
            self.elastic.indices.create(index=statIndex, body={})
        self.elastic.index(index=statIndex, doc_type=statDict['type'], body=statDict)

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

    def postEntitiesToElastic(self, entityConfig, entities):
        requests = []

        # TODO (bchapman) Could remove a few calls to elastic by caching the exists information
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

    def postEventLogEntriesToElastic(self, eventLogEntries):

        print '-- eventLogEntries --'
        print json.dumps(eventLogEntries, sort_keys=True, indent=4, separators=(',', ': '))
        print

        requests = []
        for entry in eventLogEntries:
            entityType, changeType = entry['event_type'].split('_', 3)[1:]
            entityConfig = self.entityConfigManager.getConfigForType(entityType)

            meta = entry['meta']
            # TODO
            # Is this correct?
            if entry['entity']:
                _id = entry['entity']['id']
            else:
                _id = meta['entity_id']

            headerInfo = {
                '_index': entityConfig['index'],
                '_type': entityType.lower(),
                '_id': _id,
            }

            if changeType == 'Change':
                header = {'update': headerInfo}

                attrName = entry['attribute_name']
                if attrName not in entityConfig['fields']:
                    # TODO
                    # Should this be added the filters?
                    LOG.debug("Untracked field updated: {0}".format(attrName))
                    continue

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
                            body = {
                                "doc": val
                            }
                        else:
                            body = {
                                'doc': {
                                    attrName: val
                                }
                            }
                else:
                    val = meta['new_value']
                    body = {
                        "doc": {
                            attrName: val
                        }
                    }

                requests.extend([header, body])

                if 'updated_at' in entityConfig['fields']:
                    body = {
                        "doc": {
                            'updated_at': entry['created_at'],
                        }
                    }
                    requests.extend([header, body])

                if 'updated_at' in entityConfig['fields']:
                    body = {
                        "doc": {
                            'updated_by': utils.getBaseEntity(entry['user']),
                        }
                    }
                    requests.extend([header, body])


            elif changeType == 'New':
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

                # TODO
                # How to handle date updated / created fields

                print("new header: {0}".format(header)) # TESTING
                print("new body: {0}".format(body)) # TESTING
                requests.extend([header, body])

            elif changeType == 'Retirement':
                header = {'delete': headerInfo}
                requests.extend([header])

                if 'updated_at' in entityConfig['fields']:
                    body = {
                        "doc": {
                            'updated_at': entry['created_at'],
                        }
                    }
                    requests.extend([header, body])

                if 'updated_at' in entityConfig['fields']:
                    body = {
                        "doc": {
                            'updated_by': utils.getBaseEntity(entry['user']),
                        }
                    }
                    requests.extend([header, body])

            elif changeType == 'Revival':
                # This is one of the few times
                # we have to go and retrieve the information from shotgun
                header = {'index': headerInfo}
                filters = [['id', 'is', entry['entity']['id']]]
                filters.extend(entityConfig.get('filters', []))
                # TODO
                # I could batch these for multiple revives per batch...
                body = self.sg.find_one(entityType, filters, entityConfig['fields'].keys())
                requests.extend([header, body])

            else:
                raise TypeError("Unkown change type: {0}".format(changeType))

        print '-- requests --'
        print json.dumps(requests, sort_keys=True, indent=4, separators=(',', ': '))
        print


        responses = self.elastic.bulk(body=requests)
        submitTime = datetime.datetime.utcnow()

        print '-- responses --'
        print json.dumps(responses, sort_keys=True, indent=4, separators=(',', ': '))
        print

        if responses['errors']:
            for response in responses['items']:
                for responseEntry in response.values():
                    if responseEntry.get('error', None):
                        LOG.error(responseEntry['error'])
            raise IOError("Errors occurred creating entities")

        if self.config['enableStats']:
            # Post the min/max/avg delay in milliseconds
            delays = []
            for entry in eventLogEntries:
                createdTime = entry['created_at']
                createdTime = datetime.datetime(*map(int, re.split('[^\d]', createdTime)[:-1]))
                diff = submitTime - createdTime
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
                'created_at': submitTime.isoformat(),
            }
            self.postStatToElastic(stat)

    def reimportEntities(self, entityTypes):
        LOG.info("Reimporting entities for types: {0}".format(', '.join(entityTypes)))
        entityConfigs = [self.entityConfigManager.getConfigForType(t) for t in entityTypes]
        self.importEntities(entityConfigs)

    def run(self):
        LOG.debug("Starting Main Event Loop")
        while True:
            # TODO
            # Exception handling
            # Work is a tuple of (workType, data)
            work = self.monitorSocket.recv_pyobj()

            if work is None:
                continue

            if not isinstance(work, dict):
                raise TypeError("Invalid work item, expected dict: {0}".format(work))

            if work['type'] == 'latestEventLogEntry':
                self.updatelatestEventLogEntry(work['data']['entity'])
            elif work['type'] == 'reimportEntities':
                # TODO
                # Create utility and test this
                self.reimportEntity(work['data']['entityTypes'])
            elif work['type'] == 'eventLogEntries':
                self.postEventLogEntriesToElastic(work['data']['entities'])
            elif work['type'] == 'stat':
                self.postStatToElastic(work['data'])
            else:
                raise TypeError("Unkown work type: {0}".format(work['type']))

            LOG.debug("Work: {0}".format(work))

            # LOG.debug("Posting new event")
            # self.dbWorkerManager.postWork(work)

    def updatelatestEventLogEntry(self, entity):
        self.history['latestEventLogEntry'] = entity
        self.writeHistoryToDisk()

    def writeHistoryToDisk(self):
        with open(self.historyPath, 'w') as f:
            yaml.dump(self.history, f, default_flow_style=False, indent=4)
