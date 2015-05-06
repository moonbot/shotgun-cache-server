import os
import logging
import zmq
import math
import traceback
import datetime
import time
import multiprocessing

import rethinkdb

from main import CONFIG_PATH_ENV_KEY
import utils

__all__ = [
    'ImportManager',
    'ImportWorker',
]

LOG = logging.getLogger(__name__)


class ImportManager(object):
    def __init__(self, controller, config):
        super(ImportManager, self).__init__()
        self.controller = controller
        self.config = config

        self.workID = 0
        self.activeWorkItemsPerType = {}
        self.countsPerType = {}
        self.idsPerType = {}
        self.workPullSocket = None
        self.workPostSocket = None
        self.totalEntitiesImported = 0

    def importEntities(self, entityConfigs):
        """
        Batch import entities from shotgun into the local shotgun cache
        Uses multiple processes to speed up retrieval
        """
        LOG.debug("Importing {0} entity types".format(len(entityConfigs)))
        importStartTime = time.time()

        # Reset
        self.workID = 0
        self.activeWorkItemsPerType = {}
        self.countsPerType = {}
        self.idsPerType = {}
        self.importFailed = False
        self.totalEntitiesImported = 0
        self.importTimestampsPerType = {}

        self.createPostSocket()
        self.createPullSocket()

        processes = self.launchImportProcesses(entityConfigs)

        self.post_countWork(entityConfigs, self.workPostSocket)

        while True:
            work = self.workPullSocket.recv_pyobj()

            if not isinstance(work, dict):
                raise TypeError("Invalid work item, expected dict: {0}".format(work))

            # Update the count of active work items
            configType = work['work']['configType']
            activeWorkItems = self.activeWorkItemsPerType[configType]
            workID = work['work']['id']
            activeWorkItems.remove(workID)

            meth_name = 'handle_{0}'.format(work['type'])
            if hasattr(self, meth_name):
                getattr(self, meth_name)(work)
            else:
                raise ValueError("Unhandled work type: {0}".format(work['type']))

            if not len(self.activeWorkItemsPerType):
                break

        for proc in processes:
            LOG.debug("Terminating import worker process: {0}".format(proc.pid))
            proc.terminate()

        timeToImport = (time.time() - importStartTime) * 1000  # ms
        self.totalEntitiesImported = sum([c['importCount'] for c in self.countsPerType.values()])
        self.post_stat(timeToImport, entityConfigs)

        if self.importFailed:
            raise IOError("Import Process failed for one ore more entities, check log for details")

        LOG.debug("Imported {0} entities".format(self.totalEntitiesImported))

    def finalize_import_for_entity_type(self, entityType):
        LOG.info("Imported all entities for type '{0}'".format(entityType))
        entityConfig = self.controller.entityConfigManager.getConfigForType(entityType)

        # Get a list of
        cachedEntityIDs = set(rethinkdb
            .table(entityConfig['table'])
            .map(lambda asset: asset['id'])
            .coerce_to('array')
            .run(self.controller.rethink)
        )
        importedEntityIDs = set(self.idsPerType[entityType])
        diffIDs = cachedEntityIDs.difference(importedEntityIDs)

        if len(diffIDs):
            # Delete these extra entities
            # This allows us to update the cache in place without
            # having the drop the table before the import, allowing for
            # a more seamless import / update process
            LOG.info("Deleting extra entities found in cache with IDs: {0}".format(diffIDs))
            rethinkdb.db('shotguncache').table(entityConfig['table']).get_all(rethinkdb.args(diffIDs)).delete().run(self.controller.rethink)

        self.config.history.setdefault('config_hashes', {})[entityType] = entityConfig.hash
        self.config.history.setdefault('cached_entity_types', {})[entityType] = self.importTimestampsPerType[entityType]
        self.config.history.save()

        self.activeWorkItemsPerType.pop(entityType)

    def createPostSocket(self):
        workPostContext = zmq.Context()
        self.workPostSocket = workPostContext.socket(zmq.PUSH)
        self.workPostSocket.bind(self.config['import.zmq_pull_url'])

    def createPullSocket(self):
        workPullSocket = zmq.Context()
        self.workPullSocket = workPullSocket.socket(zmq.PULL)
        self.workPullSocket.bind(self.config['import.zmq_post_url'])

    def launchImportProcesses(self, entityConfigs):
        """
        Use multiprocessing to start a pool of entity import processes
        Each of these use zmq as a message queue for work items which retrieve
        information from shotgun.
        """
        # Tried using multiprocessing.Pool
        # but had better luck with Processes directly
        # due to using the importer class and instance methods
        processes = []
        numProcesses = self.config['import.processes']
        for n in range(numProcesses):
            importer = ImportWorker(
                config=self.config,
                entityConfigs=entityConfigs,
                schema=self.controller.entityConfigManager.schema,
            )
            proc = multiprocessing.Process(target=importer.start)
            proc.start()
            processes.append(proc)
            LOG.debug("Launched process {0}/{1}: {2}".format(n + 1, numProcesses, proc.pid))

        # Give time for all the workers to connect
        time.sleep(1)
        return processes

    def handle_exception(self, work):
        entityType = work['work']['configType']
        LOG.error("Import Failed for type '{type}'.\n{tb}".format(
            type=entityType,
            tb=work['data']['traceback']
        ))
        self.importFailed = True

    def handle_counts(self, work):
        counts = work['data']
        entityType = work['work']['configType']
        self.countsPerType[entityType] = counts

        for page in range(counts['pageCount']):
            getEntitiesWork = {
                'type': 'loadEntities',
                'id': self.workID,
                'page': page + 1,
                'configType': entityType
            }
            self.workPostSocket.send_pyobj(getEntitiesWork)
            self.activeWorkItemsPerType[entityType].append(self.workID)
            self.workID += 1

    def handle_loadedEntities(self, work):
        entityType = work['work']['configType']

        prepareEntitiesWork = {
            'type': 'prepareEntities',
            'id': self.workID,
            'page': work['work']['page'],
            'configType': entityType,
            'data': work['data']
        }
        self.workPostSocket.send_pyobj(prepareEntitiesWork)
        self.activeWorkItemsPerType[entityType].append(self.workID)
        self.workID += 1

        # Store the timestamp for the import
        # We'll use this to discard old EventLogEntities that happened before the import
        # However, eventlogentry's that are created while importing will still be applied
        timestamps = self.importTimestampsPerType.setdefault(entityType, {})
        timestamps.setdefault('startImportTimestamp', work['data']['startImportTimestamp'])

    def handle_downloadedFile(self, work):
        entityType = work['work']['configType']

        # TODO
        # Merge in download path into entity

        if not len(self.activeWorkItemsPerType[entityType]):
            self.finalize_import_for_entity_type(entityType)

    def handle_preparedEntities(self, work):
        entities = work['data']['entities']
        entityType = work['work']['configType']
        pageCount = self.countsPerType[entityType]['pageCount']

        self.countsPerType[entityType].setdefault('importCount', 0)
        self.countsPerType[entityType]['importCount'] += len(entities)
        self.idsPerType.setdefault(entityType, []).extend([e['id'] for e in entities])
        LOG.info("Imported {currCount}/{totalCount} entities for type '{typ}' on page {page}/{pageCount}".format(
            currCount=self.countsPerType[entityType]['importCount'],
            totalCount=self.countsPerType[entityType]['entityCount'],
            typ=entityType,
            page=work['work']['page'],
            pageCount=pageCount,
        ))

        itemsToDownload = work['data']['itemsToDownload']
        if len(work['data']['itemsToDownload']):
            for item in itemsToDownload:
                work = {
                    'type': 'downloadFile',
                    'data': item,
                    'id': self.workID,
                    'configType': entityType,
                }
                self.workPostSocket.send_pyobj(work)
                self.activeWorkItemsPerType[entityType].append(self.workID)
                self.workID += 1

        entityConfig = self.controller.entityConfigManager.getConfigForType(entityType)
        self.controller.post_entities(entityConfig, entities)

        if not len(self.activeWorkItemsPerType[entityType]):
            self.finalize_import_for_entity_type(entityType)

    def post_countWork(self, entityConfigs, workSocket):
        """
        Send work items to the import processes to load information
        about the counts of the entities
        """
        for config in entityConfigs:
            work = {'type': 'getCount', 'id': self.workID, 'configType': config.type}
            self.activeWorkItemsPerType.setdefault(config.type, []).append(self.workID)
            workSocket.send_pyobj(work)
            self.workID += 1

            self.post_entityConfig(config)

    def post_entityConfig(self, entityConfig):
        LOG.debug("Posting entity config")

        schemaTable = self.config['rethink_schema_table']

        if schemaTable not in rethinkdb.table_list().run(self.controller.rethink):
            LOG.debug("Creating table for schema: {0}".format(entityConfig.type))
            rethinkdb.table_create(schemaTable, primary_key='type').run(self.controller.rethink)

        entitySchema = self.controller.entityConfigManager.schema[entityConfig.type]
        cacheSchema = dict([(field, s) for field, s in entitySchema.items() if field in entityConfig['fields']])

        if LOG.getEffectiveLevel() < 10:
            LOG.debug("Cache Schema:\n{0}".format(utils.prettyJson(cacheSchema)))

        config = {}
        config['type'] = entityConfig.type
        config['schema'] = cacheSchema
        config['created_at'] = datetime.datetime.utcnow().isoformat()

        result = rethinkdb.table(schemaTable).insert(config, conflict="replace").run(self.controller.rethink)
        if result['errors']:
            raise IOError(result['first_error'])

    def post_stat(self, totalImportTime, entityConfigs):
        """
        Post related stats about the import process to the db to provide analytics.
        These are posted based on the overall importEntities process, not individual imports.
        """
        stat = {
            'type': 'import_entities',
            'types_imported_count': len(entityConfigs),
            'entity_types': [c.type for c in entityConfigs],
            'total_entities_imported': self.totalEntitiesImported,
            'duration': round(totalImportTime, 3),
            'created_at': datetime.datetime.utcnow().isoformat(),
            'processes': self.config['import.processes'],
            'batch_size': self.config['import.batch_size'],
            'import_failed': self.importFailed,
            # Summarize and page counts shotgun calls
            'total_shotgun_calls': sum([c['pageCount'] for c in self.countsPerType.values()]) + len(entityConfigs),
        }
        self.controller.post_stat(stat)


class ImportWorker(object):
    def __init__(self, config, entityConfigs, schema):
        super(ImportWorker, self).__init__()
        self.config = config
        self.entityConfigs = dict([(c.type, c) for c in entityConfigs])
        self.schema = schema

        self.sg = None
        self.incomingContext = None
        self.workPullContext = None
        self.workPostContext = None
        self.workPostSocket = None

    def start(self):
        self.sg = self.config.createShotgunConnection()
        self.createPullSocket()
        self.createPostSocket()
        self.run()

    def createPostSocket(self):
        self.workPostContext = zmq.Context()
        self.workPostSocket = self.workPostContext.socket(zmq.PUSH)
        self.workPostSocket.connect(self.config['import.zmq_post_url'])

    def createPullSocket(self):
        self.workPullContext = zmq.Context()
        self.workPullContext = self.workPullContext.socket(zmq.PULL)
        self.workPullContext.connect(self.config['import.zmq_pull_url'])

    def run(self):
        LOG.debug("Running Entity Import Loop")

        while True:
            work = self.workPullContext.recv_pyobj()

            if not isinstance(work, dict):
                raise TypeError("Invalid work item, expected dict: {0}".format(work))

            meth_name = 'handle_{0}'.format(work['type'])
            if hasattr(self, meth_name):
                try:
                    getattr(self, meth_name)(work)
                except Exception, e:
                    result = {
                        'type': 'exception',
                        'data': {
                            'exc': e,
                            'traceback': traceback.format_exc()
                        },
                        'work': work
                    }
                    self.workPostSocket.send_pyobj(result)
            else:
                raise ValueError("Unhandled work type: {0}".format(work['type']))

    def handle_getCount(self, work):
        LOG.debug("Getting counts for type '{0}' on process {1}".format(work['configType'], os.getpid()))
        entityConfig = self.entityConfigs[work['configType']]
        entityCount = self.getEntityCount(entityConfig)
        pageCount = int(math.ceil(entityCount / float(self.config['import.batch_size'])))
        result = {
            'type': 'counts',
            'data': {
                'entityCount': entityCount,
                'pageCount': pageCount,
            },
            'work': work,
        }
        self.workPostSocket.send_pyobj(result)

    def handle_downloadFile(self, work):
        url = work['data']['url']
        destFolder = work['data']['destFolder']
        LOG.info("Downloading {0} on process {1}".format(url, os.getpid()))
        st = time.time()
        filename = utils.get_image_filename_from_url_header(url)
        print("filename: {0}".format(filename)) # TESTING
        dest = os.path.join(destFolder, filename)
        dirPath = os.path.dirname(dest)
        print("dirPath: {0}".format(dirPath)) # TESTING
        if not os.path.exists(dirPath):
            os.makedirs(dirPath)
        print("dest: {0}".format(dest)) # TESTING
        utils.download(url, dest)
        total = (time.time() - st) * 1000  # ms
        LOG.debug("Downloaded {0} in {1:0.2f} ms".format(dest, total))

        result = {
            'type': 'downloadedFile',
            'data': {
                'duration': total,
            },
            'work': work,
        }
        self.workPostSocket.send_pyobj(result)

    def handle_prepareEntities(self, work):
        LOG.debug("Preparing Entities for type '{0}' on page {1} on process {2}".format(work['configType'], work['page'], os.getpid()))
        startPrepareTimestamp = datetime.datetime.utcnow().isoformat()

        entityType = work['configType']
        entityConfig = self.entityConfigs[entityType]
        entities = work['data']['entities']

        # # TODO
        # attachmentFields = []
        # for field in entityConfig.get('fields', {}):
        #     fieldSchema = self.schema[entityType].get(field, None)
        #     if not fieldSchema:
        #         continue

        #     print("fieldSchema: {0}".format(fieldSchema)) # TESTING
        #     dataType = fieldSchema.get('data_type', {}).get('value', None)
        #     print("dataType: {0}".format(dataType)) # TESTING
        #     if dataType in ['image']:
        #         attachmentFields.append(field)
        # print("attachmentFields: {0}".format(attachmentFields)) # TESTING
        # # Handle images / attachments

        itemsToDownload = []

        for entity in entities:
            # Get rid of extra data found in sub-entities
            # We don't have a way to reliably keep these up to date except
            # for the type and id
            entitySchema = self.schema[entityConfig.type]
            for field, val in entity.items():
                # Originally was storing dates and times as rethink date and time objects
                # but found this to be much slower than just storing strings
                # and converting to date objects when needed for filtering
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
                elif fieldDataType == 'image' and val is not None:
                    # Extensions are dynamically added on based on data type
                    print("base url: {0}".format(val)) # TESTING
                    subFolder = os.path.join(entityType, str(entity['id']))
                    print("subFolder: {0}".format(subFolder)) # TESTING
                    downloadsPath = self.config['downloads_path']
                    if not os.path.isabs(downloadsPath):
                        downloadsPath = os.path.join(os.environ[CONFIG_PATH_ENV_KEY], downloadsPath)
                    downloadsPath = os.path.abspath(downloadsPath)
                    destFolder = os.path.join(downloadsPath, subFolder)
                    itemsToDownload.append(
                        {
                            'url': val,
                            'destFolder': destFolder,
                            'entityID': entity['id'],
                            'entityType': entityType,
                            'field': field,
                        }
                    )
                    # newURL = os.path.join(self.config['http_url_prefix'], subPath)
                    # print("newURL: {0}".format(newURL)) # TESTING
                    # entity[field] = newURL

        data = work['data']
        data.update({
            'startPrepareTimestamp': startPrepareTimestamp,
            'itemsToDownload': itemsToDownload,
        })
        result = {
            'type': 'preparedEntities',
            'data': data,
            'work': work,
        }
        self.workPostSocket.send_pyobj(result)

    def handle_loadEntities(self, work):
        LOG.debug("Loading Entities for type '{0}' on page {1} on process {2}".format(work['configType'], work['page'], os.getpid()))
        startImportTimestamp = datetime.datetime.utcnow().isoformat()
        entityConfig = self.entityConfigs[work['configType']]
        entities = self.getEntities(entityConfig, work['page'])
        result = {
            'type': 'loadedEntities',
            'data': {
                'entities': entities,
                'startImportTimestamp': startImportTimestamp,
            },
            'work': work,
        }
        self.workPostSocket.send_pyobj(result)

    def getEntities(self, entityConfig, page):
        try:
            kwargs = dict(
                entity_type=entityConfig.type,
                fields=entityConfig.get('fields', {}).keys(),
                filters=[],
                order=[{'column': 'id', 'direction': 'asc'}],
                limit=self.config['import.batch_size'],
                page=page
            )
            result = self.sg.find(**kwargs)
        except Exception:
            LOG.exception("Type: {entity_type}, filters: {filters}, fields: {fields} filterOperator: {filter_operator}".format(**kwargs))
            raise

        return result

    def getEntityCount(self, entityConfig):
        # Switched to find until shotgun bug is fixed
        # that results in summarize returning a different count
        # Stems from Tasks getting created with no project assigned
        # sg.find will still count these, but summarize will not
        result = self.sg.find(
            entity_type=entityConfig.type,
            filters=[],
            fields=['id'],
        )
        return len(result)
