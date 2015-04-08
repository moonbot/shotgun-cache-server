import os
import logging
import zmq
import math
import traceback
import datetime
import time
import multiprocessing

__all__ = [
    'InitialImportManager',
    'InitialImportWorker',
]

LOG = logging.getLogger(__name__)


class InitialImportManager(object):
    def __init__(self, controller, config):
        super(InitialImportManager, self).__init__()
        self.controller = controller
        self.config = config

        self.workID = 0
        self.activeWorkItemsPerType = {}
        self.countsPerType = {}
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

            configType = work['work']['configType']

            # Update the count of active work items
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

    def createPostSocket(self):
        workPostContext = zmq.Context()
        self.workPostSocket = workPostContext.socket(zmq.PUSH)
        self.workPostSocket.bind(self.config['initialImport.zmqPullUrl'])

    def createPullSocket(self):
        workPullSocket = zmq.Context()
        self.workPullSocket = workPullSocket.socket(zmq.PULL)
        self.workPullSocket.bind(self.config['initialImport.zmqPostUrl'])

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
        numProcesses = self.config['initialImport.processes']
        for n in range(numProcesses):
            importer = InitialImportWorker(
                config=self.config,
                entityConfigs=entityConfigs,
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
            work = {
                'type': 'getEntities',
                'id': self.workID,
                'page': page+1,
                'configType': entityType
            }
            self.workPostSocket.send_pyobj(work)
            self.activeWorkItemsPerType[entityType].append(self.workID)
            self.workID += 1

    def handle_entitiesImported(self, work):
        entities = work['data']['entities']
        entityType = work['work']['configType']
        pageCount = self.countsPerType[entityType]['pageCount']

        self.countsPerType[entityType].setdefault('importCount', 0)
        self.countsPerType[entityType]['importCount'] += len(entities)
        LOG.info("Imported {currCount}/{totalCount} entities for type '{typ}' on page {page}/{pageCount}".format(
            currCount=self.countsPerType[entityType]['importCount'],
            totalCount=self.countsPerType[entityType]['entityCount'],
            typ=entityType,
            page=work['work']['page'],
            pageCount=pageCount,
        ))

        entityConfig = self.controller.entityConfigManager.getConfigForType(entityType)
        self.controller.post_entities(entityConfig, entities)

        # Store the timestamp for the initial import
        # We'll use this to discard old eventlogentities that happened before the import
        # However, eventlogentry's that are created while importing will still be applied
        timestamps = self.importTimestampsPerType.setdefault(entityType, {})
        timestamps.setdefault('startImportTimestamp', work['data']['startImportTimestamp'])

        if not len(self.activeWorkItemsPerType[entityType]):
            LOG.info("Imported all entities for type '{0}'".format(entityType))

            self.controller.history.setdefault('configHashes', {})[entityType] = entityConfig.hash
            self.controller.history.setdefault('cachedEntityTypes', {})[entityType] = self.importTimestampsPerType[entityType]
            self.controller.history.save()

            self.activeWorkItemsPerType.pop(entityType)

    def post_countWork(self, entityConfigs, workSocket):
        """
        Send work items to the import processes to load information
        about the counts of the entities
        """
        for config in entityConfigs:
            work = {'type': 'getPageCount', 'id': self.workID, 'configType': config.type}
            self.activeWorkItemsPerType.setdefault(config.type, []).append(self.workID)
            workSocket.send_pyobj(work)
            self.workID += 1

            # We clear the old index out, before rebuilding the data
            # All data should be stored in shotgun, so we will never lose data
            if self.controller.elastic.indices.exists(index=config['index']):
                LOG.debug("Deleting existing elastic index: {0}".format(config['index']))
                self.controller.elastic.indices.delete(index=config['index'])

    def post_stat(self, totalImportTime, entityConfigs):
        """
        Post related stats about the import process to elastic to provide analytics.
        These are posted based on the overall importEntities process, not individual imports.
        """
        stat = {
            'type': 'import_entities',
            'types_imported_count': len(entityConfigs),
            'entity_types': [c.type for c in entityConfigs],
            'total_entities_imported': self.totalEntitiesImported,
            'entity_details': dict([(t, c) for t, c in self.countsPerType.items()]),
            'duration': round(totalImportTime, 3),
            'created_at': datetime.datetime.utcnow().isoformat(),
            'processes': self.config['initialImport.processes'],
            'batch_size': self.config['initialImport.batchSize'],
            'import_failed': self.importFailed,
            # Summarize and page counts shotgun calls
            'total_shotgun_calls': sum([c['pageCount'] for c in self.countsPerType.values()]) + len(entityConfigs),
        }
        self.controller.post_stat(stat)


class InitialImportWorker(object):
    def __init__(self, config, entityConfigs):
        super(InitialImportWorker, self).__init__()
        self.config = config
        self.entityConfigs = dict([(c.type, c) for c in entityConfigs])

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
        self.workPostSocket.connect(self.config['initialImport.zmqPostUrl'])

    def createPullSocket(self):
        self.workPullContext = zmq.Context()
        self.workPullContext = self.workPullContext.socket(zmq.PULL)
        self.workPullContext.connect(self.config['initialImport.zmqPullUrl'])

    def run(self):
        LOG.debug("Running Entity Import Loop")
        # TODO
        # Maybe add a signal catch to shutdown nicely
        # Currently exited using terminate

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

    def handle_getPageCount(self, work):
        LOG.debug("Getting counts for type '{0}' on process {1}".format(work['configType'], os.getpid()))
        entityConfig = self.entityConfigs[work['configType']]
        entityCount = self.getEntityCount(entityConfig)
        pageCount = int(math.ceil(entityCount / float(self.config['initialImport.batchSize'])))
        lastPageDiff = (pageCount * self.config['initialImport.batchSize']) - entityCount
        result = {
            'type': 'counts',
            'data': {
                'entityCount': entityCount,
                'pageCount': pageCount,
                'lastPageDiff': lastPageDiff,
            },
            'work': work,
        }
        self.workPostSocket.send_pyobj(result)

    def handle_getEntities(self, work):
        LOG.debug("Importing Entities for type '{0}' on page {1} on process {2}".format(work['configType'], work['page'], os.getpid()))
        startImportTimestamp = datetime.datetime.utcnow().isoformat()
        entityConfig = self.entityConfigs[work['configType']]
        entities = self.getEntities(entityConfig, work['page'])
        result = {
            'type': 'entitiesImported',
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
                filters=entityConfig.get('filters', []),
                filter_operator=entityConfig.get('filterOperator', 'all'),
                order=[{'column': 'id', 'direction': 'asc'}],
                limit=self.config['initialImport.batchSize'],
                page=page
            )
            result = self.sg.find(**kwargs)
        except Exception:
            LOG.exception("Type: {entity_type}, filters: {filters}, fields: {fields} filterOperator: {filter_operator}".format(**kwargs))
            raise

        return result

    def getEntityCount(self, entityConfig):
        result = self.sg.summarize(
            entity_type=entityConfig.type,
            filters=entityConfig.get('filters', []),
            filter_operator=entityConfig.get('filterOperator', 'all'),
            summary_fields=[{'field': 'id', 'type': 'count'}],
        )
        return result['summaries']['id']
