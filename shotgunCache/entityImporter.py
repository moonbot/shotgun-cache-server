import os
import logging
import zmq
import math
import traceback
import datetime
import time
import multiprocessing

__all__ = [
    'EntityImportManager',
    'EntityImporter',
]

LOG = logging.getLogger(__name__)
# LOG.level = 10

SG = None


class EntityImportManager(object):
    def __init__(self, controller, zmqPullUrl, zmqPostUrl, processes, batchSize):
        super(EntityImportManager).__init__()
        self.controller = controller
        self.zmqPullUrl = zmqPullUrl
        self.zmqPostUrl = zmqPostUrl
        self.processes = processes
        self.batchSize = batchSize

        self.workID = 0
        self.activeWorkItemsPerType = {}

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
        numProcesses = self.processes
        for n in range(numProcesses):
            importer = EntityImporter(
                shotgunConnector=self.shotgunConnector,
                zmqPullUrl=self.zmqPullUrl,
                zmqPostUrl=self.zmqPostUrl,
                entityConfigs=entityConfigs,
                batchSize=self.batchSize,
            )
            proc = multiprocessing.Process(target=importer.start)
            proc.start()
            processes.append(proc)
            LOG.debug("Launched process {0}/{1}: {2}".format(n + 1, numProcesses, proc.pid))

        # Give time for all the workers to connect
        time.sleep(1)
        return processes

    def postCountWork(self, entityConfigs, workSocket):
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

    def importEntities(self, entityConfigs):
        # Reset
        self.workID = 0
        self.activeWorkItemsPerType = {}

        # indexTemplate = self.config['indexNameTemplate']
        LOG.debug("Importing {0} entity types".format(len(entityConfigs)))
        importStartTime = time.time()

        importPostContext = zmq.Context()
        importPostSocket = importPostContext.socket(zmq.PUSH)
        importPostSocket.bind(self.zmqPullUrl)

        importPullContext = zmq.Context()
        importPullSocket = importPullContext.socket(zmq.PULL)
        importPullSocket.bind(self.zmqPostUrl)

        processes = self.launchImportProcesses(entityConfigs)

        self.postCountWork(entityConfigs, importPostSocket)

        importFailed = False
        countsPerType = {}
        while True:
            workResult = importPullSocket.recv_pyobj()

            configType = workResult['work']['configType']

            # Update the count of active work items
            activeWorkItems = self.activeWorkItemsPerType[configType]
            workID = workResult['work']['id']
            activeWorkItems.remove(workID)

            if workResult['type'] == 'exception':
                LOG.error("Import Failed for type '{type}'.\n{tb}".format(
                    type=workResult['work']['configType'],
                    tb=workResult['data']['traceback']
                ))
                self.workID = workResult['work']['id']
                importFailed = True

            elif workResult['type'] == 'counts':
                counts = workResult['data']
                countsPerType[configType] = counts

                for page in range(counts['pageCount']):
                    work = {
                        'type': 'getEntities',
                        'id': self.workID,
                        'page': page+1,
                        'configType': workResult['work']['configType']
                    }
                    importPostSocket.send_pyobj(work)
                    activeWorkItems.append(self.workID)
                    self.workID += 1

            elif workResult['type'] == 'entitiesImported':
                entities = workResult['data']['entities']
                page = workResult['work']['page']
                pageCount = countsPerType[configType]['pageCount']

                countsPerType[configType].setdefault('importCount', 0)
                countsPerType[configType]['importCount'] += len(entities)
                # TODO
                # curr count is not right
                LOG.info("Imported {currCount}/{totalCount} entities for type '{typ}' on page {page}/{pageCount}".format(
                    currCount=countsPerType[configType]['importCount'],
                    totalCount=countsPerType[configType]['entityCount'],
                    typ=configType,
                    page=workResult['work']['page'],
                    pageCount=pageCount,
                ))

                entityConfig = self.entityConfigManager.getConfigForType(configType)
                self.controller.postEntitiesToElastic(entityConfig, entities)

                if not len(activeWorkItems):
                    LOG.info("Imported all entities for type '{0}'".format(configType))

                    self.controller.history.setdefault('configHashes', {})[configType] = entityConfig.hash
                    self.controller.history.setdefault('loaded', []).append(configType)
                    self.controller.history['loaded'] = list(set(self.history['loaded']))
                    self.controller.writeHistoryToDisk()

                    self.activeWorkItemsPerType.pop(configType)
            else:
                raise ValueError("Unkown result type from importer: {0}".format(workResult['type']))

            if not len(self.activeWorkItemsPerType):
                break

        for proc in processes:
            LOG.debug("Terminating import process: {0}".format(proc.pid))
            proc.terminate()

        timeToImport = (time.time() - importStartTime) * 1000  # ms
        stat = {
            'type': 'import_entities',
            'no_types_imported': len(entityConfigs),
            'entity_types': [c.type for c in entityConfigs],
            'total_entities_imported': sum([c['entityCount'] for c in countsPerType.values()]),
            'entity_details': dict([(t, c) for t, c in countsPerType.items()]),
            'duration': round(timeToImport, 3),
            'created_at': datetime.datetime.utcnow().isoformat(),
            # Summarize and page counts shotgun calls
            'total_shotgun_calls': sum([c['pageCount'] for c in countsPerType.values()]) + len(entityConfigs),
        }
        self.controller.postStatToElastic(stat)

        LOG.debug("Import finished")
        if importFailed:
            raise IOError("Import Process failed for one ore more entities, check log for details")


class EntityImporter(object):
    def __init__(self, zmqPullUrl, zmqPostUrl, shotgunConnector, entityConfigs, batchSize=250):
        super(EntityImporter, self).__init__()
        self.shotgunConnector = shotgunConnector
        self.zmqPullUrl = zmqPullUrl
        self.zmqPostUrl = zmqPostUrl
        self.batchSize = batchSize
        self.entityConfigs = dict([(c.type, c) for c in entityConfigs])

        self.sg = None
        self.incomingContext = None
        self.incomingSocket = None
        self.outgoingContext = None
        self.outgoingSocket = None

    def start(self):
        self.sg = self.shotgunConnector.getInstance()

        self.incomingContext = zmq.Context()
        self.incomingSocket = self.incomingContext.socket(zmq.PULL)
        self.incomingSocket.connect(self.zmqPullUrl)

        self.outgoingContext = zmq.Context()
        self.outgoingSocket = self.outgoingContext.socket(zmq.PUSH)
        self.outgoingSocket.connect(self.zmqPostUrl)

        self.run()

    def run(self):
        LOG.debug("Running Entity Import Loop")
        # Exited using terminate
        # TODO
        # Maybe add a signal catch to shutdown nicely

        while True:
            work = self.incomingSocket.recv_pyobj()

            try:
                if work['type'] == 'getPageCount':
                    LOG.debug("Getting counts for type '{0}' on process {1}".format(work['configType'], os.getpid()))
                    entityConfig = self.entityConfigs[work['configType']]
                    entityCount = self.getEntityCount(entityConfig)
                    pageCount = int(math.ceil(entityCount / float(self.batchSize)))
                    lastPageDiff = (pageCount * self.batchSize) - entityCount
                    result = {
                        'type': 'counts',
                        'data': {
                            'entityCount': entityCount,
                            'pageCount': pageCount,
                            'lastPageDiff': lastPageDiff,
                        },
                        'work': work,
                    }
                    self.outgoingSocket.send_pyobj(result)

                elif work['type'] == 'getEntities':
                    LOG.debug("Importing Entities for type '{0}' on page {1} on process {2}".format(work['configType'], work['page'], os.getpid()))
                    entityConfig = self.entityConfigs[work['configType']]
                    entities = self.getEntities(entityConfig, work['page'])
                    result = {
                        'type': 'entitiesImported',
                        'data': {
                            'entities': entities,
                        },
                        'work': work,
                    }
                    self.outgoingSocket.send_pyobj(result)

                    # TODO
                    # Handle typeComplete in main controller
                else:
                    raise ValueError("Unknown work type: {0}".format(work['type']))

            except Exception, e:
                result = {
                    'type': 'exception',
                    'data': {
                        'exc': e,
                        'traceback': traceback.format_exc()
                    },
                    'work': work
                }
                self.outgoingSocket.send_pyobj(result)

    def getEntities(self, entityConfig, page):
        try:
            kwargs = dict(
                entity_type=entityConfig.type,
                fields=entityConfig.get('fields', {}).keys(),
                filters=entityConfig.get('filters', []),
                filter_operator=entityConfig.get('filterOperator', 'all'),
                order=[{'column': 'id', 'direction': 'asc'}],
                limit=self.batchSize,
                page=page
            )
            result = self.sg.find(**kwargs)
        except Exception:
            # TODO
            # print command?
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
