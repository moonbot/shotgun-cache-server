import os
import zmq
import multiprocessing
import yaml
import time

import logging

import mbotenv  # For paths, TODO

import entityConfig
# import dbWorkerManager
import entityImporter
import monitor
import utils

LOG = logging.getLogger(__name__)
LOG.level = 10

# FUTURE
# Create a utility to diff the cache with shotgun
# Create a utility that can signal a rebuild of
#   specific entity types while the controller is running

class DatabaseController(object):
    def __init__(self, config=None):
        super(DatabaseController, self).__init__()
        self.config = config
        if self.config is None:
            self.config = self.read_config()

        self._load_history()
        self._init_shotgunConnector()
        self._init_entityConfigManager()
        # self._init_db_controller()
        self._init_monitor()

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

        self.history = result

    def read_config(self):
        return yaml.load(open('config.yaml', 'r').read())

    def _init_entityConfigManager(self):
        config = self.config['entityConfigManager']
        self.entityConfigManager = entityConfig.EntityConfigManager(
            enableStats=self.config['enableStats'],
            previousHashes=self.history.get('configHashes', {}),
            shotgunConnector=self.shotgunConnector,
            **config
        )

    def _init_monitor(self):
        config = self.config['monitor']
        self.monitorContext = zmq.Context()
        self.monitorSocket = self.monitorContext.socket(zmq.PULL)
        self.monitorSocket.bind(config['zmqPostUrl'])

        self.monitor = monitor.ShotgunEventMonitor(
            enableStats=self.config['enableStats'],
            latestEventID=self.history.get('latestEventID', None),
            shotgunConnector=self.shotgunConnector,
            **config
        )
        self.monitorProcess = multiprocessing.Process(target=self.monitor.start)
        self.monitorProcess.daemon = True

    # def _init_db_controller(self):
    #     self.dbWorkerManager = dbWorkerManager.DBWorkerManager(
    #         shotgun=self.config['shotgun'],
    #         indexNameTemplate=self.config['indexNameTemplate'],
    #         elasticSettings=self.config['elastic'],
    #     )

    def start(self):
        self.entityConfigManager.load()
        self.monitor.setEntityTypes(self.entityConfigManager.getEntityTypes())
        self.monitorProcess.start()
        # self.dbWorkerManager.start()
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

        self.importEntities(configsToImport)

    def importEntities(self, entityConfigs):
        # indexTemplate = self.config['indexNameTemplate']
        LOG.debug("Importing {0} entity types".format(len(entityConfigs)))

        importConfig = self.config['entityImport']

        importPostContext = zmq.Context()
        importPostSocket = importPostContext.socket(zmq.PUSH)
        importPostSocket.bind(importConfig['zmqPullUrl'])

        importPullContext = zmq.Context()
        importPullSocket = importPullContext.socket(zmq.PULL)
        importPullSocket.bind(importConfig['zmqPostUrl'])

        # Tried using multiprocessing.Pool
        # but had better luck with Processes directly
        # due to using the importer class and instance methods
        processes = []
        numProcesses = importConfig['processes']
        for n in range(numProcesses):
            importer = entityImporter.EntityImporter(
                shotgunConnector=self.shotgunConnector,
                zmqPullUrl=importConfig['zmqPullUrl'],
                zmqPostUrl=importConfig['zmqPostUrl'],
                entityConfigs=entityConfigs,
                batchSize=importConfig['batchSize'],
            )
            proc = multiprocessing.Process(target=importer.start)
            proc.start()
            processes.append(proc)
            LOG.debug("Launched process {0}/{1}: {2}".format(n + 1, numProcesses, proc.pid))

        # Give time for all the workers to connect
        time.sleep(1)

        workID = 0
        activeWorkItemsPerType = {}
        for config in entityConfigs:
            work = {'type': 'getPageCount', 'id': workID, 'configType': config.type}
            activeWorkItemsPerType.setdefault(config.type, []).append(workID)
            importPostSocket.send_pyobj(work)
            workID += 1
            # TODO
            # Send command to database to delete the current index
            # index = self.config['indexNameTemplate'].format(type=c.type)
            # self.elasticSocket.send_pyobj(('deleteIndex', {'index': index}))

        importFailed = False
        countsPerType = {}
        while True:
            result = importPullSocket.recv_pyobj()
            configType = result['work']['configType']
            activeWorkItems = activeWorkItemsPerType[configType]
            workID = result['work']['id']
            activeWorkItems.remove(workID)

            if result['type'] == 'exception':
                LOG.error("Import Failed for type '{type}'.\n{tb}".format(
                    type=result['work']['configType'],
                    tb=result['data']['traceback']
                ))
                workID = result['work']['id']
                importFailed = True

            elif result['type'] == 'counts':
                counts = result['data']
                countsPerType[configType] = counts

                for page in range(counts['pageCount']):
                    work = {
                        'type': 'getEntities',
                        'id': workID,
                        'page': page,
                        'configType': result['work']['configType']
                    }
                    importPostSocket.send_pyobj(work)
                    activeWorkItems.append(workID)
                    workID += 1

            elif result['type'] == 'entitiesImported':
                entities = result['data']['entities']
                page = result['work']['page']
                LOG.info("Imported {currCount}/{totalCount} entities for type '{typ}' on page {page}/{pageCount}".format(
                    currCount=page * importConfig['batchSize'] + len(entities),
                    totalCount=countsPerType[configType]['entityCount'],
                    typ=configType,
                    page=result['work']['page'],
                    pageCount=countsPerType[configType]['pageCount'],
                ))

                if not len(activeWorkItems):
                    LOG.info("Imported all entities for type '{0}'".format(configType))

                    entityConfig = self.entityConfigManager.getConfigForEntity(configType)
                    self.history['configHashes'][configType] = entityConfig.hash
                    self.history.setdefault('loaded', []).append(configType)
                    self.writeHistoryToDisk()

                    activeWorkItemsPerType.pop(configType)
            else:
                raise ValueError("Unkown result type from importer: {0}".format(result['type']))

            if not len(activeWorkItemsPerType):
                break

        for proc in processes:
            LOG.debug("Terminating import process: {0}".format(proc.pid))
            proc.terminate()

        LOG.debug("Import finished")
        if importFailed:
            raise IOError("Import Process failed for one ore more entities, check log for details")

    def postImportedEntity(self, entity):
        pass
        # LOG.debug("Posting imported entity: {0}:{1}".format(entity['type'], entity['id']))
        # TODO
        # Potential memory issue loading all entities at once, may need to be paged

    def reimportEntities(self, entityTypes):
        LOG.info("Reimporting entities for types: {0}".format(', '.join(entityTypes)))
        entityConfigs = [self.entityConfigManager.getConfigForEntity(t) for t in entityTypes]
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

            if work['type'] == 'latestEventID':
                self.updateLatestEventID(work['data']['eventID'])
            elif work['type'] == 'reimportEntities':
                # TODO
                # Create utility and test this
                self.reimportEntity(work['data']['entityTypes'])
            elif work['type'] == 'entityUpdate':
                # TODO
                pass
            elif work['type'] == 'stat':
                if self.config['enableStats']:
                    pass
            else:
                raise TypeError("Unkown work type: {0}".format(work['type']))

            LOG.debug("Work: {0}".format(work))

            LOG.debug("Posting new event")
            # self.dbWorkerManager.postWork(work)

    def updateLatestEventID(self, eventID):
        self.history['latestEventID'] = eventID
        self.writeHistoryToDisk()

    def writeHistoryToDisk(self):
        with open(self.historyPath, 'w') as f:
            yaml.dump(self.history, f, default_flow_style=False, indent=4)


if __name__ == '__main__':
    logging.basicConfig()
    controller = DatabaseController()
    controller.start()
