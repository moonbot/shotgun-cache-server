import os
import zmq
import multiprocessing
import yaml

import mbotenv

import entityConfig
# import dbWorkerManager
import monitor

ROOTLOG = mbotenv.get_logger()
LOG = mbotenv.get_logger(__name__)
LOG.level = 10


class DatabaseController(object):
    def __init__(self, config=None):
        super(DatabaseController, self).__init__()
        self.config = config
        if self.config is None:
            self.config = self.read_config()

        self._load_history()
        self._init_entityConfigManager()
        # self._init_db_controller()
        self._init_monitor()

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
            **config
        )

    def _init_monitor(self):
        config = self.config['monitor']
        self.monitorContext = zmq.Context()
        self.monitorSocket = self.monitorContext.socket(zmq.PULL)
        self.monitorSocket.bind(config['zmqPostUrl'])

        self.monitor = monitor.ShotgunMonitor(
            enableStats=self.config['enableStats'],
            latestEventID=self.history.get('latestEventID', None),
            **config
        )
        self.monitorProcess = multiprocessing.Process(target=self.monitor.start)

    # def _init_db_controller(self):
    #     self.dbWorkerManager = dbWorkerManager.DBWorkerManager(
    #         shotgun=self.config['shotgun'],
    #         indexNameTemplate=self.config['indexNameTemplate'],
    #         elasticSettings=self.config['elastic'],
    #     )

    def start(self):
        self.entityConfigManager.load()
        self.monitorProcess.start()
        # self.dbWorkerManager.start()
        self.initializeEntities()
        self.run()

    def initializeEntities(self):
        LOG.debug("Initializing Entities")
        # for sgType in self.entityConfigManager.entityTypes():
        #     if self.entityConfigManager.configHasChanged(sgType):
        #         LOG.debug("CONFIG CHANGED {0}: Entity config template has changed, rebuilding cache".format(sgType))
        #         self.dbWorkerManager.postWork({'type': sgType})
        for c in self.entityConfigManager.configs.values():
            lastHash = self.history.get('configHashes', {}).get(c.type, None)

            if lastHash != c.hash:
                LOG.info("{0} Config Changed".format(c.type))
                # TODO
                # Start preload for this entity

            self.history['configHashes'][c.type] = c.hash

        self.writeHistoryToDisk()

    # def postWork(self, entityEvent, preload=False):
    #     pass
    #     # Why do I need ordering?
    #     #

    # def getWork(self):
    #     # Find the entity event with the earliest updated_at field in the queue
    #     pass

    def run(self):
        LOG.debug("Starting Main Event Loop")
        while True:
            # TODO
            # Exception handling
            work = self.monitorSocket.recv_pyobj()

            if work is None:
                continue

            if not isinstance(work, tuple):
                raise TypeError("Invalid work item: {0}".format(work))

            if work[0] == 'latestEventID':
                self.updateLatestEventID(work[1])

            print("work: {0}".format(work)) # TESTING

            LOG.debug("Posting new event")
            # self.dbWorkerManager.postWork(work)

    def updateLatestEventID(self, eventID):
        self.history['latestEventID'] = eventID
        self.writeHistoryToDisk()

    def writeHistoryToDisk(self):
        with open(self.historyPath, 'w') as f:
            yaml.dump(self.history, f, default_flow_style=False, indent=4)

if __name__ == '__main__':
    controller = DatabaseController()
    controller.start()
