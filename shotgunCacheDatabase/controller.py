import os
import zmq
import multiprocessing
import yaml

import logging

import mbotenv  # For paths, TODO

import entityConfig
# import dbWorkerManager
import monitor

LOG = logging.getLogger(__name__)
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
            previousHashes=self.history.get('configHashes', {}),
            shotgunConfig=self.config['shotgun'],
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
            shotgunConfig=self.config['shotgun'],
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
        self.monitor.setEntityTypes(self.entityConfigManager.getEntityTypes())
        self.monitorProcess.start()
        # self.dbWorkerManager.start()
        self.preloadEntities()
        self.run()

    def preloadEntities(self):
        LOG.debug("Initializing Entities")
        # TODO
        # Force preloading?
        for c in self.entityConfigManager.configs.values():
            if c.needsUpdate():
                LOG.info("{0} Config Updated".format(c.type))
                # TODO
                # Start preload for this entity

            self.history['configHashes'][c.type] = c.hash

        self.writeHistoryToDisk()

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
