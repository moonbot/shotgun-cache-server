import os
import sys
import json
import mbotenv
import hashlib

__all__ = [
    'EntityConfig',
    'EntityConfigManager',
]

LOG = mbotenv.get_logger(__name__)
LOG.level = 10

# TODO
# How to handle fields added / removed from shotgun
# Should I be querying shotgun during load to ensure the fields exists?
# Entity Field changes happen with Shotgun_DisplayColumn_Retirement

class EntityConfig(object):
    def __init__(self, type, configPath):
        self.type = type
        self.configPath = configPath
        self.hash = None
        self.config = self.loadConfig()

    def loadConfig(self):
        LOG.debug("Loading Entity Config for type: {0}".format(self.type))
        try:
            with open(self.configPath, 'r') as f:
                data = f.read()
                self.hash = str(hashlib.md5(data).hexdigest())
                return json.loads(data)
        except ValueError, e:
            raise type(e), type(e)(e.message + ' happens in {0}'.format(self.configPath)), sys.exc_info()[2]


class EntityConfigManager(object):
    def __init__(self, configFolder, enableStats=False):
        super(EntityConfigManager, self).__init__()
        self.enableStats = enableStats
        self.configFolder = configFolder
        self.configs = {}

    def load(self):
        for path in self.getConfigFilePaths():
            LOG.debug("Loading Entity Config with path: {0}".format(path))
            cfg = EntityConfig(
                os.path.basename(os.path.splitext(path)[0]),
                path
            )
            self.configs[cfg.type] = cfg

    def getConfigFilePaths(self):
        path = os.path.abspath(self.configFolder)
        result = []
        for f in os.listdir(path):
            if not f.endswith('.json'):
                continue
            result.append(os.path.join(path, f))
        LOG.debug("Found {0} Entity Config Files".format(len(result)))
        return result

    def getEntityTypes(self):
        return self.configs.keys()

    def getConfigForEntity(self, type):
        return self.configs.__getitem__(type)