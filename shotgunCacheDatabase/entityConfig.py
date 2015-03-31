import os
import sys
import json
import logging
import hashlib

import shotgun_api3 as sg

__all__ = [
    'EntityConfig',
    'EntityConfigManager',
]

LOG = logging.getLogger(__name__)
LOG.level = 10

# FUTURE
# Better place to create shotgun connection than in EntityConfigManager?
# Project specific schema?

class EntityConfig(object):
    def __init__(self, type, configPath, previousHash, manager):
        self.type = type
        self.configPath = configPath
        self.hash = None
        self.previousHash = previousHash
        self.manager = manager

        self.config = self.loadConfig()

    def needsUpdate(self):
        if self.hash is None:
            return True
        elif self.hash != self.previousHash:
            return True
        return False

    def loadConfig(self):
        LOG.debug("Loading Entity Config for type: {0}".format(self.type))

        with open(self.configPath, 'r') as f:
            data = f.read()
        self.hash = str(hashlib.md5(data).hexdigest())

        try:
            config = json.loads(data)
        except ValueError, e:
            raise type(e), type(e)(e.message + ' happens in {0}'.format(self.configPath)), sys.exc_info()[2]

        self.config = config
        self.validateConfig()

    def validateConfig(self):
        if 'fields' not in self.config or not len(self.config['fields']):
            raise ValueError("No fields defined for '{0}' in {1}".format(self.type, self.configPath))

        schema = self.manager.schema
        if self.type not in schema:
            raise ValueError("Type '{0}' missing from Shotgun schema, defined in {1}".format(self.type, self.configPath))

        typeSchema = schema[self.type]
        for field in self.config.get('fields', {}):
            if field not in typeSchema:
                raise ValueError("Field '{0}' for Type '{1}' missing from Shotgun schema, defined in {2}".format(field, self.type, self.configPath))


class EntityConfigManager(object):
    def __init__(self, configFolder, previousHashes, shotgunConfig, enableStats=False):
        super(EntityConfigManager, self).__init__()
        self.enableStats = enableStats
        self.configFolder = configFolder
        self.shotgunConfig = shotgunConfig
        self.previousHashes = previousHashes
        self.configs = {}
        self.sg = None
        self.schema = None

    def load(self):
        self.connectToShotgun()
        self.loadSchema()
        for path in self.getConfigFilePaths():
            typ = os.path.basename(os.path.splitext(path)[0])
            cfg = EntityConfig(
                typ,
                path,
                self.previousHashes.get(typ, None),
                self
            )
            self.configs[cfg.type] = cfg

    def connectToShotgun(self):
        LOG.debug("Connecting to Shotgun")
        self.sg = sg.Shotgun(
            self.shotgunConfig['url'],
            self.shotgunConfig['scriptName'],
            self.shotgunConfig['apiKey']
        )

    def loadSchema(self):
        LOG.debug("Loading Shotgun Schema")
        self.schema = self.sg.schema_read()

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
