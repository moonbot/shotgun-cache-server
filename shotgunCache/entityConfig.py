import os
import sys
import json
import logging
import hashlib

from collections import Mapping, OrderedDict

__all__ = [
    'EntityConfig',
    'EntityConfigManager',
]

LOG = logging.getLogger(__name__)
LOG.level = 10

# FUTURE
# Better place to create shotgun connection than in EntityConfigManager?
# Project specific schema?


class EntityConfig(Mapping):
    def __init__(self, type, configPath, previousHash, elasticIndex):
        self.type = type
        self.configPath = configPath
        self.hash = None
        self.previousHash = previousHash
        self.elasticIndex = elasticIndex

        self.config = None
        self.loadConfig()

    def __getitem__(self, key):
        return self.config.__getitem__(key)

    def __iter__(self):
        return self.config.__iter__()

    def __len__(self):
        return len(self.config)

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


class EntityConfigManager(object):
    def __init__(self, configFolder, previousHashes, shotgunConnector, elasticIndexTemplate, elasticDefaultMapping):
        super(EntityConfigManager, self).__init__()
        self.configFolder = configFolder
        self.shotgunConnector = shotgunConnector
        self.previousHashes = previousHashes
        self.elasticDefaultMapping = elasticDefaultMapping
        self.configs = {}
        self.schema = None
        self.elasticIndexTemplate = elasticIndexTemplate
        self.sg = self.shotgunConnector.getInstance()

    def load(self):
        self.loadSchema()
        for path in self.getConfigFilePaths():
            typ = os.path.basename(os.path.splitext(path)[0])
            config = EntityConfig(
                typ,
                path,
                self.previousHashes.get(typ, None)
            )
            self.validateConfig(config)
            self.configs[config.type] = config

    def loadSchema(self):
        LOG.debug("Loading Shotgun Schema")
        self.schema = self.sg.schema_read()

    def validateConfig(self, config):
        if 'fields' not in config or not len(config['fields']):
            raise ValueError("No fields defined for '{0}' in {1}".format(config.type, config.configPath))

        if config.type not in self.schema:
            raise ValueError("Type '{0}' missing from Shotgun schema, defined in {1}".format(config.type, config.configPath))

        typeSchema = self.schema[config.type]
        for field in config.config.get('fields', {}):
            if field not in typeSchema:
                raise ValueError("Field '{0}' for Type '{1}' missing from Shotgun schema, defined in {2}".format(field, config.type, config.configPath))

    def getConfigFilePaths(self):
        path = os.path.abspath(self.configFolder)
        result = []
        if not os.path.exists(path):
            raise OSError("Entity config folder path doesn't exist: {0}".format(path))

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

    def generateEntityConfigFiles(self, types):
        LOG.debug("Reading Shotgun schema")
        schema = self.sg.schema_read()

        LOG.debug("Creating config files")
        for sgType in types:
            if sgType not in schema:
                raise ValueError("Missing shotgun entity type: {0}".format(sgType))

            destFolderPath = os.path.abspath(self.configFolder)
            destPath = os.path.join(destFolderPath, '{type}.json'.format(type=sgType))

            entityConfig = OrderedDict()

            index = self.elasticIndexTemplate.format(type=sgType.lower())  # Elastic requires lowercase
            entityConfig['index'] = index

            defaultMapping = self.elasticDefaultMapping
            if defaultMapping:
                entityConfig['defaultMapping'] = defaultMapping

            typeSchema = schema[sgType]
            # print envtools.format_dict(typeSchema)
            fields = typeSchema.keys()
            fieldsConfig = entityConfig['fields'] = OrderedDict()
            for field in sorted(fields):
                fieldConfig = {}
                fieldSchema = typeSchema[field]

                fieldDataType = fieldSchema.get('data_type', {}).get('value', None)
                if fieldDataType == 'multi_entity':
                    fieldConfig['mapping'] = {'type': 'nested', 'include_in_parent': True}
                elif fieldDataType == 'image':
                    # Don't store these yet
                    # TODO binary support
                    continue

                fieldsConfig[field] = fieldConfig

            if not os.path.exists(destFolderPath):
                os.makedirs(destFolderPath)
            with open(destPath, 'w') as f:
                f.write(json.dumps(entityConfig, indent=4))
            LOG.info("{0} Entity Config Template: {1}".format(sgType, destPath))
