import os
import sys
import json
import logging
import hashlib
import fnmatch

from collections import Mapping, OrderedDict

import utils

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
    """
    Config dictionary for a single entity type
    """
    def __init__(self, type, configPath, previousHash):
        self.type = type
        self.configPath = configPath
        self.hash = None
        self.previousHash = previousHash

        self.config = None
        self.loadConfig()

    def __getitem__(self, key):
        return self.config.__getitem__(key)

    def __iter__(self):
        return self.config.__iter__()

    def __len__(self):
        return len(self.config)

    def needsUpdate(self):
        """
        Check if the config has changed
        """
        if self.hash is None:
            return True
        elif self.hash != self.previousHash:
            return True
        return False

    def loadConfig(self):
        """
        Read the config from the json file
        """
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
    """
    Manages the entity config files storing the details of how
    Shotgun Entities are stored in the database
    """

    def __init__(self, configFolder, previousHashes, shotgunConnector):
        super(EntityConfigManager, self).__init__()
        self.configFolder = configFolder
        self.previousHashes = previousHashes
        self.shotgunConnector = shotgunConnector

        self.sg = self.shotgunConnector.getInstance()

        self.configs = {}
        self.schema = None

    def load(self):
        self.loadShotgunSchema()
        for path in self.getConfigFilePaths():
            typ = os.path.basename(os.path.splitext(path)[0])
            config = EntityConfig(
                typ,
                path,
                self.previousHashes.get(typ, None),
            )
            self.validateConfig(config)
            self.configs[config.type] = config

    def loadShotgunSchema(self):
        LOG.debug("Loading Shotgun Schema")
        self.schema = self.sg.schema_read()

    def validateConfig(self, config):
        """
        Validate an entity config dictionary

        Args:
            config (dict): entity config dictionary
        Raises:
            ValueError
        """
        if 'fields' not in config or not len(config['fields']):
            raise ValueError("No fields defined for '{0}' in {1}".format(config.type, config.configPath))

        if config.type not in self.schema:
            raise ValueError("Type '{0}' missing from Shotgun schema, defined in {1}".format(config.type, config.configPath))

        typeSchema = self.schema[config.type]
        for field in config.config.get('fields', {}):
            if field not in typeSchema:
                raise ValueError("Field '{0}' for Type '{1}' missing from Shotgun schema, defined in {2}".format(field, config.type, config.configPath))

    def getConfigFilePaths(self):
        """
        Get a list of all config file paths containing entity configs
        Returns:
            list of str: file paths
        """
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
        """
        Get a list of all entity types we have configs for

        Returns:
            list of str: entity types
        """
        return self.configs.keys()

    def getConfigForType(self, type):
        """
        Get the entity config instance for the supplied type
        """
        return self.configs.__getitem__(type)

    def generateEntityConfigFiles(self, types, indexTemplate=None, defaultDynamicTemplatesPerType=None, ignoreFields=[]):
        """
        Generate the entity config json files for the supplied shotgun entity types

        Args:
            types (list of str): List of Shotgun Entity Types
            indexTemplate (str): Template for the elastic index name
                supplied format keywords:
                    type - Shotgun type
                Ex:
                    shotguncache-entity-{type}
            defaultDynamicTemplatesPerType (dict): Dictionary of elastic dynamic templates.
                templates stored inside the 'all' key will be applied to all entity types.
                Ex:
                {
                    'all': [{
                                'template_1': {
                                    "match_mapping_type": "string",
                                    "mapping": {
                                        "index": "not_analyzed",
                                        "type": "string",
                                        "omit_norms": true
                                    },
                                    "match": "*"
                                }
                            }]

                }
            ignoreFields (list of str): global list of field names to exclude.
                These can use wildcards using fnmatch
                Ex:
                    created_*
                    cached_display_name

        Raises:
            ValueError

        """
        LOG.debug("Reading Shotgun schema")
        schema = self.sg.schema_read()

        defauiltDynamicTemplates = defaultDynamicTemplatesPerType.get('all', {})

        LOG.debug("Creating config files")
        for sgType in types:
            if sgType not in schema:
                raise ValueError("Missing shotgun entity type: {0}".format(sgType))

            destFolderPath = os.path.abspath(self.configFolder)
            destPath = os.path.join(destFolderPath, '{type}.json'.format(type=sgType))

            entityConfig = OrderedDict()

            index = indexTemplate.format(type=sgType.lower())  # Elastic requires lowercase
            entityConfig['index'] = index

            dynamicTemplates = defauiltDynamicTemplates[:]
            dynamicTemplates.extend(defaultDynamicTemplatesPerType.get(sgType, []))
            entityConfig['dynamic_templates'] = dynamicTemplates

            typeSchema = schema[sgType]
            fields = typeSchema.keys()

            if ignoreFields:
                def excludeIgnoredFields(field):
                    for pat in ignoreFields:
                        result = fnmatch.fnmatch(field, pat)
                        if result:
                            return False
                    return True
                fields = filter(excludeIgnoredFields, fields)

            fieldsConfig = OrderedDict()
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
            entityConfig['fields'] = fieldsConfig

            if not os.path.exists(destFolderPath):
                os.makedirs(destFolderPath)
            with open(destPath, 'w') as f:
                f.write(json.dumps(entityConfig, indent=4))
            LOG.info("{0} Entity Config Template: {1}".format(sgType, destPath))