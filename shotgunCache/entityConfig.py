import os
import sys
import logging
import hashlib
import fnmatch
import json
from collections import Mapping, OrderedDict

__all__ = [
    'EntityConfig',
    'EntityConfigManager',
]

LOG = logging.getLogger(__name__)


class EntityConfig(Mapping):
    """
    Config dictionary for a single entity type
    """
    def __init__(self, type, configPath):
        self.type = type
        self.configPath = configPath
        self.hash = None

        self.config = None
        self.loadConfig()

    def __getitem__(self, key):
        return self.config.__getitem__(key)

    def __iter__(self):
        return self.config.__iter__()

    def __len__(self):
        return len(self.config)

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
    def __init__(self, config):
        super(EntityConfigManager, self).__init__()
        self.config = config

        self.sg = self.config.createShotgunConnection()

        self.configs = {}
        self.schema = None

    def __contains__(self, key):
        return key in self.configs

    def load(self):
        LOG.debug("Retrieving schema from shotgun")
        self.schema = self.sg.schema_read()
        self.loadConfigFromFiles()

    def loadConfigFromFiles(self):
        """
        Read the config files and create EntityConfig instances
        """
        for path in self.getConfigFilePaths():
            typ = os.path.basename(os.path.splitext(path)[0])
            if typ == 'EventLogEntry':
                raise NotImplemented("Can't cache EventLogEntries")

            config = EntityConfig(
                typ,
                path,
            )
            self.validateConfig(config)
            self.configs[config.type] = config

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
        path = os.path.abspath(self.config.entityConfigFolderPath)
        result = []
        if not os.path.exists(path):
            LOG.debug("Creating entity config folder: {0}".format(path))
            os.mkdir(path)

        for f in os.listdir(path):
            if not f.endswith('.json'):
                continue
            result.append(os.path.join(path, f))
        LOG.debug("Found {0} Entity Config Files".format(len(result)))
        return result

    def allConfigs(self):
        """
        Get a list of all EntityConfig instances

        Returns:
            list of EntityConfig
        """
        return self.configs.values()

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

        Args:
            type (str): Shotgun Entity Type
        """
        return self.configs.__getitem__(type)

    def createEntityConfigFiles(self, types, tableTemplate=None, ignoreFields=[]):
        """
        Create the entity config json files for the supplied shotgun entity types

        Args:
            types (list of str): List of Shotgun Entity Types
            tableTemplate (str): Template for the rethinkdb table name
                supplied format keywords:
                    type - Shotgun type
                Ex:
                    entity-{type}
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

        LOG.debug("Creating config files")
        result = []
        for sgType in types:
            if sgType not in schema:
                raise ValueError("Missing shotgun entity type: {0}".format(sgType))

            if sgType == 'EventLogEntry':
                raise NotImplemented("Can't cache EventLogEntry entities")

            destFolderPath = os.path.abspath(self.config.entityConfigFolderPath)
            destPath = os.path.join(destFolderPath, '{type}.json'.format(type=sgType))

            entityConfig = OrderedDict()

            table = tableTemplate.format(type=sgType)

            entityConfig['table'] = table

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

            filters = []
            for field, _filters in self.config['create_entity_config.default_filters'].items():
                if field in fields:
                    filters.extend(_filters)
            entityConfig['filters'] = filters

            fieldsConfig = OrderedDict()
            for field in sorted(fields):
                fieldConfig = {}
                fieldSchema = typeSchema[field]

                fieldDataType = fieldSchema.get('data_type', {}).get('value', None)
                if fieldDataType == 'multi_entity':
                    fieldConfig['mapping'] = {'type': 'nested', 'include_in_parent': True}
                elif fieldDataType == 'image':
                    # Not supported yet
                    continue

                fieldsConfig[field] = fieldConfig
            entityConfig['fields'] = fieldsConfig

            if not os.path.exists(destFolderPath):
                os.makedirs(destFolderPath)
            with open(destPath, 'w') as f:
                f.write(json.dumps(entityConfig, indent=4))
            result.append((sgType, destPath))

        return result
