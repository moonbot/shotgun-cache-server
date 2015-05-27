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
        self.load_config()

    def __getitem__(self, key):
        return self.config.__getitem__(key)

    def __iter__(self):
        return self.config.__iter__()

    def __len__(self):
        return len(self.config)

    def load_config(self):
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

        self.sg = self.config.create_shotgun_connection()

        self.configs = {}
        self._schema = None

    def __contains__(self, key):
        return key in self.configs

    @property
    def schema(self):
        if self._schema is None:
            LOG.debug("Retrieving schema from shotgun")
            self._schema = self.sg.schema_read()
        return self._schema

    def load(self, validate=True):
        self.load_config_from_files(validate=validate)

    def load_config_from_files(self, validate=True):
        """
        Read the config files and create EntityConfig instances
        """
        self.configs = {}
        for path in self.get_config_file_paths():
            typ = os.path.basename(os.path.splitext(path)[0])
            if typ == 'EventLogEntry':
                raise NotImplemented("Can't cache EventLogEntries")

            config = EntityConfig(
                typ,
                path,
            )
            if validate:
                self.validate_config(config)
            self.configs[config.type] = config

    def validate_config(self, config):
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

    def get_config_file_paths(self):
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

    def all_configs(self):
        """
        Get a list of all EntityConfig instances

        Returns:
            list of EntityConfig
        """
        return self.configs.values()

    def get_entity_types(self):
        """
        Get a list of all entity types we have configs for

        Returns:
            list of str: entity types
        """
        return self.configs.keys()

    def get_config_for_type(self, type):
        """
        Get the entity config instance for the supplied type

        Args:
            type (str): Shotgun Entity Type
        """
        return self.configs.__getitem__(type)

    def create_entity_config_files(self, types, tablePrefix=None, ignoreFields=[]):
        """
        Create the entity config json files for the supplied shotgun entity types

        Args:
            types (list of str): List of Shotgun Entity Types
            tablePrefix (str): Prefix for the rethinkdb table name
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
        defaultCachedDisplayNameMap = self.config['create_entity_config']['default_cached_display_name_dependent_fields']

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

            table = tablePrefix + str(sgType)

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

            fieldsConfig = OrderedDict()
            for field in sorted(fields):
                fieldConfig = {}
                fieldSchema = typeSchema[field]

                if field == 'cached_display_name':
                    fieldConfig['dependent_fields'] = defaultCachedDisplayNameMap.get(sgType, [])
                    if fieldConfig['dependent_fields'] == []:
                        LOG.warning("No dependent_fields defined for 'cached_display_name' for entity type '{0}'".format(sgType))

                fieldDataType = fieldSchema.get('data_type', {}).get('value', None)
                if fieldDataType == 'multi_entity':
                    fieldConfig['mapping'] = {'type': 'nested', 'include_in_parent': True}

                fieldsConfig[field] = fieldConfig
            entityConfig['fields'] = fieldsConfig

            if not os.path.exists(destFolderPath):
                os.makedirs(destFolderPath)
            with open(destPath, 'w') as f:
                f.write(json.dumps(entityConfig, indent=4))
            result.append((sgType, destPath))

        return result
