import os
import logging
import math
import datetime
import time
import fnmatch
import gevent
import shutil

import rethinkdb as r

import utils

__all__ = [
    'EntityImporter',
]

LOG = logging.getLogger(__name__)


class EntityImporter(object):
    def __init__(self, controller, config):
        super(EntityImporter, self).__init__()
        self.controller = controller
        self.config = config

        self.shotgunPool = utils.ShotgunConnectionPool(config, config['import']['max_shotgun_connections'])
        self.rethinkPool = utils.RethinkConnectionPool(config, config['import']['max_rethink_connections'])

        self.idsPerType = {}
        self.existingEntityTables = []
        self.totalEntitiesImported = 0

    def auto_import_entities(self):
        """
        Automatically import any changes or new entities
        """
        LOG.debug("Loading configs")
        # TODO - better name?
        self.controller.entityConfigManager.load_config_from_files()
        configs = self.controller.entityConfigManager.all_configs()
        [c.load_config() for c in configs]

        # Update the config if it's new or the hash has changes
        def _check_config_needs_update(config):
            savedConfigHash = self.config.history.get('cached_entities', {}).get(config.type, {}).get('config-hash', None)
            if savedConfigHash is None:
                return True
            if config.hash is None:
                return True
            elif config.hash != savedConfigHash:
                return True
            return False

        configsToLoad = filter(_check_config_needs_update, configs)

        self.delete_untracked_entities_from_cache(configs)

        if not len(configsToLoad):
            LOG.info("All entity types have been imported.")
            return

        LOG.info("Importing {0} entity types into the cache".format(len(configsToLoad)))
        self.import_entities(configsToLoad)
        LOG.info("Import complete!")

    def delete_untracked_entities_from_cache(self, configs):
        """
        Delete data from cache for entities that are no longer cached
        """
        if not self.config['delete_cache_for_untracked_entities']:
            # TODO - is this needed?
            return

        # Get the list of cached entity types
        tablePrefix = self.config['rethink_entity_table_prefix']
        with self.rethinkPool.get() as conn:
            existingTables = r.table_list().run(conn)

        existingCacheTables = []
        tablePattern = tablePrefix + '*'
        for table in existingTables:
            if fnmatch.fnmatch(table, tablePattern):
                existingCacheTables.append(table)

        usedCacheTables = [c['table'] for c in configs]
        unusedCacheTables = [t for t in existingCacheTables if t not in usedCacheTables]
        LOG.debug("Unused cache tables: {0}".format(unusedCacheTables))

        LOG.info("Deleting {0} cache tables".format(len(unusedCacheTables)))
        with self.rethinkPool.get() as conn:
            for table in unusedCacheTables:
                LOG.info("Deleting table: {0}".format(table))
                r.table_drop(table).run(conn)

        # Delete untracked files
        downloadsFolder = self.config.downloadsFolderPath
        entitiesWithFiles = [f for f in os.listdir(downloadsFolder) if os.path.isdir(os.path.join(downloadsFolder, f))]
        oldFolders = set(entitiesWithFiles) - set([c.type for c in configs])
        print("oldFolders: {0}".format(oldFolders)) # TESTING
        for oldFolder in oldFolders:
            folderPath = os.path.join(downloadsFolder, oldFolder)
            LOG.debug("Removing untracked files for type '{0}' at {1}".format(oldFolder, folderPath))
            shutil.rmtree(folderPath)

    def import_entities(self, entityConfigs):
        """
        Batch import entities from shotgun into the local shotgun cache
        Uses multiple processes to speed up retrieval
        """
        LOG.debug("Importing {0} entity types".format(len(entityConfigs)))
        importStartTime = time.time()

        # Reset
        self.totalEntitiesImported = 0

        greenlets = [gevent.spawn(self.import_entities_for_config, c) for c in entityConfigs]
        gevent.joinall(greenlets)

        totalImportTime = time.time() - importStartTime
        LOG.debug("Imported {0} entities in {1:0.2f}s".format(self.totalEntitiesImported, totalImportTime))

    def import_entities_for_config(self, config):
        LOG.debug("Importing entities for type '{0}'".format(config.type))
        entityCount = self.get_entity_counts(config)
        pageCount = int(math.ceil(entityCount / float(self.config['import.batch_size'])))

        startImportTimestamp = datetime.datetime.utcnow().isoformat()
        greenlets = [gevent.spawn(self.load_entities_for_config, config, page) for page in range(1, pageCount + 1)]
        gevent.joinall(greenlets)

        self.post_schema_for_config(config)

        self.finalize_import_for_config(config, startImportTimestamp)

    def get_entity_counts(self, entityConfig):
        # Using find instead of summarize since there is currently an inconsistency between
        # the counts returned from each.
        # find includes tasks found in task templates which don't have a project assigned.
        # However, summarize does not.
        # In order to try to keep this as abstract as possible, we'll use find instead
        # of implementing a one-off fix to ignore tasks with no project assigned.
        with self.shotgunPool.get() as sg:
            result = sg.find(
                entity_type=entityConfig.type,
                filters=[],
                fields=['id'],
            )
        return len(result)

    def load_entities_for_config(self, config, page):
        LOG.debug("Loading entities for type '{0}' on page {1}".format(config.type, page))
        try:
            kwargs = dict(
                entity_type=config.type,
                fields=config.get('fields', {}).keys(),
                filters=[],
                order=[{'column': 'id', 'direction': 'asc'}],
                limit=self.config['import.batch_size'],
                page=page
            )
            with self.shotgunPool.get() as sg:
                entities = sg.find(**kwargs)
        except Exception:
            LOG.error("Type: {entity_type}, filters: {filters}, fields: {fields} filterOperator: {filter_operator}".format(**kwargs), exc_info=True)
            raise

        greenlets = [gevent.spawn(self.prepare_entity, config, e) for e in entities]
        gevent.joinall(greenlets)
        entities = [g.value for g in greenlets]

        self.post_entities(config, entities)
        self.totalEntitiesImported += len(entities)

    def prepare_entity(self, config, entity):
        # Get rid of extra data found in sub-entities
        # We don't have a way to reliably keep these up to date except
        # for the type and id
        downloadGreenlets = []
        entitySchema = self.controller.entityConfigManager.schema[config.type]
        for field, val in entity.items():
            # Originally was storing dates and times as rethink date and time objects
            # but found this to be much slower than just storing strings
            # and converting to date objects when needed for filtering
            if field not in entitySchema:
                continue
            if field in ['type', 'id']:
                continue
            fieldDataType = entitySchema[field].get('data_type', {}).get('value', None)
            if fieldDataType == 'multi_entity':
                val = [utils.get_base_entity(e) for e in val]
                entity[field] = val
            elif fieldDataType == 'entity':
                val = utils.get_base_entity(val)
                entity[field] = val
            elif fieldDataType == 'image':
                downloadGreenlet = gevent.spawn(utils.download_file_for_field, self.config, entity, field, val)
                downloadGreenlets.append(downloadGreenlet)

        gevent.joinall(downloadGreenlets)
        [entity.update(g.value) for g in downloadGreenlets]
        return entity

    def post_entities(self, config, entities):
        LOG.debug("Posting {0} entities for type '{1}'".format(len(entities), config.type))

        tableName = config['table']

        if tableName not in self.existingEntityTables:
            with self.rethinkPool.get() as conn:
                if tableName not in r.table_list().run(conn):
                    LOG.debug("Creating table for type '{0}'".format(config.type))
                    r.table_create(tableName).run(conn)
                    self.existingEntityTables.append(tableName)

        with self.rethinkPool.get() as conn:
            result = r.table(tableName).insert(entities, conflict="replace").run(conn)
        LOG.debug("Posted entities")
        if result['errors']:
            raise IOError(result['first_error'])

        if result['errors']:
            # TODO
            # Better error descriptions?
            raise IOError("Errors occurred creating entities: {0}".format(result))

        self.idsPerType.setdefault(config.type, []).extend([e['id'] for e in entities])

    def post_schema_for_config(self, config):
        LOG.debug("Posting schema for type '{0}'".format(config.type))

        schemaTable = self.config['rethink_schema_table']

        if schemaTable not in self.existingEntityTables:
            if schemaTable not in r.table_list().run(self.controller.rethink):
                LOG.debug("Creating table for schema: {0}".format(config.type))
                with self.rethinkPool.get() as conn:
                    r.table_create(schemaTable, primary_key='type').run(conn)

        entitySchema = self.controller.entityConfigManager.schema[config.type]
        cacheSchema = dict([(field, s) for field, s in entitySchema.items() if field in config['fields']])

        # if LOG.getEffectiveLevel() < 10:
        #     LOG.debug("Cache Schema:\n{0}".format(utils.pretty_json(cacheSchema)))

        data = {}
        data['type'] = config.type
        data['schema'] = cacheSchema
        data['created_at'] = datetime.datetime.utcnow().isoformat()

        with self.rethinkPool.get() as conn:
            result = r.table(schemaTable).insert(data, conflict="replace").run(conn)
        if result['errors']:
            raise IOError(result['first_error'])

    def finalize_import_for_config(self, config, startImportTimestamp):
        LOG.info("Imported all entities for type '{0}'".format(config.type))

        # Remove existing entities stored in the cache
        # that are no longer in shotgun
        cachedEntityIDs = set(r
            .table(config['table'])
            .map(lambda asset: asset['id'])
            .coerce_to('array')
            .run(self.controller.rethink)
        )
        importedEntityIDs = set(self.idsPerType[config.type])
        diffIDs = cachedEntityIDs.difference(importedEntityIDs)

        if len(diffIDs):
            # Delete these extra entities
            # This allows us to update the cache in place without
            # having the drop the table before the import, allowing for
            # a more seamless import / update process
            LOG.info("Deleting extra entities found in cache with IDs: {0}".format(diffIDs))
            r.db('shotguncache').table(config['table']).get_all(r.args(diffIDs)).delete().run(self.controller.rethink)

        # Save the import in the history
        self.config.history.load()
        configHistory = self.config.history.setdefault('cached_entities', {}).setdefault(config.type, {})
        configHistory['config-hash'] = config.hash
        configHistory['import-timestamp'] = startImportTimestamp
        self.config.history.save()
