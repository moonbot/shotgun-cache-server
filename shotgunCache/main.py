#!/usr/bin/env python

from __future__ import print_function

import os
import sys
import json
import argparse
import logging
import fnmatch
import rethinkdb

LOG = logging.getLogger('shotgunCache')
SCRIPT_DIR = os.path.dirname(__file__)
DEFAULT_CONFIG_PATH = '~/shotguncache'
CONFIG_PATH_ENV_KEY = 'SHOTGUN_CACHE_CONFIG'


class Parser(object):
    bannerWidth = 84

    def __init__(self):
        self.configPath = DEFAULT_CONFIG_PATH

        self.parser = argparse.ArgumentParser()
        self.parser.add_argument(
            '-c', '--config',
            default=None,
            help='Config folder path. '
                 'If not specified uses default({0}) or value of env var {1}'.format(
                     DEFAULT_CONFIG_PATH, CONFIG_PATH_ENV_KEY
                 )
        )
        self.parser.add_argument(
            '-v', '--verbosity',
            default=3,
            help='Verbosity level between 1 and 4. 1 - errors and warnings, 4 - everything'
        )

        self.subparsers = self.parser.add_subparsers()
        self.setup_subparser_setup()
        self.setup_subparser_run()
        self.setup_subparser_createEntityConfigs()
        self.setup_subparser_validateCounts()
        self.setup_subparser_validateFields()
        self.setup_subparser_rebuild()
        self.setup_subparser_resetStats()

    def parse(self, args):
        if not len(args):
            args = ['-h']

        parsed = self.parser.parse_args(args)

        # Verbosity
        verbosity = int(parsed.verbosity)
        if verbosity == 1:
            LOG.setLevel(logging.ERROR)
        elif verbosity == 2:
            LOG.setLevel(logging.WARNING)
        elif verbosity == 3:
            LOG.setLevel(logging.INFO)
        elif verbosity == 4:
            LOG.setLevel(logging.DEBUG)
        elif verbosity == 5:
            LOG.setLevel(1)

        # Config Path
        configPath = self.configPath
        if parsed.config is not None:
            configPath = parsed.config
        else:
            if CONFIG_PATH_ENV_KEY in os.environ:
                configPath = os.environ[CONFIG_PATH_ENV_KEY]
                LOG.debug("Using config path from env var {0}".format(CONFIG_PATH_ENV_KEY))
        self.configPath = resolveConfigPath(configPath)
        os.environ[CONFIG_PATH_ENV_KEY] = self.configPath
        LOG.debug("Config Path: {0}".format(self.configPath))

        return parsed.func(vars(parsed))

    @property
    def configFilePath(self):
        return os.path.join(self.configPath, 'config.yaml')

    def setup_subparser_setup(self):
        parser = self.subparsers.add_parser(
            'setup',
            help='Guided setup for configuring your cache server',
        )
        parser.set_defaults(func=self.handle_setup)

    def setup_subparser_run(self):
        parser = self.subparsers.add_parser(
            'run',
            help='Run the cache server',
        )
        parser.set_defaults(func=self.handle_run)

    def setup_subparser_createEntityConfigs(self):
        parser = self.subparsers.add_parser(
            'create-entity-configs',
            help='Generate entity config files from Shotgun schema',
        )
        parser.add_argument(
            'entityTypes',
            nargs="+",
            help='Entity Types to create configs for'
        )
        parser.set_defaults(func=self.handle_createEntityConfigs)

    def setup_subparser_validateCounts(self):
        parser = self.subparsers.add_parser(
            'validate-counts',
            help='Fast validation. Validate current entity counts between Shotgun and cache.'
        )
        parser.add_argument(
            '-p', '--processes',
            default=8,
            help='Number of processes to use for retrieving data from the databases'
        )
        parser.add_argument(
            '--missing',
            action="store_true",
            default=False,
            help='List the entity id\'s for missing entities'
        )
        parser.add_argument(
            'entityTypes',
            nargs='*',
            default=[],
            help='List of entity types to check.  By default all are checked'
        )
        parser.set_defaults(func=self.handle_validateCounts)

    def setup_subparser_validateFields(self):
        parser = self.subparsers.add_parser(
            'validate-fields',
            help='Slow validation. Validates entities based on data from fields between Shotgun and cache.'
                 'By default this validates the entity type and id fields.'
        )
        parser.add_argument(
            '-p', '--processes',
            default=8,
            help='Number of processes to use for retrieving data from the databases'
        )
        parser.add_argument(
            '--filters',
            default=[],
            type=json.loads,
            help='JSON formatted list of filters. Ex: \'[["id","is",100]]\'',
        )
        parser.add_argument(
            '--filterop',
            default='all',
            help='Filter operator'
        )
        parser.add_argument(
            '--fields',
            default="type,id",
            help='Fields to check, comma separated no spaces',
        )
        parser.add_argument(
            '--allCachedFields',
            default=False,
            action="store_true",
            help='Use all cached fields',
        )
        parser.add_argument(
            '--all',
            default=False,
            action="store_true",
            help='Check all cached entity types.'
                 'WARNING: This can take a really long time and be taxing on Shotgun servers.'
        )
        parser.add_argument(
            'entityTypes',
            nargs='*',
            default=[],
            help='List of entity types to check.  By default all are checked'
        )
        parser.set_defaults(func=self.handle_validateFields)

    def setup_subparser_rebuild(self):
        parser = self.subparsers.add_parser(
            'rebuild',
            help='Rebuild of a list of entity types, rebuilds live if the server is currently running.',
        )
        parser.add_argument(
            '--live',
            default=False,
            action="store_true",
            help='Send a signal to the active cache server to reload entities',
        )
        parser.add_argument(
            '--url',
            default=None,
            help="URL to the shotgunCache server."
                 "Defaults to 'zmq_controller_work_url' in config if not set"
        )
        parser.add_argument(
            '--all',
            default=False,
            action="store_true",
            help='Rebuild all cached entity types'
        )
        parser.add_argument(
            'entityTypes',
            nargs="*",
            help="Entity types to re-download from Shotgun."
                 "WARNING, this will delete and rebuild the entire local cache of this entity type."
        )
        parser.set_defaults(func=self.handle_rebuild)

    def setup_subparser_resetStats(self):
        parser = self.subparsers.add_parser(
            'reset-stats',
            help='Delete saved cache stats',
        )
        parser.add_argument(
            '-a', '--all',
            help='Reset all stat types',
            default=False,
            action='store_true'
        )
        parser.add_argument(
            'statTypes',
            nargs="*",
            help='List of statistic types to delete'
        )
        parser.set_defaults(func=self.handle_resetStats)

    def handle_setup(self, parseResults):
        import shotgunCache
        # Use ruamel to support preserving comments
        import ruamel.yaml

        def askUser(msg, error_msg="", required=False, retryCount=2):
            tryNum = 1
            while True:
                result = raw_input(msg)
                if result == "" and error_msg:
                    print(error_msg)
                if not required or result != "" or tryNum >= retryCount:
                    break
                tryNum += 1
            return result

        configTemplatePath = os.path.join(SCRIPT_DIR, 'resources', 'config.yaml.template')
        configTemplate = ruamel.yaml.load(open(configTemplatePath, 'r').read(), ruamel.yaml.RoundTripLoader)

        # Create the config folder
        configPath = askUser("Config Folder ({0}): ".format(self.configPath))
        if configPath == "":
            configPath = self.configPath

        defaultConfigPath = resolveConfigPath(DEFAULT_CONFIG_PATH)
        if configPath != defaultConfigPath:
            print ('\nYou\'ve set the config folder to location other than \'{0}\'\n'.format(DEFAULT_CONFIG_PATH) +
                   'In order for shotgunCache to find your config folder\n'
                   'you\'ll need to either set the env var \'{0}\' to the path,\n'.format(CONFIG_PATH_ENV_KEY) +
                   'or supply it via the --config flag each time you run a command\n')
            askUser("Press ENTER to continue")

        # Create the config folder
        configPath = resolveConfigPath(configPath)
        configPathDir = os.path.dirname(configPath)
        if not os.path.exists(configPathDir):
            print("ERROR: Parent folder for config doesn't exist, please create it: {0}".format(configPathDir))
        if not os.path.exists(configPath):
            LOG.debug("Creating config folder: {0}".format(configPath))
            os.mkdir(configPath)

        # Load shotgun settings
        base_url = askUser(
            "Shotgun Server URL (Ex: {0}): ".format(configTemplate['shotgun']['base_url']),
            'ERROR: You must supply a base url',
            required=True,
        )
        if not base_url:
            return
        configTemplate['shotgun']['base_url'] = base_url

        helpURL = "https://support.shotgunsoftware.com/entries/21193476-How-to-create-and-manage-API-Scripts"
        print("\nTo connect to Shotgun, we need script name and an API key, for more information visit:\n{0}\n".format(helpURL))

        script_name = askUser(
            "Script Name (Ex: {0}): ".format(configTemplate['shotgun']['script_name']),
            'ERROR: You must supply a script name',
            required=True,
        )
        if not script_name:
            return
        configTemplate['shotgun']['script_name'] = script_name

        api_key = askUser(
            "API Key (Ex: {0}): ".format(configTemplate['shotgun']['api_key']),
            'ERROR: You must supply a script name',
            required=True,
        )
        if not api_key:
            return
        configTemplate['shotgun']['api_key'] = api_key

        rethink_host = askUser(
            "RethinkDB host ({0}): ".format(configTemplate['rethink']['host']),
        )
        if rethink_host:
            configTemplate['rethink']['host'] = rethink_host

        rethink_port = askUser(
            "RethinkDB host ({0}): ".format(configTemplate['rethink']['port']),
        )
        if rethink_port:
            configTemplate['rethink']['host'] = rethink_port

        rethink_db = askUser(
            "RethinkDB Database name ({0}): ".format(configTemplate['rethink']['db']),
        )
        if rethink_db:
            configTemplate['rethink']['db'] = rethink_db

        configFilePath = os.path.join(configPath, 'config.yaml')
        with open(configFilePath, 'w') as f:
            data = ruamel.yaml.dump(configTemplate, Dumper=ruamel.yaml.RoundTripDumper, indent=4)
            f.write(data)

        entityConfigFolder = os.path.join(configPath, configTemplate['entity_config_foldername'])
        if not os.path.exists(entityConfigFolder):
            LOG.debug("Creating entity config folder: {0}".format(entityConfigFolder))
            os.mkdir(entityConfigFolder)

        print("\nWhich entities would you like to cache?\nThis should be a space separated list of Shotgun entity types.\nYou can also leave this blank and specify these later using 'shotgunCache create-entity-configs'\n")
        entityTypes = askUser("Entity Types (Ex: Asset Shot): ", required=False)
        if entityTypes:
            entityTypes = entityTypes.split(' ')

            config = shotgunCache.Config.loadFromYaml(configFilePath)
            controller = shotgunCache.DatabaseController(config)
            newConfigs = controller.entityConfigManager.createEntityConfigFiles(
                entityTypes,
                tableTemplate=controller.config['rethink_entity_table_template'],
                ignoreFields=controller.config['create_entity_config']['field_patterns_to_ignore'],
            )
            for entityType, configPath in newConfigs:
                print("'{0}' Entity Config Template: {1}".format(entityType, configPath))

        print (
            '\nSetup now complete!\n\n'
            'Before starting the server, you can make additional configuration changes\n'
            'See https://github.com/moonbot/shotgun-cache-server/wiki for more details\n\n'
            'Once you\'ve finished your changes you can start the server by running\n'
            '> shotgunCache run'
        )

    def handle_createEntityConfigs(self, parseResults):
        import shotgunCache

        config = shotgunCache.Config.loadFromYaml(self.configFilePath)

        controller = shotgunCache.DatabaseController(config)
        newConfigs = controller.entityConfigManager.createEntityConfigFiles(
            parseResults['entityTypes'],
            tableTemplate=controller.config['rethink_entity_table_template'],
            ignoreFields=controller.config['create_entity_config']['field_patterns_to_ignore'],
        )
        for entityType, configPath in newConfigs:
            print("'{0}' Entity Config Template: {1}".format(entityType, configPath))

    def handle_run(self, parseResults):
        import shotgunCache

        config = shotgunCache.Config.loadFromYaml(self.configFilePath)

        controller = shotgunCache.DatabaseController(config)
        controller.start()

    def handle_validateCounts(self, parseResults):
        import shotgunCache
        print('Validating Counts...')

        config = shotgunCache.Config.loadFromYaml(self.configFilePath)

        entityConfigManager = shotgunCache.EntityConfigManager(config=config)
        entityConfigManager.load()

        if parseResults['entityTypes']:
            entityConfigs = [entityConfigManager.getConfigForType(t) for t in parseResults['entityTypes']]
        else:
            entityConfigs = entityConfigManager.allConfigs()

        if not len(entityConfigs):
            print('No entities are configured to be cached')
            return

        validator = shotgunCache.CountValidator(config, entityConfigs)
        results = validator.start(raiseExc=False)

        failed = False
        totalCounts = {'sgCount': 0, 'cacheCount': 0, 'pendingDiff': 0}
        lineFmt = "{entityType: <26} {status: <10} {sgCount: <10} {cacheCount: <10} {pendingDiff: <12} {shotgunDiff: <12}"

        # Title Line
        titleLine = lineFmt.format(
            status="Status",
            entityType="Entity Type",
            sgCount="Shotgun",
            pendingDiff="Pending",
            cacheCount="Cache",
            shotgunDiff="Shotgun Diff"
        )
        print(titleLine)
        print('-' * self.bannerWidth)

        # Entity Totals
        for result in sorted(results, key=lambda r: r['entityType']):
            status = 'FAIL' if result['failed'] else 'OK'
            shotgunDiff = result['sgCount'] - result['cacheCount']
            print(lineFmt.format(
                entityType=result['entityType'],
                status=status,
                sgCount=result['sgCount'],
                cacheCount=result['cacheCount'],
                pendingDiff=shotgunCache.addNumberSign(result['pendingDiff']),
                shotgunDiff=shotgunCache.addNumberSign(shotgunDiff),
            ))
            totalCounts['sgCount'] += result['sgCount']
            totalCounts['cacheCount'] += result['cacheCount']
            totalCounts['pendingDiff'] += result['pendingDiff']
            if result['failed']:
                failed = True

        # Total
        print('-' * self.bannerWidth)
        status = 'ERRORS' if failed else 'OK'
        shotgunDiff = totalCounts['sgCount'] - totalCounts['cacheCount']
        print(lineFmt.format(
            entityType="Total",
            status=status,
            sgCount=totalCounts['sgCount'],
            cacheCount=totalCounts['cacheCount'],
            shotgunDiff=shotgunCache.addNumberSign(shotgunDiff),
            pendingDiff=shotgunCache.addNumberSign(totalCounts['pendingDiff']),
        ))

    def handle_validateFields(self, parseResults):
        import shotgunCache
        print('Validating Data...')

        config = shotgunCache.Config.loadFromYaml(self.configFilePath)

        entityConfigManager = shotgunCache.EntityConfigManager(config=config)
        entityConfigManager.load()

        if parseResults['all']:
            entityConfigs = entityConfigManager.allConfigs()
        elif parseResults['entityTypes']:
            entityConfigs = [entityConfigManager.getConfigForType(t) for t in parseResults['entityTypes']]
        else:
            print('ERROR: No entity types specified')
            return

        if not len(entityConfigs):
            print('No entities are configured to be cached')
            return

        filters = parseResults['filters']
        if not isinstance(filters, (list, tuple)):
            raise TypeError("Filters must be a list or a tuple")

        filterOperator = parseResults['filterop']
        if filterOperator not in ['all', 'any']:
            raise ValueError("Filter operator must be either 'all' or 'any'. Got {0}".format(filterOperator))

        fields = parseResults['fields'].split(',')

        validator = shotgunCache.FieldValidator(
            config,
            entityConfigManager,
            entityConfigs,
            filters=filters,
            filterOperator=filterOperator,
            fields=fields,
            allCachedFields=parseResults['allCachedFields'],
        )
        results = validator.start(raiseExc=False)
        results = sorted(results, key=lambda r: r['entityType'])

        errors = 0
        totalShotgunEntityCount = sum((r['shotgunMatchCount'] for r in results))

        # Missing Items
        for result in results:
            for msg, key in [('Shotgun', 'missingFromShotgun'), ('Cache', 'missingFromCache')]:
                entityType = result['entityType']
                if not result[key]:
                    continue

                print("MISSING: '{entityType}' Entity has {numMissing} missing from {source}".format(
                    entityType=entityType,
                    numMissing=len(result[key]),
                    source=msg)
                )
                print('-' * self.bannerWidth)
                for missingEntity in result[key]:
                    errors += 1
                    print(shotgunCache.prettyJson(missingEntity))
                print

        # Different Items
        for result in results:
            if not result['diffs']:
                continue

            print("DIFFERENCE: '{entityType}' has {diffCount}/{totalCount} matches that are different".format(
                entityType=entityType,
                diffCount=len(result['diffs']),
                totalCount=result['shotgunMatchCount'])
            )
            print('-' * self.bannerWidth)
            print('--- Shotgun\n+++ Cache')
            for diff in result['diffs']:
                errors += 1
                print(diff)
            print

        if not errors:
            print('SUCCESS: All {0} entities valid'.format(totalShotgunEntityCount))
        else:
            print('ERROR: {0} errors found in {0} entities'.format(errors, totalShotgunEntityCount))

    def handle_rebuild(self, parseResults):
        import shotgunCache
        import zmq

        if not parseResults['entityTypes'] and not parseResults['all']:
            print('ERROR: No config types specified')
            return 1

        config = shotgunCache.Config.loadFromYaml(self.configFilePath)

        entityConfigManager = shotgunCache.EntityConfigManager(config=config)
        LOG.info("Loading entity configs")
        entityConfigManager.load()

        availableTypes = entityConfigManager.getEntityTypes()
        configTypes = parseResults['entityTypes']
        if parseResults['all']:
            configTypes = availableTypes
        else:
            for configType in configTypes:
                if configType not in availableTypes:
                    print("WARNING: No cache configured for entity type '{0}'".format(configType))

        for configType in configTypes:
            if configType in config.history['config_hashes']:
                print("Clearing saved hash for '{0}'".format(configType))
                config.history['config_hashes'][configType] = None

        config.history.save()

        if parseResults['live']:
            url = parseResults['url']
            if url is None:
                url = config['zmq_controller_work_url']

            # Check if the server is running, if so, send a rebuild signal
            context = zmq.Context()
            socket = context.socket(zmq.PUSH)
            socket.connect(url)

            work = {
                'type': 'reloadChangedConfigs',
                'data': {}
            }
            print('Sending reload signal to cache server')
            socket.send_pyobj(work)
            print('Reload signal sent to {0}'.format(url))
        else:
            print('Entities will be reloaded the next time the server is launched')

    def handle_resetStats(self, parseResults):
        import shotgunCache

        config = shotgunCache.Config.loadFromYaml(self.configFilePath)

        statTableTemplate = config['rethink_stat_table_template']

        rethink = rethinkdb.connect(**config['rethink'])
        existingTables = rethink.table_list()

        pattern = statTableTemplate.format(type='*')

        tablesToDrop = []
        if parseResults['all']:
            tablesToDrop = [t for t in existingTables if fnmatch.fnmatch(t, pattern)]
        else:
            if not len(parseResults['statTypes']):
                raise ValueError("No stat types supplied, and '--all' was not supplied")
            for statType in parseResults['statTypes']:
                table = statTableTemplate.format(type=statType)
                if table not in existingTables:
                    LOG.warning("No table exists for type '{0}': {1}".format(statType, table))
                else:
                    tablesToDrop.append(table)

        if not len(tablesToDrop):
            LOG.info("No stats to delete")
            return

        LOG.info("Deleting {0} stat tables".format(len(tablesToDrop)))
        for table in tablesToDrop:
            LOG.info("Deleting table: {0}".format(table))
            rethink.table_drop(table)

def resolveConfigPath(path):
    path = os.path.expanduser(path)
    path = os.path.abspath(path)
    return path


def main(args, exit=False):
    global CONFIG_PATH

    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",  # ISO8601
    )

    parser = Parser()
    exitCode = parser.parse(args[1:])
    if exit:
        LOG.debug("Exit Code: {0}".format(exitCode))
        sys.exit(exitCode)
    return exitCode


if __name__ == '__main__':
    main(sys.argv)
