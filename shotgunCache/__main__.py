"""
Standard Python Command Line Interface for Butterfly

Allows building and exporting weights files
"""

import os
import sys
import json
import argparse
import logging
import fnmatch
import elasticsearch

LOG = logging.getLogger('shotgunCache')

SCRIPT_DIR = os.path.dirname(__file__)

# ---------------------------------------------
# Main
# ---------------------------------------------

APP = None


class Parser(object):
    ops = [
        'run',
        'setup',
        'generateEntityConfigs',
        'rebuild',
        'resetStats',
        'validateCounts',
        'validateData',
        'help',
    ]
    termWidth = 80

    def __init__(self):
        self.parsers = self.setup_parsers()

    def setup_parsers(self):
        parsers = {}

        mainParser = parsers['main'] = argparse.ArgumentParser(add_help=False)

        # Operations
        grp = mainParser.add_argument_group('Operations')
        grp.add_argument(
            'setup',
            default=False,
            action='store_true',
            help='Guided setup for configuring your cache server',
        )
        grp.add_argument(
            'generateEntityConfigs',
            default=False,
            action='store_true',
            help='Generate entity config files from Shotgun schema',
        )
        grp.add_argument(
            'run',
            help='Run the cache server',
            default=False,
            action='store_true'
        )
        grp.add_argument(
            'validateCounts',
            default=False,
            action='store_true',
            help='Fast validation. Validate current entity counts between Shotgun and cache.'
        )
        grp.add_argument(
            'validateData',
            default=False,
            action='store_true',
            help='Slower and more data intensive check.  Validates entities based on data from fields between Shotgun and cache. By default this validates the entity id field.'
        )
        grp.add_argument(
            'rebuild',
            default=False,
            action='store_true',
            help='Rebuild of a list of entity types, rebuilds live if the server is currently running.',
        )
        grp.add_argument(
            'resetStats',
            default=False,
            action='store_true',
            help='Delete saved cache stats',
        )
        grp.add_argument(
            'help',
            default=False,
            action='store_true',
            help='Display help for an operation',
        )

        # Create parsers for each operation type
        for parserName in self.ops:
            parsers[parserName] = argparse.ArgumentParser(usage="%(prog)s " + parserName + " [options]", add_help=False)

        defaultConfigPath = os.environ.get('CACHE_CONFIG', '../config.yaml')
        defaultConfigPath = os.path.abspath(os.path.join(SCRIPT_DIR, defaultConfigPath))

        # Parser specific options

        # Validate run
        parsers['run'].add_argument(
            '-v', '--verbosity',
            default=logging.INFO,
            help='Verbosity level'
        )
        parsers['run'].add_argument(
            '-c', '--config',
            default=defaultConfigPath,
            help='Path to config.yaml. If not set uses CACHE_CONFIG environment variable or ../config.yaml.',
        )

        # Validate counts
        parsers['validateCounts'].add_argument(
            '-v', '--verbosity',
            default=logging.WARN,
            help='Verbosity level'
        )
        parsers['validateCounts'].add_argument(
            '-p', '--processes',
            default=8,
            help='Number of processes to use for retrieving data from the databases'
        )
        parsers['validateCounts'].add_argument(
            '-c', '--config',
            default=defaultConfigPath,
            help='Path to config.yaml. If not set uses CACHE_CONFIG environment variable or ../config.yaml.'
        )
        parsers['validateCounts'].add_argument(
            '--missing',
            action="store_true",
            default=False,
            help='List the entity id\'s for missing entities'
        )
        parsers['validateCounts'].add_argument(
            'entityTypes',
            nargs='*',
            default=[],
            help='List of entity types to check.  By default all are checked'
        )

        # Validate data
        parsers['validateData'].add_argument(
            '-v', '--verbosity',
            default=logging.WARN,
            help='Verbosity level'
        )
        parsers['validateData'].add_argument(
            '-p', '--processes',
            default=8,
            help='Number of processes to use for retrieving data from the databases'
        )
        parsers['validateData'].add_argument(
            '--filters',
            default=[],
            type=json.loads,
            help='JSON formatted list of filters. Ex: \'[["id","is",100]]\'',
        )
        parsers['validateData'].add_argument(
            '--filterop',
            default='all',
            help='Filter operator'
        )
        parsers['validateData'].add_argument(
            '--fields',
            default="id",
            help='Fields to check, comma separated no spaces',
        )
        parsers['validateData'].add_argument(
            '--allCachedFields',
            default=False,
            action="store_true",
            help='Use all cached fields',
        )
        parsers['validateData'].add_argument(
            '--all',
            default=False,
            action="store_true",
            help='Check all cached entity types. WARNING: This can take a really long time and be taxing on Shotgun servers.'
        )
        parsers['validateData'].add_argument(
            '-c', '--config',
            default=defaultConfigPath,
            help='Path to config.yaml. If not set uses CACHE_CONFIG environment variable or ../config.yaml.'
        )
        parsers['validateData'].add_argument(
            'entityTypes',
            nargs='*',
            default=[],
            help='List of entity types to check.  By default all are checked'
        )

        # Generate Entity Configs
        parsers['generateEntityConfigs'].add_argument(
            '-v', '--verbosity',
            help='Verbosity level',
            default=logging.INFO
        )
        parsers['generateEntityConfigs'].add_argument(
            '-c', '--config',
            default=defaultConfigPath,
            help='Path to config.yaml. If not set uses CACHE_CONFIG environment variable or ../config.yaml.',
        )
        parsers['generateEntityConfigs'].add_argument(
            'entityTypes',
            nargs="+",
            help='Entity Types to generate configs for'
        )

        # Rebuild
        parsers['rebuild'].add_argument(
            '-v', '--verbosity',
            help='Verbosity level',
            default=logging.INFO
        )
        parsers['rebuild'].add_argument(
            '--live',
            default=False,
            action="store_true",
            help='Send a signal to the active cache server to reload entities',
        )
        parsers['rebuild'].add_argument(
            '--url',
            default=None,
            help="URL to the shotgunCache server. Defaults to 'zmq_controller_work_url' in config if not set"
        )
        parsers['rebuild'].add_argument(
            '-c', '--config',
            help='Path to config.yaml. If not set uses CACHE_CONFIG environment variable or ../config.yaml.',
            default=defaultConfigPath
        )
        parsers['rebuild'].add_argument(
            '--all',
            default=False,
            action="store_true",
            help='Rebuild all cached entity types'
        )
        parsers['rebuild'].add_argument(
            'entityTypes',
            nargs="*",
            help='Entity types to re-download from Shotgun. WARNING, this will delete and rebuild the entire local cache of this entity type.'
        )

        # Reset Stats
        parsers['resetStats'].add_argument(
            '-v', '--verbosity',
            help='Verbosity level',
            default=logging.INFO
        )
        parsers['resetStats'].add_argument(
            '-a', '--all',
            help='Reset all stat types',
            default=False,
            action='store_true'
        )
        parsers['resetStats'].add_argument(
            '-c', '--config',
            help='Path to config.yaml. If not set uses CACHE_CONFIG environment variable or ../config.yaml.',
            default=defaultConfigPath
        )
        parsers['resetStats'].add_argument(
            'statTypes',
            nargs="*",
            help='List of statistic types to delete'
        )

        # Help
        parsers['help'].add_argument(
            'op',
            nargs="*",
            help='Diplay help for the supplied operation name'
        )

        return parsers

    def parse(self, args):
        if not len(args):
            args = ['help']

        op = args[0]
        if op not in self.ops:
            LOG.error("Invalid operation: {0}".format(op))
            return 1

        parseResults = vars(self.parsers[op].parse_args(args[1:]))
        if hasattr(self, "parse_" + op):
            return getattr(self, "parse_" + op)(parseResults)
        else:
            LOG.error("Missing operation method: {0}".format(op))
            return 2

    def parse_run(self, parseResults):
        sys.path.append(os.path.dirname(SCRIPT_DIR))
        import shotgunCache

        LOG.level = int(parseResults['verbosity'])

        configPath = parseResults['config']
        config = shotgunCache.Config.loadFromYaml(configPath)

        # Verbosity
        controller = shotgunCache.DatabaseController(config)
        controller.start()

    def parse_validateCounts(self, parseResults):
        sys.path.append(os.path.dirname(SCRIPT_DIR))
        import shotgunCache
        print('Validating Counts...')

        LOG.level = int(parseResults['verbosity'])

        configPath = parseResults['config']
        config = shotgunCache.Config.loadFromYaml(configPath)

        entityConfigManager = shotgunCache.EntityConfigManager(config=config)
        entityConfigManager.load()

        if parseResults['entityTypes']:
            entityConfigs = [entityConfigManager.getConfigForType(t) for t in parseResults['entityTypes']]
        else:
            entityConfigs = entityConfigManager.allConfigs()

        if not len(entityConfigs):
            print 'No entities are configured to be cached'
            return

        validator = shotgunCache.CountValidator(config, entityConfigs, parseResults['missing'])
        results = validator.start(raiseExc=False)

        for result in results:
            for msg, key in [('Shotgun', 'missingIDsFromShotgun'), ('Cache', 'missingIDsFromCache')]:
                entityType = result['entityType']
                if not result[key]:
                    continue

                print("MISSING: '{0}' Entity IDs missing from {1}".format(entityType, msg))
                print('-' * self.termWidth)
                print(', '.join([str(n) for n in result[key]]))
                print

        failed = False
        totalCounts = {'sgCount': 0, 'elasticCount': 0, 'pendingDiff': 0}
        lineFmt = "{entityType: <16} {status: <10} {sgCount: <10} {elasticCount: <10} {pendingDiff: <12} {shotgunDiff: <12}"

        # Title Line
        titleLine = lineFmt.format(
            status="Status",
            entityType="Entity Type",
            sgCount="Shotgun",
            pendingDiff="Pending",
            elasticCount="Cache",
            shotgunDiff="Shotgun Diff"
        )
        print(titleLine)
        print('-' * self.termWidth)

        # Entity Totals
        for result in sorted(results, key=lambda r: r['entityType']):
            status = 'FAIL' if result['failed'] else 'OK'
            shotgunDiff = result['sgCount'] - result['elasticCount']
            print(lineFmt.format(
                entityType=result['entityType'],
                status=status,
                sgCount=result['sgCount'],
                elasticCount=result['elasticCount'],
                pendingDiff=shotgunCache.addNumberSign(result['pendingDiff']),
                shotgunDiff=shotgunCache.addNumberSign(shotgunDiff),
            ))
            totalCounts['sgCount'] += result['sgCount']
            totalCounts['elasticCount'] += result['elasticCount']
            totalCounts['pendingDiff'] += result['pendingDiff']
            if result['failed']:
                failed = True

        # Total
        print('-' * self.termWidth)
        status = 'ERRORS' if failed else 'OK'
        shotgunDiff = totalCounts['sgCount'] - totalCounts['elasticCount']
        print(lineFmt.format(
            entityType="Total",
            status=status,
            sgCount=totalCounts['sgCount'],
            elasticCount=totalCounts['elasticCount'],
            shotgunDiff=shotgunCache.addNumberSign(shotgunDiff),
            pendingDiff=shotgunCache.addNumberSign(totalCounts['pendingDiff']),
        ))

    def parse_validateData(self, parseResults):
        sys.path.append(os.path.dirname(SCRIPT_DIR))
        import shotgunCache
        print('Validating Data...')

        LOG.level = int(parseResults['verbosity'])

        configPath = parseResults['config']
        config = shotgunCache.Config.loadFromYaml(configPath)

        entityConfigManager = shotgunCache.EntityConfigManager(config=config)
        entityConfigManager.load()

        if parseResults['all']:
            entityConfigs = entityConfigManager.allConfigs()
        elif parseResults['entityTypes']:
            entityConfigs = [entityConfigManager.getConfigForType(t) for t in parseResults['entityTypes']]
        else:
            print 'ERROR: No entity types specified'
            return

        if not len(entityConfigs):
            print 'No entities are configured to be cached'
            return

        filters = parseResults['filters']
        if not isinstance(filters, (list, tuple)):
            raise TypeError("Filters must be a list or a tuple")

        filterOperator = parseResults['filterop']
        if filterOperator not in ['all', 'any']:
            raise ValueError("Filter operator must be either 'all' or 'any'. Got {0}".format(filterOperator))

        fields = parseResults['fields'].split(',')

        validator = shotgunCache.DataValidator(
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
                print('-' * self.termWidth)
                for missingEntity in result[key]:
                    errors += 1
                    print(shotgunCache.prettyJson(e))
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
            print('-' * self.termWidth)
            print('--- Shotgun\n+++ Cache')
            for diff in result['diffs']:
                errors += 1
                print(diff)
            print

        if not errors:
            print('SUCCESS: All {0} entities valid'.format(totalShotgunEntityCount))
        else:
            print('ERROR: {0} errors found in {0} entities'.format(errors, totalShotgunEntityCount))

    def parse_generateEntityConfigs(self, parseResults):
        sys.path.append(os.path.dirname(SCRIPT_DIR))
        import shotgunCache

        LOG.level = int(parseResults['verbosity'])

        configPath = parseResults['config']
        config = shotgunCache.Config.loadFromYaml(configPath)

        controller = shotgunCache.DatabaseController(config)
        newConfigs = controller.entityConfigManager.generateEntityConfigFiles(
            parseResults['entityTypes'],
            indexTemplate=controller.config['elastic_entity_index_template'],
            defaultDynamicTemplatesPerType=controller.config['generate_entity_config']['elastic_dynamic_templates'],
            ignoreFields=controller.config['generate_entity_config']['field_patterns_to_ignore'],
        )
        for entityType, configPath in newConfigs:
            print("'{0}' Entity Config Template: {1}".format(entityType, configPath))

    def parse_resetStats(self, parseResults):
        sys.path.append(os.path.dirname(SCRIPT_DIR))
        import shotgunCache

        configPath = parseResults['config']
        config = shotgunCache.Config.loadFromYaml(configPath)

        LOG.level = int(parseResults['verbosity'])

        statIndexTemplate = config['elastic_stat_index_template']

        elastic = elasticsearch.Elasticsearch(**config['elasticsearch_connection'])
        existingIndices = elastic.indices.status()['indices'].keys()

        indicesToRemove = []
        if parseResults['all']:
            pattern = statIndexTemplate.format(type="*")
            for index in existingIndices:
                if fnmatch.fnmatch(index, pattern):
                    indicesToRemove.append(index)
        else:
            if not len(parseResults['statTypes']):
                raise ValueError("No stat types supplied, and '--all' was not supplied")
            for statType in parseResults['statTypes']:
                index = statIndexTemplate.format(type=statType)
                if index not in existingIndices:
                    LOG.warning("No elastic index exists for type '{0}': {1}".format(statType, index))
                else:
                    indicesToRemove.append(index)

        if not len(indicesToRemove):
            LOG.info("No elastic indexes to delete")
            return

        LOG.info("Deleting {0} elastic indexes".format(len(indicesToRemove)))
        for index in indicesToRemove:
            LOG.info("Deleting index: {0}".format(index))
            elastic.indices.delete(index=index)

    def parse_rebuild(self, parseResults):
        sys.path.append(os.path.dirname(SCRIPT_DIR))
        import shotgunCache
        import zmq

        configPath = parseResults['config']
        config = shotgunCache.Config.loadFromYaml(configPath)

        entityConfigManager = shotgunCache.EntityConfigManager(config=config)
        entityConfigManager.load()

        LOG.level = int(parseResults['verbosity'])

        availableTypes = entityConfigManager.getEntityTypes()
        configTypes = parseResults['entityTypes']
        if parseResults['all']:
            configTypes = availableTypes
        elif not configTypes:
            print 'ERROR: No config types supplied'
        else:
            for configType in configTypes:
                if configType not in availableTypes:
                    print "WARNING: No cache configured for entity type '{0}'".format(configType)

        for configType in configTypes:
            if configType in config.history['config_hashes']:
                print "Clearing saved hash for '{0}'".format(configType)
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
            print 'Sending reload signal to cache server'
            socket.send_pyobj(work)
            print 'Reload signal sent to {0}'.format(url)
        else:
            print 'Entities will be reloaded the next time the server is launched'

    def parse_help(self, parseResults):
        if not parseResults['op']:
            self.parsers['main'].print_help()
            return 0
        else:
            print self.parsers[parseResults['op'][0]].print_help()
            return 0


def main(args, exit=False):
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",  # ISO8601
    )

    # Turn off warnings from elasticsearch loggers
    # these spit out warnings that are sometimes confusing or unneeded
    elasticLogger = logging.getLogger("elasticsearch")
    elasticLogger.setLevel(logging.ERROR)

    parser = Parser()
    exitCode = parser.parse(args[1:])
    if exit:
        LOG.debug("Exit Code: {0}".format(exitCode))
        sys.exit(exitCode)
    return exitCode

if __name__ == '__main__':
    try:
        main(sys.argv)
    except Exception, e:
        LOG.exception(e)
