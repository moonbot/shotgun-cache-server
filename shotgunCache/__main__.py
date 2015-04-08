"""
Standard Python Command Line Interface for Butterfly

Allows building and exporting weights files
"""

import mbotenv  # Testing

import os
import sys
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
        'generateEntityConfigs',
        'rebuild',
        'resetStats',
        'validateCounts',
        'help',
    ]

    def __init__(self):
        self.parsers = self.setup_parsers()

    def setup_parsers(self):
        parsers = {}

        mainParser = parsers['main'] = argparse.ArgumentParser(add_help=False)

        # Operations
        grp = mainParser.add_argument_group('Operations')
        grp.add_argument('run', help='Run the cache server', default=False, action='store_true')
        grp.add_argument('validateCounts', help='Validate the current entity counts that should be cached from shotgun and compares it to the counts of what\'s actually stored in the cache db', default=False, action='store_true')
        grp.add_argument('generateEntityConfigs', help='Generate entity configuration files from Shotgun schema', default=False, action='store_true')
        grp.add_argument('rebuild', help='Rebuild of a list of entity types, rebuilds live if the server is currently running.', default=False, action='store_true')
        grp.add_argument('resetStats', help='(Developer use) Delete cache stats', default=False, action='store_true')
        grp.add_argument('help', help='Display help for an operation', default=False, action='store_true')

        # Create parsers for each operation type
        for parserName in self.ops:
            parsers[parserName] = argparse.ArgumentParser(usage="%(prog)s " + parserName + " [options]", add_help=False)

        defaultConfigPath = os.environ.get('CACHE_CONFIG', '../config.yaml')
        defaultConfigPath = os.path.abspath(os.path.join(SCRIPT_DIR, defaultConfigPath))

        # Parser specific options
        parsers['run'].add_argument('-v', '--verbosity', help='Verbosity level', default=logging.INFO)
        parsers['run'].add_argument('-c', '--config', help='Path to config.yaml. If not set uses CACHE_CONFIG environment variable or ../config.yaml.', default=defaultConfigPath)

        parsers['validateCounts'].add_argument('-v', '--verbosity', help='Verbosity level', default=logging.INFO)
        parsers['validateCounts'].add_argument('-p', '--processes', help='Number of processes to use for retrieving data from the databases', default=8)
        parsers['validateCounts'].add_argument('-c', '--config', help='Path to config.yaml. If not set uses CACHE_CONFIG environment variable or ../config.yaml.', default=defaultConfigPath)
        parsers['validateCounts'].add_argument('entityTypes', nargs='*', help='List of entity types to check.  By default all are checked', default=[])

        parsers['generateEntityConfigs'].add_argument('-v', '--verbosity', help='Verbosity level', default=logging.INFO)
        parsers['generateEntityConfigs'].add_argument('-c', '--config', help='Path to config.yaml. If not set uses CACHE_CONFIG environment variable or ../config.yaml.', default=defaultConfigPath)
        parsers['generateEntityConfigs'].add_argument('entityTypes', nargs="+", help='Entity Types to generate configs for')

        parsers['rebuild'].add_argument('--url', default=None, help="URL to the shotgunCache server. Defaults to setting stored in config.yaml")
        parsers['rebuild'].add_argument('-c', '--config', help='Path to config.yaml. If not set uses CACHE_CONFIG environment variable or ../config.yaml.', default=defaultConfigPath)
        parsers['rebuild'].add_argument('entityTypes', nargs="+", help='Entity types to re-download from Shotgun. WARNING, this will delete and rebuild the entire local cache of this entity type.')

        parsers['resetStats'].add_argument('-v', '--verbosity', help='Verbosity level', default=logging.INFO)
        parsers['resetStats'].add_argument('-a', '--all', help='Reset all stat types', default=False, action='store_true')
        parsers['resetStats'].add_argument('-c', '--config', help='Path to config.yaml. If not set uses CACHE_CONFIG environment variable or ../config.yaml.', default=defaultConfigPath)
        parsers['resetStats'].add_argument('statTypes', nargs="*", help='List of statistic types to delete')

        parsers['help'].add_argument('op', nargs="*", help='Diplay help for the supplied operation name')

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

        LOG.level = int(parseResults['verbosity'])

        configPath = parseResults['config']
        config = shotgunCache.Config.loadFromYaml(configPath)

        validator = shotgunCache.CountValidator(
            shotgunConnector,
            elastic,
            entityConfigs,
            processCount=8
        )

    def parse_generateEntityConfigs(self, parseResults):
        sys.path.append(os.path.dirname(SCRIPT_DIR))
        import shotgunCache

        LOG.level = int(parseResults['verbosity'])

        configPath = parseResults['config']
        config = shotgunCache.Config.loadFromYaml(configPath)

        controller = shotgunCache.DatabaseController(config)
        newConfigs = controller.entityConfigManager.generateEntityConfigFiles(
            parseResults['entityTypes'],
            indexTemplate=controller.config['elasticEntityIndexTemplate'],
            defaultDynamicTemplatesPerType=controller.config['generateEntityConfig']['elasticDynamicTemplates'],
            ignoreFields=controller.config['generateEntityConfig']['fieldPatternsToIgnore'],
        )
        for entityType, configPath in newConfigs:
            print("'{0}' Entity Config Template: {1}".format(entityType, configPath))

    def parse_resetStats(self, parseResults):
        sys.path.append(os.path.dirname(SCRIPT_DIR))
        import shotgunCache

        configPath = parseResults['config']
        config = shotgunCache.Config.loadFromYaml(configPath)

        LOG.level = int(parseResults['verbosity'])

        statIndexTemplate = config['elasticStatIndexTemplate']

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
        pass

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
