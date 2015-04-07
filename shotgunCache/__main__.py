"""
Standard Python Command Line Interface for Butterfly

Allows building and exporting weights files
"""

import os
import sys
import argparse
import logging

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
        grp.add_argument('generateEntityConfigs', help='Generate entity configuration files from Shotgun schema', default=False, action='store_true')
        grp.add_argument('rebuild', help='Rebuild of a list of entity types, rebuilds live if the server is currently running.', default=False, action='store_true')
        grp.add_argument('help', help='Display help for an operation', default=False, action='store_true')

        # Create parsers for each operation type
        for parserName in self.ops:
            parsers[parserName] = argparse.ArgumentParser(usage="%(prog)s " + parserName + " [options]", add_help=False)

        defaultConfigPath = os.environ.get('CACHE_CONFIG', '../config.yaml')
        defaultConfigPath = os.path.abspath(os.path.join(SCRIPT_DIR, defaultConfigPath))

        # Parser specific options
        parsers['run'].add_argument('-v', '--verbosity', help='Verbosity level', default=logging.INFO)
        parsers['run'].add_argument('-c', '--config', help='Path to config.yaml. If not set uses CACHE_CONFIG environment variable or ../config.yaml.', default=defaultConfigPath)

        parsers['generateEntityConfigs'].add_argument('entityTypes', nargs="+", help='Entity Types to generate configs for')
        parsers['generateEntityConfigs'].add_argument('-c', '--config', help='Path to config.yaml. If not set uses CACHE_CONFIG environment variable or ../config.yaml.', default=defaultConfigPath)

        parsers['rebuild'].add_argument('--url', default=None, help="URL to the shotgunCache server. Defaults to setting stored in config.yaml")
        parsers['rebuild'].add_argument('entityTypes', nargs="+", help='Entity types to re-download from Shotgun. WARNING, this will delete and rebuild the entire local cache of this entity type.')
        parsers['rebuild'].add_argument('-c', '--config', help='Path to config.yaml. If not set uses CACHE_CONFIG environment variable or ../config.yaml.', default=defaultConfigPath)

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
        print("configPath: {0}".format(configPath)) # TESTING

        # Verbosity
        controller = shotgunCache.DatabaseController(configPath)
        controller.start()

    def parse_generateEntityConfigs(self, parseResults):
        sys.path.append(os.path.dirname(SCRIPT_DIR))
        import shotgunCache

        configPath = parseResults['config']
        controller = shotgunCache.DatabaseController(configPath)
        controller.entityConfigManager.generateEntityConfigFiles(
            parseResults['entityTypes'],
            indexTemplate=controller.config['elasticEntityIndexTemplate'],
            defaultDynamicTemplatesPerType=controller.config['generateEntityConfig']['elasticDynamicTemplates'],
            ignoreFields=controller.config['generateEntityConfig']['fieldPatternsToIgnore'],
        )

    def parse_rebuild(self, parseResults):
        pass

    # def parse_build(self, parseResults):
    #     rigs = []
    #     rigs = zip(parseResults['rigs'], parseResults['output'])
    #     if parseResults['rigs']:
    #         rigs = [[r, ] for r in parseResults['rigs']]
    #     if parseResults['output']:
    #         if len(parseResults['output']) > len(rigs):
    #             raise ValueError("More outputs supplied than rigs")
    #         for i, o in enumerate(parseResults['output']):
    #             rigs[i].append(o[0])
    #     if not len(rigs):
    #         LOG.error("No Rigs Specified to build")
    #         print self.parsers['build'].print_help()
    #         return

    #     kwargs = {}
    #     for mainOp in ('gui', 'force', 'debug', 'open', 'browse', 'dryrun'):
    #         kwargs[mainOp] = parseResults[mainOp]
    #     if parseResults['export'] is not None:
    #         kwargs['export'] = parseResults['export']
    #     if parseResults['optimize'] is not None:
    #         kwargs['optimize'] = parseResults['optimize']
    #     if parseResults['skipSpecs']:
    #         skipSpecs = ','.join([s.strip() for s in parseResults['skipSpecs'].split(',')])
    #         kwargs['skipSpecs'] = skipSpecs
    #     if parseResults['name']:
    #         kwargs['name'] = parseResults['name']

    #     LOG.debug("Parsed Rigs: {0}".format(rigs))
    #     LOG.debug("Parsed Kwargs: {0}".format(kwargs))

    #     global APP
    #     LOG.debug("Starting QApplication")
    #     APP = getApp()

    #     batch_build.buildRigs(rigs, **kwargs)
    #     APP.exit()

    def parse_help(self, parseResults):
        if not parseResults['op']:
            self.parsers['main'].print_help()
            return 0
        else:
            print self.parsers[parseResults['op'][0]].print_help()
            return 0


def main(args, exit=False):
    logging.basicConfig()
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
