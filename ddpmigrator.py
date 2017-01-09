""" DDP migration tool """
from helpers.yamlhelper import ordered_load

import argparse
import os
import sys


SETTINGS_FILE = 'settings.yaml'


def parse_command_line_args():
    """ Parses command line arguments """
    # Declare command line arguments and switches
    parser = argparse.ArgumentParser(description='Export/import tool for Drawloop DDPs')
    parser.add_argument('command', choices=('export', 'import', 'push-ids'))
    parser.add_argument('--overwrite', dest='overwrite', action='store_true',
                        help='use this switch when GUID fields in Salesforce has to be overwritten with data from source directory')
    parser.add_argument('--sandbox', dest='sandbox', action='store_true',
                        help='use this switch when working with sandbox')
    parser.add_argument('-v', '--version', type=str, default='37.0',
                        help='API version (default: 37.0)')
    parser.add_argument('-u', '--username', type=str, required=True,
                        help='Salesforce user name')
    parser.add_argument('-p', '--password', type=str, required=True,
                        help='password')
    parser.add_argument('-t', '--token', type=str,
                        help='security token')
    parser.add_argument('-s', '--source-dir', type=str, required=True,
                        help='path to directory containing metadata')
    parser.add_argument('-b', '--baseline', type=str,
                        help='SHA of base commit')
    parser.add_argument('-d', '--ddp', nargs='*',
                        help='Name of one or more DDPs to be exported')
    args = parser.parse_args()

    if args.ddp:
        ddps = list()
        for ddp in args.ddp:
            ddp_decoded = ddp.replace('%2f', '/')
            if ddp_decoded not in ddps:
                ddps.append(ddp_decoded)
        args.ddp = ddps

    return args


def parse_settings(filename):
    """ Loads and parses settings file """
    # This is to handle PyInstaller --onefile packaging
    if hasattr(sys, '_MEIPASS'):
        filename = os.path.join(sys._MEIPASS, filename)
    file = open(filename)
    settings = ordered_load(file)
    file.close()
    return settings


def main():
    """ Main function """
    args = parse_command_line_args()
    settings = parse_settings(SETTINGS_FILE)

    # Prepare kwargs for export/import
    kwargs = {
        'username': args.username,
        'password': args.password,
        'is_sandbox': args.sandbox,
        'source_dir': args.source_dir,
        'api_version': args.version,
        'overwrite': args.overwrite
    }

    if args.token:
        kwargs['token'] = args.token

    if args.ddp:
        kwargs['ddp'] = args.ddp

    # Choose command class name based on command
    if args.command == 'export':
        from commands.ddpexport import DdpExport
        class_ = DdpExport
    elif args.command == 'import':
        from commands.ddpimport import DdpImport
        if args.baseline:
            kwargs['baseline'] = args.baseline
        class_ = DdpImport
    elif args.command == 'push-ids':
        from commands.ddppushids import DdpPushIds
        class_ = DdpPushIds
    else:
        sys.exit("Unknown command {0}".format(args.command))

    # Create instance of command class
    action = class_(settings, **kwargs)

    # Do the work
    action.do()


if __name__ == "__main__":
    main()
