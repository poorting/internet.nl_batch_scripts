#! /usr/bin/env python3

###############################################################################
import sys
import os
import signal
import argparse
# from argparse import RawTextHelpFormatter

import requests
import textwrap
import pprint

from lib import utils as ut

###############################################################################
program_name = os.path.basename(__file__)
VERSION = 0.2


###############################################################################
class ArgumentParser(argparse.ArgumentParser):

    def error(self, message):
        print('\n\033[1;33mError: {}\x1b[0m\n'.format(message))
        self.print_help(sys.stderr)
        # self.exit(2, '%s: error: %s\n' % (self.prog, message))
        self.exit(2)

    # def format_usage(self):
    #     usage = super()
    #     return "CUSTOM"+usage


###############################################################################
# Subroutines
# ------------------------------------------------------------------------------
def parser_add_arguments():
    """
        Parse comamnd line parameters
    """
    parser = ArgumentParser(
        prog=program_name,
        description=textwrap.dedent('''\
                        Utility for interacting with the batch requests API of internet.nl

                        The following commands are supported:
                        
                        \033[1m list\033[0m - list all or some of the batch requests 
                        \033[1m stat\033[0m - get the status of a specific request
                        \033[1m get\033[0m  - retrieve the results of a request
                        \033[1m del\033[0m  - delete a request

                        '''),
        epilog=textwrap.dedent('''\
                        \033[1mConfiguration\033[0m
                        The configuration (endpoint and credentials) are taken from the first section of the 
                        \033[3mbatch-request.conf\033[0m configuration file. You can specify other sections to use with 
                        the \033[3m-p\033[0m option. If username or password are missing then you are prompted for them.
                        If the endpoint is missing then the default internet.nl endpoint is used.
                        
                        If there is no configuration file then the default internet.nl endpoint is used
                        and you are prompted for a username and password. 

                        \033[1mSome examples:\033[0m
                        
                        \033[3m./%(prog)s\033[0m
                        list the details of all the measurement batches

                        \033[3m./%(prog)s list 4 \033[0m
                        list the details of the last four measurement batches

                        \033[3m./%(prog)s list 4 -p \033[0m
                        same, but prompts you for username and password

                        \033[3m./%(prog)s list 4 -p dev\033[0m
                        same, takes configuration from \033[3m[dev]\033[0m section in \033[3mbatch-request.conf\033[0m

                        \033[3m./%(prog)s stat 02e19b69317a4aa2958980312754de52\033[0m
                        get the status of batch 02e19b69317a4aa2958980312754de52

                        \033[3m./%(prog)s get 02e19b69317a4aa2958980312754de52\033[0m
                        get the json results of batch 02e19b69317a4aa2958980312754de52
                        
                        '''),
        formatter_class=argparse.RawTextHelpFormatter, )

    parser.add_argument("command",
                        help="the request to execute (default: list)",
                        action="store",
                        nargs="?",
                        choices=["list", "stat", "get", "del", ],
                        default="list")

    parser.add_argument('parameter', nargs='?', metavar='parameter',
                        help=textwrap.dedent('''\
                        extra parameter for the request, type and meaning depend on the request:
                        \033[1mlist\033[0m         (optional): the number of items to list (default: 0 = all)
                        \033[1mstat,get,del\033[0m (required): the request_id for the \033[1mstat,get or del\033[0m request
                        '''))

    parser.add_argument("-V", "--version",
                        help="print version and exit",
                        action="version",
                        version='%(prog)s (version {})'.format(VERSION))

    parser.add_argument('-p',
                        action="store",
                        nargs="?",
                        const='',
                        metavar='SECTION',
                        help=textwrap.dedent('''\
                        get the credentials from the specified \033[3m[SECTION]\033[0m in the configuration file 
                        just the option without SECTION will prompt you for username and password
                        '''))

    # parser.usage = "duckdb-to-graphs.py [-q] [-t TYPE] database [N]"

    return parser


###############################################################################
def main():
    # signal.signal(signal.SIGINT, signal_handler)

    parser = parser_add_arguments()

    # try:
    args = parser.parse_args()
    # Works only for Python 3.9+
    # except argparse.ArgumentError as ae:
    # except SystemExit:
    #     print('Catching an argumentError')
    #     exit(1)

    pp = pprint.PrettyPrinter(indent=4)

    request_id = '0'
    action = args.command
    parameter = args.parameter

    if action in ['stat', 'get', 'del']:
        if parameter is None:
            print('Please specify a valid request_id')
            exit(2)
        request_id = parameter
    elif action == 'list':
        if parameter is None:
            request_id = 0
        else:
            try:
                request_id = int(parameter)
                if request_id < 0:
                    raise ValueError()
            except ValueError:
                print('\n\033[0;33mPlease specify a positive integer number as a parameter for the \033[1mlist\033[0;33m command\x1b[0m\n')
                exit(2)

    try:
        credentials = ut.getCredentials(args.p)
    except KeyboardInterrupt:
        sys.stdout.flush()
        print()
        exit(0)
    except Exception as e:
        print("error opening/processing credentials file: {}".format(e))
        exit(1)
    try:
        if action == 'list':
            r = requests.get(credentials['endpoint'],
                             params={'limit': request_id},
                             auth=(credentials['username'], credentials['password']))
        elif action == 'stat':
            r = requests.get(credentials['endpoint'] + '/' + request_id,
                             auth=(credentials['username'], credentials['password']))
        elif action == 'get':
            r = requests.get(credentials['endpoint'] + '/' + request_id + '/results',
                             auth=(credentials['username'], credentials['password']))
        elif action == 'del':
            r = requests.patch(credentials['endpoint'] + '/' + request_id,
                               auth=(credentials['username'], credentials['password']))
    except Exception as ce:
        print("\n\033[1;33mA {} occurred\x1b[0m\n".format(type(ce).__name__))
        print(ce, '\n')
        exit(3)

    if r.status_code == 200 or r.status_code == 201:
        if action == 'list':
            for resp in r.json()['requests']:
                pp.pprint(resp)
                print()
        elif action == 'stat':
            pp.pprint(r.json()['request'])
        elif action == 'get':
            print(r.text)
        elif action == 'del':
            print('Result = {}'.format(r.status_code))
            pp.pprint(r.json())
    else:
        print('Something went wrong! (Error = {})'.format(r.status_code))
        pp.pprint(r.reason)

    # All done


if __name__ == '__main__':
    # Run the main process
    main()
