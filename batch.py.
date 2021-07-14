#! /usr/bin/env python3

###############################################################################
import sys
import os
import argparse
import logging
from   datetime import date

import requests
import textwrap
import pandas as pd
import pprint

from lib import utils as ut

###############################################################################
program_name = os.path.basename(__file__)
VERSION = 0.2
logger = logging.getLogger(__name__)


###############################################################################
class ArgumentParser(argparse.ArgumentParser):

    def error(self, message):
        print('\n\033[1;33mError: {}\x1b[0m\n'.format(message))
        print("invoke \033[1m{} -h\033[0m for help\n".format(os.path.basename(__file__)))
        # self.print_help(sys.stderr)
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

    date_now = date.today().strftime('%Y%m%d')

    parser = ArgumentParser(
        prog=program_name,
        description=textwrap.dedent('''\
                        Utility for interacting with the batch requests API of internet.nl

                        The following commands are supported:
                        
                        \033[1m sub\033[0m  - submit a new batch request 
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
                        '''),

                        # \033[1mSome examples:\033[0m
                        #
                        # \033[3m./%(prog)s\033[0m
                        # list the details of all the measurement batches
                        #
                        # \033[3m./%(prog)s list 4 \033[0m
                        # list the details of the last four measurement batches
                        #
                        # \033[3m./%(prog)s list 4 -p \033[0m
                        # same, but prompts you for username and password
                        #
                        # \033[3m./%(prog)s list 4 -p dev\033[0m
                        # same, takes configuration from \033[3m[dev]\033[0m section in \033[3mbatch-request.conf\033[0m
                        #
                        # \033[3m./%(prog)s stat 02e19b69317a4aa2958980312754de52\033[0m
                        # get the status of batch 02e19b69317a4aa2958980312754de52
                        #
                        # \033[3m./%(prog)s get 02e19b69317a4aa2958980312754de52\033[0m
                        # get the json results of batch 02e19b69317a4aa2958980312754de52
                        
        formatter_class=argparse.RawTextHelpFormatter, )

    parser.add_argument("command",
                        help=textwrap.dedent('''\
                            the request to execute (default: list)
                            
                        '''),
                        action="store",
                        # nargs="?",
                        choices=["sub", "list", "stat", "get", "del", ],
                        # default="list")
                        )

    parser.add_argument('parameter', nargs='?', metavar='parameter',
                        help=textwrap.dedent('''\
                        extra parameter for the request, type and meaning depend on the request:
                        
                        \033[1msub\033[0m          (required): \033[1m{web|mail}\033[0m the type of measurement to submit
                        \033[1mlist\033[0m         (optional): the number of items to list (default: 0 = all)
                        \033[1mstat,get,del\033[0m (required): the request_id for the \033[1mstat,get or del\033[0m request
                        '''))

    parser.add_argument("-d",
                        metavar='FILE',
                        help=textwrap.dedent('''\
                        the domains xlsx file to use for the sub request (mandatory) 
                        the domains file is used to get all the domains to be tested
                        the database for each processed domain. This is typically the same file as used
                        for submitting domains to internet.nl, containing a \033[3mweb\033[0m column with the domains
                        for a website test and a \033[3mmail\033[0m column with the domains for a mail test.

                        metadata will be retrieved from the column with the name \033[3mtype\033[0m, unless another
                        name or names are provided with the \033[3m-m\033[0m argument

                        web, mail and the metadata column(s) need to aligned, e.g. \033[3m'www.foo.org',
                        'foo.org', 'foo metadata'\033[0m must be in the same row in their respective web, mail
                        and metadata column.

                        '''),
                        action="store")

    parser.add_argument("-s",
                        metavar='[sheet_name]',
                        help=textwrap.dedent('''\
                        the name of the sheet in FILE to use. Only effective in combination with the 
                        \033[3m-d\033[0m argument for the \033[3msub\033[0m command 

                        '''),
                        action="store")

    parser.add_argument("-n",
                        metavar='name',
                        help=textwrap.dedent('''\
                        the name of the measurement submission request. Only effective in combination 
                        with the \033[3msub\033[0m command (default: use current date of {})

                        '''.format(date_now)),
                        action="store")


    parser.add_argument('-p',
                        action="store",
                        nargs="?",
                        const='',
                        metavar='SECTION',
                        help=textwrap.dedent('''\
                        get the credentials from the specified \033[3m[SECTION]\033[0m in the configuration file 
                        just the option without SECTION will prompt you for username and password
                        '''))

    parser.add_argument("-v", "--verbose",
                        help="more verbose output",
                        action="store_true")
    parser.add_argument("--debug",
                        help="show debug output",
                        action="store_true")
    parser.add_argument("-V", "--version",
                        help="print version and exit",
                        action="version",
                        version='%(prog)s (version {})'.format(VERSION))

    # parser.usage = "duckdb-to-graphs.py [-q] [-t TYPE] database [N]"

    return parser


# ------------------------------------------------------------------------------
class CustomConsoleFormatter(logging.Formatter):
    """
        Log facility format
    """

    def format(self, record):
        # info = '\033[0;32m'
        info = ''
        warning = '\033[0;33m'
        error = '\033[1;33m'
        debug = '\033[1;34m'
        reset = "\x1b[0m"

        # formatter = "%(levelname)s - %(message)s"
        formatter = "%(message)s"
        if record.levelno == logging.INFO:
            log_fmt = info + formatter + reset
            self._style._fmt = log_fmt
        elif record.levelno == logging.WARNING:
            log_fmt = warning + formatter + reset
            self._style._fmt = log_fmt
        elif record.levelno == logging.ERROR:
            log_fmt = error + formatter + reset
            self._style._fmt = log_fmt
        elif record.levelno == logging.DEBUG:
            # formatter = '%(asctime)s %(levelname)s [%(filename)s.py:%(lineno)s/%(funcName)s] %(message)s'
            formatter = '%(levelname)s [%(filename)s.py:%(lineno)s/%(funcName)s] %(message)s'
            log_fmt = debug + formatter + reset
            self._style._fmt = log_fmt
        else:
            self._style._fmt = formatter

        return super().format(record)


# ------------------------------------------------------------------------------
def get_logger(args):

    logger = logging.getLogger(__name__)

    # Create handlers
    console_handler = logging.StreamHandler()
    #    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter = CustomConsoleFormatter()
    console_handler.setFormatter(formatter)

    if args.verbose:
        logger.setLevel(logging.INFO)

    if args.debug:
        logger.setLevel(logging.DEBUG)

    # add handlers to the logger
    logger.addHandler(console_handler)

    return logger

###############################################################################
def main():
    global VERBOSE, DEBUG, logger

    parser = parser_add_arguments()

    args = parser.parse_args()

    VERBOSE = args.verbose
    DEBUG = args.debug

    logger = get_logger(args)

    pp = pprint.PrettyPrinter(indent=4)

    request_id = '0'
    domainsFile = None
    sheet_name = 0
    action = args.command
    parameter = args.parameter
    domains = pd.DataFrame()
    request_name = date.today().strftime('%Y%m%d')


    if args.d is not None:
        domainsFile = args.d
        if args.s is not None:
            sheet_name = args.s
        logger.info("taking domains from sheet {} in {}".format(sheet_name, domainsFile))

    if args.n is not None:
        request_name = args.n

    if action in ['stat', 'get', 'del']:
        if parameter is None:
            print('Please specify a valid request_id')
            exit(2)
        request_id = parameter
    elif action == 'sub':
        if parameter not in ['web', 'mail']:
            print('Please specify a valid type of measurement (either web or mail)')
            exit(2)

        if not domainsFile:
            print('Please specify a domains xlsx file with -d FILE')
            exit(2)

    elif action == 'list':
        if parameter is None:
            request_id = 0
        else:
            try:
                request_id = int(parameter)
                if request_id <= 0:
                    raise ValueError()
            except ValueError:
                print('\n\033[0;33mPlease specify a positive integer (>0) number as a parameter for the \033[1mlist\033[0;33m command\x1b[0m\n')
                exit(2)

    try:
        if domainsFile:
            # Check if it exists first
            if os.path.isfile(domainsFile):
                domains = pd.read_excel(domainsFile, sheet_name=sheet_name, engine='openpyxl')
    except Exception as e:
        logger.error("error processing domains Excel file: {}".format(e))
        exit(1)

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
        elif action == 'sub':
            # as name simply use the current date
            if parameter in domains.columns:
                submitDomains = {"name": request_name,
                                 "type": parameter,
                                 "domains": domains[parameter].dropna().tolist()}
                r = requests.post(credentials['endpoint'], json=submitDomains,
                                  auth=(credentials['username'], credentials['password']))
            else:
                print("There is no column named '{}' in sheet {} of {}".format(parameter, sheet_name, domainsFile))
                exit(0)

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
        elif action == 'sub':
            print('Result = {}'.format(r.status_code))
            pp.pprint(r.json())
    else:
        print('Something went wrong! (Error = {})'.format(r.status_code))
        pp.pprint(r.reason)

    # All done


if __name__ == '__main__':
    # Run the main process
    main()
