#! /usr/bin/env python3

###############################################################################
import sys
import os
import argparse
import textwrap
import logging
import tempfile

import duckdb
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
                        Utility for processing internet.nl json batch measurement results and storing them in different formats
                        supported formats are duckdb database, CSV or xlsx file.

                        '''),
        # epilog=textwrap.dedent('''\
        #                 \033[1mSome examples:\033[0m
        #
        #                 \033[3m./%(prog)s measurements xlsx measurement.json\033[0m
        #                 processes the measurement results from the \033[3mmeasurement.json\033[0m file, storing the results in \033[3mmeasurements.xlsx\033[0m
        #                 with the sheet name either \033[3mweb\033[0m or \033[3mmail\033[0m depending on whether the json file contains web or mail results
        #
        #                 \033[3m./%(prog)s measurements csv measurement.json\033[0m
        #                 processes the measurement results from the \033[3mmeasurement.json\033[0m file, storing the results in either
        #                 \033[3mmeasurements-web.csv\033[0m or \033[3mmeasurements-mail.csv\033[0m depending on whether the json file contains web or mail results
        #
        #                 \033[3m./%(prog)s measurements duckdb measurement.json\033[0m
        #                 processes the measurement results from the \033[3mmeasurement.json\033[0m file, storing the results in \033[3mmeasurements.duckdb\033[0m
        #                 in the table \033[3mweb\033[0m or \033[3mmail\033[0m depending on whether the json file contains web or mail results
        #
        #                 \033[3m./%(prog)s measurements.duckdb measurement.json -d domains/domains.xlsx\033[0m
        #                 same, but fetches additional metadata from the (default) \033[3mtype\033[0m column in the \033[3mdomains/domains.xlsx\033[0m file
        #
        #                 \033[3m./%(prog)s measurements.duckdb measurement.json -d domains/domains.xlsx -m type,name\033[0m
        #                 same, but fetches additional metadata from the \033[3mtype\033[0m and \033[3mname\033[0m columns in the \033[3mdomains/domains.xlsx\033[0m file
        #
        #                 \033[3m./%(prog)s measurements.duckdb measurements/ -d all.xlsx -m type,name,province\033[0m
        #                 processes all the .json files in the \033[3mmeasurements\033[0m directory and fetches additional metadata from the \033[3mtype,
        #                 name\033[0m and \033[3mprovince\033[0m columns in the \033[3mall.xlsx\033[0m file, storing the results in the \033[3mmeasurements.duckdb\033[0m database,
        #                 storing the results in the \033[3mweb\033[0m and/or \033[3mmail\033[0m table(s)
        #                 '''),
        formatter_class=argparse.RawTextHelpFormatter, )

    # usage: process-results.py [-h] [-d FILE] [-m col[,col1,...]] [-s [sheet_name]] [-r] [-v] [--debug] [-V] {json file|json dir} {csv|xlsx|duckdb} outputfile
    # parser.usage = "process.py {json file|json dir} {csv|xlsx|duckdb} outputfile [-d FILE] [-m col[,col1,...]] [-i]"
    parser.usage = "process.py {json file|json dir} outputfile [-d FILE] [-m col[,col1,...]] [-i]"

    parser.add_argument("filename",
                        metavar='{json file|json dir}',
                        help=textwrap.dedent('''\
                        the json file, or directory containing json files, with the measurement
                        results to process
                        
                        '''),
                        action="store",
                        )

    # parser.add_argument("filetype",
    #                     metavar='{csv|xlsx|duckdb}',
    #                     help=textwrap.dedent('''\
    #                     type of \033[3moutputfile\033[0m(s) to produce
    #
    #                     '''),
    #                     choices=['csv', 'xlsx', 'duckdb'],
    #                     action="store",
    #                     )

    parser.add_argument("outputfile",
                        help=textwrap.dedent('''\
                        filename (with extension) to store the results in. The extension defines the 
                        type of file. Supported formats are duckdb, csv or xlsx.
                        '''),
                        )

    parser.add_argument("-d",
                        metavar='FILE',
                        help=textwrap.dedent('''\
                        the domains xlsx file to use for getting additional domain metadata to store in
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

    parser.add_argument("-m",
                        help=textwrap.dedent('''\
                        the column name or names in \033[3mFILE\033[0m to use for additional domain metadata to store
                        in the database for each domain processed. (default: \033[3mtype\033[0m).
                        
                        '''),
                        action="store",
                        metavar='col[,col1,...]',
                        default='type')

    parser.add_argument("-s",
                        metavar='[sheet_name]',
                        help=textwrap.dedent('''\
                        the name of the sheet in FILE to use (default is the first sheet)
                        
                        '''),
                        action="store")

    parser.add_argument("-i",
                        help=textwrap.dedent('''\
                        Process the json file(s) individually rather than combining them in a single
                        output file. The outputfile argument is ignored in this case. The filename(s) of
                        the output file(s) are equal to the input files with the different extension
                        depending on specified type of output file.
                        
                        As a side effect this option does raw processing of the json file(s), meaning
                        that all categories and tests are processed rather than just a fixed set. 

                        '''),
                        action="store_true")

    parser.add_argument("-e",
                        help=textwrap.dedent('''\
                        Export the duckdb database as a directory as well (in directory outputfile.duckdb.d)
                        useful when your duckdb cli is a different version than the one used by the python venv 
                        and refuses to open the outputfile.duckdb file. simply open duckdb cli and import the 
                        database with:  IMPORT DATABASE 'outputfile.duckdb.d'; 

                        '''),
                        action="store_true")

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

    return parser


# ------------------------------------------------------------------------------
class CustomConsoleFormatter(logging.Formatter):
    """
        Log facility format
    """

    def format(self, record):
        # info = '\033[0;32m'
        info = '\t'
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

    # Create empty dataframe
    domains = pd.DataFrame()
    domainsFile = ''
    columns_to_add = []
    sheet_name = 0

    if args.d:
        domainsFile = args.d
        logger.info("taking domains metadata from {}".format(domainsFile))

    if args.m:
        columns_to_add = args.m.split(',')
    logger.info("metadata columns to add: {}".format(columns_to_add))

    if args.s:
        sheet_name = args.s


    filelist = []
    filename = args.filename

    if len(args.outputfile.split('.')) < 2:
        logger.error("No extension specified for the output file")
        parser.print_help(sys.stderr)
        exit(1)

    args.filetype = args.outputfile.split('.')[-1].lower()
    if args.filetype not in ['xlsx','csv','duckdb']:
        logger.error("Only xlsx, csv or duckdb formats are supported")
        parser.print_help(sys.stderr)
        exit(1)
    outputfile = ".".join(args.outputfile.split('.')[:-1])

    if os.path.isdir(filename):
        if not filename.endswith("/"):
            filename = filename + '/'
        with os.scandir(filename) as it:
            for entry in it:
                if not entry.name.startswith('.') and entry.is_file() and entry.name.endswith('.json'):
                    filelist.append('{0}{1}'.format(filename, entry.name))
    else:
        filelist.append(filename)

    logger.debug("filelist to process: {}".format(filelist))

    try:
        if not domainsFile == '':
            # Check if it exists first
            if os.path.isfile(domainsFile):
                domains = pd.read_excel(domainsFile, sheet_name=0, engine='openpyxl')
    except Exception as e:
        logger.error("error processing domains Excel file: {}".format(e))
        exit(1)

    filelist.sort()

    if args.i:
        for fn in filelist:
            data = ut.openJSON(fn)
            mt = ut.getMeasurementType(data)
            outputfile = ''.join(fn.split('.')[:-1])
            result = ut.JSONtoCSVall(data, domains, columns_to_add)
            # print(result['header'] + '\n' + result['body'])
            csv = result['header'] + '\n' + result['body']

            if args.filetype == 'csv':
                print("Creating {}.csv".format(outputfile))
                file = open("{}.csv".format(outputfile), 'w')
                print(csv, file=file)
                file.close()
            elif args.filetype == 'xlsx':
                print("Creating {}.xlsx file".format(outputfile))
                tmpfile, tmpfilename = tempfile.mkstemp()
                with open(tmpfile, 'w') as f:
                    print(csv, file=f)
                df = pd.read_csv(tmpfilename)
                df.sort_values(by=['submit_date', 'score', 'domain'], ascending=False, inplace=True)
                df.to_excel("{}.xlsx".format(outputfile), index=None, header=True)

            elif args.filetype == 'duckdb':
                duckdb_file = "{}.duckdb".format(outputfile)
                if os.path.isfile(duckdb_file):
                    print("Removing {}".format(duckdb_file))
                    os.remove(duckdb_file)
                print("Creating {}.duckdb file".format(outputfile))
                con = duckdb.connect(database=duckdb_file, read_only=False)
                print("creating table {}".format(mt))
                tmpfile, tmpfilename = tempfile.mkstemp()
                with open(tmpfile, 'w') as f:
                    print(csv, file=f)

                logger.info("CREATE TABLE {} AS SELECT * FROM read_csv_auto('{}')".format(mt, tmpfilename))
                con.execute("CREATE TABLE {} AS SELECT * FROM read_csv_auto('{}')".format(mt, tmpfilename))

                con.close()
    else:
        # Split in mail and web files
        files = {'mail': [], 'web': []}
        for fn in filelist:
            data = ut.openJSON(fn)
            files[ut.getMeasurementType(data)].append(fn)

        csvs = {'mail': [], 'web': []}

        for mt in ['web', 'mail']:
            header = []
            body = []
            for fn in files[mt]:
                data = ut.openJSON(fn)
                logger.info("Processing {}".format(fn))
                result = ut.JSONtoCSV(data, domains, columns_to_add)
                header.append(result['header'])
                body.append(result['body'])

            if len(header) > 0:
                body = '\n'.join(body)
                header = header[0]
                print("Processed {} {} measurements from {} files".format(body.count('\n')+1, mt, len(files[mt])))
                csvs[mt].append(header + '\n' + body)

        if args.filetype == 'csv':
            for mt in ['web', 'mail']:
                if len(csvs[mt]) > 0:
                    print("\tcreating to {}-{}.csv".format(outputfile, mt))
                    file = open("{}-{}.csv".format(outputfile, mt), 'w')
                    print(csvs[mt][0], file=file)
                    file.close()
        elif args.filetype == 'xlsx':
            print("Creating {}.xlsx file".format(outputfile))
            with pd.ExcelWriter('{}.xlsx'.format(outputfile)) as writer:
                for mt in ['web', 'mail']:
                    if len(csvs[mt]) > 0:
                        tmpfile, tmpfilename = tempfile.mkstemp()
                        with open(tmpfile, 'w') as f:
                            print(csvs[mt][0], file=f)
                        df = pd.read_csv(tmpfilename)
                        df.sort_values(by=['submit_date', 'score', 'domain'], ascending=False, inplace=True)
                        print("\tcreating sheet {}".format(mt))
                        df.to_excel(writer, sheet_name=mt,index=None, header=True)

        elif args.filetype == 'duckdb':
            duckdb_file = "{}.duckdb".format(outputfile)
            if os.path.isfile(duckdb_file):
                print("Removing {}".format(duckdb_file))
                os.remove(duckdb_file)
            print("Creating {} file".format(duckdb_file))
            con = duckdb.connect(database=duckdb_file, read_only=False)
            for mt in ['web', 'mail']:
                if len(csvs[mt]) > 0:
                    print("\tcreating table {}".format(mt))
                    tmpfile, tmpfilename = tempfile.mkstemp()
                    with open(tmpfile, 'w') as f:
                        print(csvs[mt][0], file=f)
                    logger.info("CREATE TABLE {} AS SELECT * FROM read_csv_auto('{}')".format(mt, tmpfilename))
                    con.execute("CREATE TABLE {} AS SELECT * FROM read_csv_auto('{}')".format(mt, tmpfilename))

            if args.e:
                con.execute("EXPORT DATABASE '{}.d' (FORMAT CSV, DELIMITER '|')".format(duckdb_file))
            con.close()


if __name__ == '__main__':
    # Run the main process
    main()
