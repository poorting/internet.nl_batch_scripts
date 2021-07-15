#! /usr/bin/env python3

###############################################################################
import sys
import os
import logging
import argparse
from argparse import RawTextHelpFormatter
import textwrap

import pandas as pd
import pprint
import math
import duckdb

from bokeh.io import show, output_file, export_svgs, export_png
from bokeh.models import ColumnDataSource, FactorRange, ranges, LabelSet, LinearColorMapper
from bokeh.plotting import figure
from bokeh.transform import factor_cmap
from bokeh.palettes import all_palettes
from bokeh.transform import transform

from lib import utils as ut

###############################################################################
# Program settings
VERBOSE, DEBUG = False, False
program_name = os.path.basename(__file__)
VERSION = 0.2
logger = logging.getLogger(__name__)

_mweb = False
_mmail = False
###############################################################################
# Nice looking color palettes for graphs
paletteR = list(reversed(all_palettes['OrRd'][9]))[1:]
paletteG = list(reversed(all_palettes['Greens'][9]))[1:]
paletteB = list(reversed(all_palettes['Blues'][9]))[1:]
paletteRG = list(reversed(all_palettes['Purples'][9]))[1:]
paletteBG = list(reversed(all_palettes['GnBu'][9]))[1:]
paletteBR = list(reversed(all_palettes['PuRd'][9]))[1:]
paletteOrg = list(reversed(all_palettes['Oranges'][9]))[1:]
paletteSector = all_palettes['Set2'][8]
paletteHeatmap = ["#c10000", "#00a100"]
paletteTop5 = ["#ff0000", "#c10000", "#00a100", "#00ff00"]

type_palettes = [paletteR, paletteRG, paletteBG, paletteOrg, paletteB, paletteG]

qry_items_score = {
    'web': ' round(avg(score)) as "score(web)", round(avg(web_ipv6*100)) AS "IPv6 (web)", '
           'round(avg(web_dnssec*100)) AS "DNSSEC (web)", round(avg(web_https*100)) AS "TLS" ',
    'mail': ' round(avg(score)) as "score(mail)", round(avg(mail_ipv6*100)) AS "IPv6 (mail)", '
            'round(avg(mail_dnssec*100)) AS "DNSSEC (mail)", round(avg(mail_starttls_tls_available*100)) AS "STARTTLS",'
            'round(avg(mail_auth_spf_policy*100)) as "SPF", round(avg(mail_auth_dkim_exist*100)) AS "DKIM", '
            'round(avg(mail_auth_dmarc_policy*100)) AS "DMARC" ',
}

qry_items_detail = {
    'web': ' domain, score, web_ipv6 as "IPv6", web_dnssec AS "DNSSEC", web_https AS "TLS" ',
    'mail': ' domain, score, mail_ipv6 AS "IPv6", mail_dnssec AS "DNSSEC", mail_starttls_tls_available AS "STARTTLS",'
            'mail_auth_spf_policy as "SPF", mail_auth_dkim_exist AS "DKIM", mail_auth_dmarc_policy AS "DMARC" ',
}

###############################################################################
#
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
                        Description goes here
                        '''),
        epilog=textwrap.dedent('''\
                        Example: ./%(prog)s -q -n 3 DB.duckdb
                        
                        Creates graphs of the latest 3 quarters present in the data from DB.duckdb'''),
        formatter_class=RawTextHelpFormatter,
    )

    parser.add_argument("database",
                        help="file name of the duckdb database to use")

    parser.add_argument("output_dir",
                        help="directory where the graphs should be saved",
                        )

    parser.add_argument('periods', type=int, nargs='?', metavar='N', default=5,
                        help='number of measurement runs to show (default: 5)', )

    parser.add_argument("-q", "--quarters",
                        help=textwrap.dedent('''\
                        create graphs of the last [N] quarters instead of
                        the (default) last [N] months
                             '''),
                        action="store_true")

    parser.add_argument("-m",
                        help=textwrap.dedent('''\
                            creates separate and combined graphs for values of
                            this metadata field in the database (default: type)
                             '''),
                        action="store",
                        metavar='TYPE',
                        default='type')
    parser.add_argument("-v", "--verbose",
                        help="more verbose output",
                        action="store_true")
    parser.add_argument("-d", "--debug",
                        help="print debug info",
                        action="store_true")
    parser.add_argument("-V", "--version",
                        help="print version and exit",
                        action="version",
                        version='%(prog)s (version {})'.format(VERSION))

    # group = parser.add_mutually_exclusive_group()
    # group.add_argument ...

    # parser.usage = "... {}".format(" ".join((parser.format_usage().split(' ')[1:])))
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
    """
    Instanciate logging facility. By default, info logs are also
    stored in the logfile.
    param: cmd line args
    """
    logger = logging.getLogger(__name__)

    # Create handlers
    console_handler = logging.StreamHandler()
    #    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter = CustomConsoleFormatter()
    console_handler.setFormatter(formatter)

    if args.debug:
        logger.setLevel(logging.DEBUG)
    elif args.verbose:
        logger.setLevel(logging.INFO)

    # add handlers to the logger
    logger.addHandler(console_handler)

    return logger


# ------------------------------------------------------------------------------
def createBarGraph(df, title=' ', y_label='score/percentage', label_suffix='', palette=paletteR):
    pp = pprint.PrettyPrinter(indent=4)

    # All but first column are the categories
    categories = list(df.columns)[1:]
    # pp.pprint("categories: {}".format(categories))
    # Convert every value to an int
    for cat in categories:
        df = df.astype({cat: int})

    # Content of the first column are the subcategories per category
    subcats = list(df.iloc[:, 0])
    # pp.pprint("subcategories: {}".format(subcats))
    x = [(category, subcat) for category in categories for subcat in subcats]

    values = []
    for cat in categories:
        values.extend(df[cat].tolist())

    # pp.pprint("values: {}".format(values))

    value_labels = []
    for value in values:
        value_labels.append(str(value) + label_suffix)

    # pp.pprint("value_labels: {}".format(value_labels))

    source = ColumnDataSource(data=dict(x=x, y=values, labels=value_labels))

    p = figure(x_range=FactorRange(*x), y_range=ranges.Range1d(start=0, end=105), y_minor_ticks=10,
               y_axis_label=y_label, plot_height=800, plot_width=1280, title=title, title_location='above',
               toolbar_location=None, tools="")
    # min_border_top

    labels = LabelSet(x='x', y='y', text='labels', level='glyph', x_offset=7, y_offset=5, angle=90, angle_units='deg',
                      source=source, render_mode='canvas', text_font_size="9pt")

    p.vbar(x='x', top='y', width=0.9, source=source, line_color="white",
           fill_color=factor_cmap('x', palette=palette, factors=subcats, start=1))

    p.add_layout(labels)

    p.x_range.range_padding = 0.05
    p.title.align = 'center'
    p.title.text_font_size = "12pt"
    p.title.text_font_style = "bold"
    p.xaxis.major_label_orientation = math.pi / 2
    p.xgrid.grid_line_color = None

    return p


# ------------------------------------------------------------------------------
def createHeatmap(df, title=' ', rangemin=0, rangemax=1, palette=paletteHeatmap, incsign=False):
    # Creeer een 'heat map' voor de individuele scores van de instellingen/domeinen
    pp = pprint.PrettyPrinter(indent=4)

    # De score tabel wordt achter het domein gevoegd (bv 'www.surf.nl (97)'), zodat de inhoud zelf alleen 0/1 (rood/groen) is.
    domains = list(df.iloc[:, 0])

    row = 0
    for domain in domains:
        score = df.iat[row, 1]
        lb = '('
        if incsign:
            if (score > 0):
                lb = '(+'

        df.iat[row, 0] = '{0} {1}{2})'.format(domain, lb, score)
        row = row + 1
    df.drop('score', axis=1, inplace=True)

    # Heatmap nu heeft domain als index (verticaal op heatmap)
    # De kolommen geven de categoriën

    df = df.set_index('domain')
    df.columns.name = 'category'

    # reshape to 1D array of 'passed' with a domain and category for each row.
    df1 = pd.DataFrame(df.stack(), columns=['passed']).reset_index()
    source = ColumnDataSource(df1)

    mapper = LinearColorMapper(palette=palette, low=rangemin, high=rangemax)

    lentxt = len(max(list(df.index), key=len))
    lencats = len(df.columns)
    width = 8 * lentxt + 25 * lencats + 20
    p = figure(title=title, x_range=list(df.columns), y_range=list(reversed(df.index)), x_axis_location="above",
               plot_width=width, plot_height=25 * len(df.index) + 150, tools="")

    p.rect(x='category', y='domain', width=1, height=1, source=source, line_color='#ffffff',
           fill_color=transform('passed', mapper))

    p.title.align = 'right'
    p.title.text_font_size = "10pt"
    p.title.text_font_style = "bold"

    p.axis.axis_line_color = None
    p.axis.major_tick_line_color = None
    p.axis.major_label_text_font_size = "10pt"
    p.axis.major_label_standoff = 0
    p.xaxis.major_label_orientation = 'vertical'

    return p


# ------------------------------------------------------------------------------
def getSectorPalette(types):
    sectors = {}
    for i in range(len(types)):
        sectors[types[i]] = type_palettes[i % len(type_palettes)]
    return sectors


# ------------------------------------------------------------------------------
def scoreLastPeriods(context, db_con):

    pp = pprint.PrettyPrinter(indent=4)

    ret_dfs = []

    print("Score last periods overall ({}-{})".format(context['start_period_str'], context['end_period_str']))

    df_mw = []

    for tbl in context['tables']:
        query = "SELECT max({3}) as {3},{1} from {0} where {2}>={4} and {2}<={5} group by {2} order by {2} asc".format(
            tbl, qry_items_score[tbl], context['period_col'], context['period_str_col'], context['start_period'], context['end_period'])
        df_mw.append(db_con.execute(query).fetchdf())

    # for df in df_mw:
    #     pp.pprint(df)

    df = df_mw[0]
    if len(df_mw)>1:
        df_mw[1].drop(context['period_str_col'], axis=1, inplace=True)
        df = pd.concat(df_mw, axis=1)

    # pp.pprint(df)

    title = 'Results overall ({}-{})'.format(context['start_period_str'], context['end_period_str'])
    filename="{}/Scores-overall".format(context['output_dir'])
    p = createBarGraph(df, title=title, palette=paletteBR)
    p.output_backend = "svg"
    export_svgs(p, filename=filename + ".svg")
    export_png(p, filename=filename + ".png")

    ret_dfs.append({'name': 'Scores-overall', 'df': df})
    return ret_dfs


# ------------------------------------------------------------------------------
def scoreLastPeriod_type(context, db_con):

    pp = pprint.PrettyPrinter(indent=4)

    if not context['type']:
        return []

    ret_dfs = []

    print("Score last period per {} ({})".format(context['type'], context['end_period_str']))

    df_mw = []

    for tbl in context['tables']:
        query = "SELECT max({4}) as '{4}', {1} from {0} where {2}=={3} group by {4} order by {4} asc".format(
            tbl, qry_items_score[tbl], context['period_col'], context['end_period'], context['type'])
        # print('\n'+query)
        df_mw.append(db_con.execute(query).fetchdf())

    df = df_mw[0]
    if len(df_mw)>1:
        df_mw[1].drop(context['type'], axis=1, inplace=True)
        df = pd.concat(df_mw, axis=1)

    title = 'Results per {} ({})'.format(context['type'], context['end_period_str'])
    filename = "{}/Scores-overall-per-{}".format(context['output_dir'], context['type'])
    p = createBarGraph(df, title=title, palette=paletteSector)
    p.output_backend = "svg"
    export_svgs(p, filename=filename + ".svg")
    export_png(p, filename=filename + ".png")

    ret_dfs.append({'name': 'Scores-overall-per-{}'.format(context['type']), 'df': df})
    return ret_dfs


# ------------------------------------------------------------------------------
def detailLastPeriod(context, db_con):

    pp = pprint.PrettyPrinter(indent=4)

    ret_dfs = []

    print("Details last periods overall ({})".format(context['end_period_str']))

    df_mw = []

    for tbl in context['tables']:
        query = "SELECT {1} from {0} where {2}={3} order by score desc, domain asc".format(
                tbl, qry_items_detail[tbl], context['period_col'], context['end_period'])
        df = db_con.execute(query).fetchdf()

        title = 'Details {} ({})'.format(tbl, context['end_period_str'])
        filename = "{}/Details-overall-{}".format(context['output_dir'], tbl)
        p = createHeatmap(df, title=title)
        p.output_backend = "svg"
        export_svgs(p, filename=filename + '.svg')
        export_png(p, filename=filename + '.png')


# ------------------------------------------------------------------------------
def deltaToPrevious(context, db_con):

    pp = pprint.PrettyPrinter(indent=4)

    ret_dfs = []

    print("Delta compared to previous period ({}-{})".format(context['prev_period_str'], context['end_period_str']))

    df_mw = []

    for tbl in context['tables']:
        query = "SELECT {1} from {0} where {2}={3} order by domain asc".format(
                tbl, qry_items_detail[tbl], context['period_col'], context['prev_period'])
        df1 = db_con.execute(query).fetchdf()

        query = "SELECT {1} from {0} where {2}={3} order by domain asc".format(
                tbl, qry_items_detail[tbl], context['period_col'], context['end_period'])
        df2 = db_con.execute(query).fetchdf()

        # Multiply everything but the score by 2 for the last one
        # Then deduct the first one from the second
        # This will give possible 'passed' values of:
        # 0->false, 1->was True and is True, 2-> was False but now True
        # for this to work we have to set the domain as index
        # (so they will be ignored for the subtraction)
        df1 = df1.set_index('domain')
        df2 = df2.set_index('domain')
        df2 = df2 * 2
        df2.score = round(df2.score / 2)
        df2 = df2.astype({"score": int})
        df = df2.subtract(df1)
        df['score'].fillna(0, inplace=True)
        df = df.astype({"score": int})

        # Now sort by score (descending), then domain (ascending)
        df.sort_values(by=['score', 'domain'], ascending=[False, True], inplace=True)
        # pp.pprint(df.head(5))
        # pp.pprint(df.tail(5))

        # Make domains a column again (otherwise Heatmap will fail)
        df.reset_index(level=0, inplace=True)
        dfTop = df.head(5).copy()
        dfBot = df.tail(5).copy()

        print("\tDelta for all ({}, {}-{})".format(tbl, context['prev_period_str'], context['end_period_str']))
        p = createHeatmap(df, palette=paletteTop5, rangemin=-1, rangemax=2,
                          title='Delta ({0}, {1}-{2})'.format(tbl, context['prev_period_str'], context['end_period_str']),
                          incsign=True)
        p.output_backend = "svg"
        export_svgs(p, filename='{}/Delta-all-{}.svg'.format(context['output_dir'], tbl))
        export_png(p, filename='{}/Delta-all-{}.png'.format(context['output_dir'], tbl))

        print("\tTop 5 ({}, {}-{})".format(tbl, context['prev_period_str'], context['end_period_str']))
        p = createHeatmap(dfTop, palette=paletteTop5, rangemin=-1, rangemax=2,
                          title="Top 5 \n({0}, {1}-{2})".format(tbl, context['prev_period_str'], context['end_period_str']),
                          incsign=True)
        p.output_backend = "svg"
        export_svgs(p, filename='{}/Delta-top-5-{}.svg'.format(context['output_dir'], tbl))
        export_png(p, filename='{}/Delta-top-5-{}.png'.format(context['output_dir'], tbl))

        print("\tBottom 5 ({}, {}-{})".format(tbl, context['prev_period_str'], context['end_period_str']))
        p = createHeatmap(dfBot, palette=paletteTop5, rangemin=-1, rangemax=2,
                          title="Last 5 \n({0}, {1}-{2})".format(tbl, context['prev_period_str'], context['end_period_str']),
                          incsign=True)
        p.output_backend = "svg"
        export_svgs(p, filename='{}/Delta-bottom-5-{}.svg'.format(context['output_dir'], tbl))
        export_png(p, filename='{}/Delta-bottom-5-{}.png'.format(context['output_dir'], tbl))


# ------------------------------------------------------------------------------
def scoreLastPeriods_type(context, db_con):

    pp = pprint.PrettyPrinter(indent=4)

    if not context['type']:
        return []

    ret_dfs = []

    for i, metadata in enumerate(context['type_vals']):
        print("Score last periods for {} ({}-{})".format(metadata, context['start_period_str'], context['end_period_str']))
        df_mw = []

        for tbl in context['tables']:
            query = 'SELECT max({3}) as {3},{1} from {0} where {2}>={4} and {2}<={5} and {6}=\'{7}\' group by {2} order by {2} asc'.format(
                tbl, qry_items_score[tbl], context['period_col'], context['period_str_col'], context['start_period'], context['end_period'],
                context['type'], metadata)
            df_mw.append(db_con.execute(query).fetchdf())

        df = df_mw[0]
        if len(df_mw)>1:
            df_mw[1].drop(context['period_str_col'], axis=1, inplace=True)
            df = pd.concat(df_mw, axis=1)

        # pp.pprint(df)

        title = 'Results {} ({}-{})'.format(metadata, context['start_period_str'], context['end_period_str'])
        filename = "{}/Scores-{}".format(context['output_dir'], metadata)
        p = createBarGraph(df, title=title, palette=type_palettes[i % len(type_palettes)])
        p.output_backend = "svg"
        export_svgs(p, filename=filename + ".svg")
        export_png(p, filename=filename + ".png")

        ret_dfs.append({'name': "Scores-{}".format(metadata), 'df': df})

    return ret_dfs


# ------------------------------------------------------------------------------
def detailLastPeriod_type(context, db_con):

    pp = pprint.PrettyPrinter(indent=4)

    if not context['type']:
        return []

    for metadata in context['type_vals']:
        print("Details last periods for {} ({})".format(metadata, context['end_period_str']))

        for tbl in context['tables']:
            query = "SELECT {1} from {0} where {2}={3} and {4}='{5}' order by score desc, domain asc".format(
                    tbl, qry_items_detail[tbl], context['period_col'], context['end_period'],
                    context['type'], metadata)
            df = db_con.execute(query).fetchdf()

            title = 'Details {} ({}, {})'.format(tbl, metadata, context['end_period_str'])
            filename = "{}/Details-{}-{}".format(context['output_dir'], metadata, tbl)
            p = createHeatmap(df, title=title)
            p.output_backend = "svg"
            export_svgs(p, filename=filename + '.svg')
            export_png(p, filename=filename + '.png')

            # df_mw.append(db_con.execute(query).fetchdf())


# ------------------------------------------------------------------------------
def get_Context(con, args):
    context = {
        'tables': [],
        'period_unit': 'month',
        'period_col': 'ym',
        'period_str_col': 'yearmonth',
        'start_period': 0,
        'end_period': 0,
        'start_period_str': '',
        'end_period_str': '',
        'type': None,
        'type_vals': [],
        'output_dir': None,
        'output_dir_is_file': False
    }

    tables = con.execute("show tables").fetchnumpy()

    out_dir = args.output_dir
    if out_dir.endswith('/'):
        out_dir = out_dir[:-1]
    if os.path.isdir(out_dir):
        context['output_dir'] = out_dir
    else:
        if os.path.isfile(out_dir):
            context['output_dir_is_file'] = True
        else:
            os.makedirs(out_dir, exist_ok=True)
            context['output_dir'] = out_dir

    if "web" in tables['name']:
        context['tables'].append('web')
    if "mail" in tables['name']:
        context['tables'].append('mail')

    if len(context['tables']) == 0:
        context['tables'] = None
        return context

    if args.quarters:
        context['period_unit'] = 'quarter'
        context['period_col'] = 'q'
        context['period_str_col'] = 'quarter'

    q_str = "SELECT DISTINCT({0}) FROM {1} WHERE {0}>0 ORDER BY {0} DESC".format(
        context['period_col'], context['tables'][0])
    arr = con.execute(q_str).fetchnumpy()
    periods_present = list(arr[context['period_col']])

    # TODO: Check on empty list!
    if len(periods_present) == 0:
        context['end_period'] = None
        return context

    context['end_period'] = periods_present[0]
    if len(periods_present) > 1:
        context['start_period'] = periods_present[-1]
        context['prev_period'] = periods_present[1]
        if len(periods_present) > args.periods:
            context['start_period'] = periods_present[args.periods - 1]
    else:
        context['start_period'] = periods_present[0]
        context['prev_period'] = None

    context['start_period_str'] = ut.Nr2Name(context['start_period'])
    context['end_period_str'] = ut.Nr2Name(context['end_period'])
    context['prev_period_str'] = ut.Nr2Name(context['prev_period'])

    q_str = "DESCRIBE {}".format(context['tables'][0])
    arr = con.execute(q_str).fetchnumpy()
    cols = list(arr['Field'])
    if args.m in cols:
        context['type'] = args.m
        q_str = "select distinct({0}) as type from {1} where {0}!='<unknown>' order by {0} desc".format(
            context['type'], context['tables'][0])
        arr = con.execute(q_str).fetchnumpy()
        context['type_vals'] = list(arr['type'])

    return context


###############################################################################
def main():
    # Set global settings according to command line arguments
    global VERBOSE, DEBUG, logger, _mweb, _mmail

    parser = parser_add_arguments()
    args = parser.parse_args()

    VERBOSE = args.verbose
    DEBUG = args.debug
    logger = get_logger(args)

    pp = pprint.PrettyPrinter(indent=4)

    database = args.database
    con = duckdb.connect(database=database, read_only=True)

    context = get_Context(con, args)

    # pp.pprint(context)
    if not context['end_period']:
        print("No {} found to display!".format(context['period_unit']))
        exit(1)

    if context['output_dir_is_file']:
        print("You cannot specify a file ({}) as an output directory!".format(args.output_dir))
        exit(2)

    ret_dfs = []
    ret_dfs.extend(scoreLastPeriod_type(context, con))
    ret_dfs.extend(scoreLastPeriods(context, con))
    ret_dfs.extend(scoreLastPeriods_type(context, con))

    detailLastPeriod(context, con)
    detailLastPeriod_type(context, con)

    if context['prev_period']:
        deltaToPrevious(context, con)

    con.close()

    # for df_lst in ret_dfs:
    #     pp.pprint(df_lst['name'])
    #     pp.pprint(df_lst['df'])
    #     print()


if __name__ == '__main__':
    # Run the main process
    main()