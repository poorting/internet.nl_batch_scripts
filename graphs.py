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
from math import pi
import duckdb

import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.font_manager import FontProperties
from matplotlib.colors import LinearSegmentedColormap

from bokeh.palettes import all_palettes

from lib import utils as ut

###############################################################################
# Program settings
VERBOSE, DEBUG = False, False
_graph_font = 'Inter'

program_name = os.path.basename(__file__)
VERSION = 0.2
logger = logging.getLogger(__name__)

_mweb = False
_mmail = False
###############################################################################
# Nice looking color palettes for graphs
paletteR = list(reversed(all_palettes['OrRd'][9]))[1:-2]
paletteG = list(reversed(all_palettes['Greens'][9]))[1:-2]
paletteB = list(reversed(all_palettes['Blues'][9]))[1:-2]
paletteRG = list(reversed(all_palettes['Purples'][9]))[1:-2]
paletteBG = list(reversed(all_palettes['GnBu'][9]))[1:-2]
paletteBR = list(reversed(all_palettes['PuRd'][9]))[1:-2]
paletteOrg = list(reversed(all_palettes['Oranges'][9]))[1:-2]
paletteSector = all_palettes['Set2'][6]
paletteHeatmap = ["#c10000", "#00a100"]
paletteDelta = ["#ff0000", "#c10000", "#00a100", "#00ff00"]

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

    parser.add_argument("-f",
                        help=textwrap.dedent('''\
                            Specify the font to use (e.g. Helvetica)
                             '''),
                        action="store",
                        metavar='FONT')

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

    sns.set_style('ticks')

    barWidth = 0.90
    # Number of bars as gap in between categories
    cat_gap = 1

    # Assume first column are the periods
    df = df.set_index(df.columns[0])

    categories = list(df.columns)
    # print("Categories ({}): {}".format(len(categories), categories))

    periods = list(df.index.values)
    # print("Periods ({}): {}".format(len(periods), periods))

    nr_of_bars = len(periods) * len(categories)

    # Create a linear color map from the palette given
    # to avoid overrunning the palette
    segments = len(periods)
    my_cmap = LinearSegmentedColormap.from_list('Custom', palette, segments)

    # if (df.columns)
    plt.figure(figsize=(3 + (len(df.columns)*len(df))/5.5, 8))
    ax = plt.subplot()
    ax.set_title(title, fontname=_graph_font, fontsize='large', y=1.05)
    ax.set_ylabel('score/percentage',  fontname=_graph_font, fontsize='medium', loc='center')
    ax.set_xlabel('category',  fontname=_graph_font, fontsize='medium', loc='center', labelpad=15.0)
    ax.spines['bottom'].set_linewidth(0.5)
    ax.spines['left'].set_linewidth(0.5)

    ax.set_ylim(0,100)
    loc = matplotlib.ticker.MultipleLocator(base=10)
    ax.yaxis.set_major_locator(loc)
    ax.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(2))
    plt.tick_params(axis='y', which='minor', direction='out', length=3, width=0.5)
    plt.tick_params(axis='y', which='major', width=0.5, labelsize='small')
    plt.grid(which='major', axis='y', linestyle='dotted', linewidth=0.5, color='black', alpha=0.3)

    ax.xaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(len(periods)+1))
    ax.xaxis.set_major_locator(matplotlib.ticker.MultipleLocator(len(periods)+cat_gap))
    plt.tick_params(axis='x', which='minor', direction='out', length=0, width=0.5, rotation=90, labelsize='x-small' )
    plt.tick_params(axis='x', which='major', direction='out', length=0, width=0.5, labelsize='small' )

    plt.xticks(fontname=_graph_font)
    plt.yticks(fontname=_graph_font)

    for i in range(0, len(periods)):
        rbars = range(i+1, nr_of_bars+cat_gap*len(categories)+1, len(periods)+cat_gap)
        plt.bar(rbars,
                df.iloc[i, :].tolist(),
                width=barWidth,
                color=my_cmap(i),
                edgecolor=(1,1,1,1),
                linewidth=1,
                label=periods[i],
                zorder=2,
                )
        # Plot the values on top
        for j, r in enumerate(rbars):
            plt.text(x=r-0.2, y=df.iloc[i,j] + 1.5, s=str(int(df.iloc[i,j])),
                     fontname=_graph_font, fontweight='normal', fontsize='small', rotation='vertical')

    barsx=[]
    for i in range(0, len(categories)):
        barsx.append(i*(len(periods)+cat_gap) + len(periods)/2 + 0.5)

    xticks = categories
    if len(df) > 3:
        plt.xticks(barsx, xticks, rotation='horizontal',  fontname=_graph_font)
    else:
        plt.xticks(barsx, xticks, rotation='vertical',  fontname=_graph_font)

    leg = plt.legend(prop={'family': _graph_font}, framealpha=0.5, edgecolor='grey')
    for line in leg.get_lines():
        line.set_linewidth(7)

    barslots = (len(periods)+cat_gap)*len(categories) - cat_gap
    plt.margins(x = 0.51/barslots  )
    ax.set_xlim(0, barslots + 1)

    plt.tight_layout()
    sns.despine()
    # plt.show()

    return ax


# ------------------------------------------------------------------------------
def createHeatmap(df, title='',incsign=False):

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
    # df1 = pd.DataFrame(df.stack(), columns=['passed']).reset_index()

    myColors = ((1.0, 0.0, 0.0, 1.0), (0.75, 0.0, 0.0, 1.0), (0.0, 0.65, 0.0, 1.0), (0.0, 1.0, 0.0, 1.0))
    my_cmap = LinearSegmentedColormap.from_list('Custom', myColors, len(myColors))

    plt.figure(figsize=(2+len(df.columns)/2, len(df)/2 + 1.75))
    plt.title(title, fontsize='large')

    # plot a heatmap with custom grid lines
    ax = sns.heatmap(df,
                     linewidths=0.5,
                     linecolor='white',
                     square=True,
                     cbar=False,
                     annot=False,
                     cmap=my_cmap,
                     # cmap='RdYlGn',
                     vmin=-1, vmax=2)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    ax.xaxis.set_ticks_position('top')
    ax.set_ylabel('')
    ax.set_xlabel('')
    ax.set_aspect('equal')

    plt.tick_params(axis='y', which='major', width=1, length=0, labelsize='medium' )
    plt.tick_params(axis='x', which='major', width=1, length=5, labelsize='medium')

    plt.tight_layout()

    return ax


# ------------------------------------------------------------------------------
def createSpiderPlot(df, title='', fill=False, palette=paletteSector):

    pp = pprint.PrettyPrinter(indent=4)

    sns.set_style('dark')

    # pp.pprint(df)

    categories = list(df.columns)[1:]
    N = len(categories)

    # What will be the angle of each axis in the plot? (we divide the plot / number of variable)
    angles = [n / float(N) * 2 * pi for n in range(N)]
    angles += angles[:1]

    # Initialise the spider plot
    plt.figure(figsize=(8, 8))
    ax = plt.subplot(polar=True)
    plt.title(title, y=1.1, fontsize=15, fontname=_graph_font)

    # If you want the first axis to be on top:
    ax.set_theta_offset(pi / 2)
    # If you want labels to go clockwise (counterclockwise is default)
    ax.set_theta_direction(direction='clockwise')

    # Draw one axe per variable + add labels
    plt.xticks(angles[:-1], categories)

    # Draw ylabels
    ticks = [i for i in range(10, 110, 10)]
    tick_labels = ["{}".format(i) for i in range(10, 110, 10)]
    ax.set_rlabel_position(0)
    plt.yticks(ticks, tick_labels, color="grey", size=9)
    plt.yticks(fontname=_graph_font)
    plt.ylim(0, 100)

    # md_types = list(df[type_col])
    md_types = list(df.iloc[:, 0])
    segments = len(md_types)
    my_cmap = LinearSegmentedColormap.from_list('Custom', palette, segments)

    for i, md_type in enumerate(md_types):
        values = df.iloc[i].drop(df.columns[0]).values.flatten().tolist()
        values += values[:1]
        ax.plot(angles, values, linewidth=1, linestyle='solid', label=md_type, color=my_cmap(i))
        if fill:
            ax.fill(angles, values, color=my_cmap(i), alpha=0.6)

    # More space between labels and plot itself
    rstep = int(ax.get_theta_direction())
    if rstep > 0:
        rmin = 0
        rmax = len(angles)
    else:
        rmin = len(angles)-1
        rmax = -1

    for label, i in zip(ax.get_xticklabels(), range(rmin, rmax, rstep)):
        angle_rad = angles[i] + ax.get_theta_offset()
        if angle_rad <= pi / 2:
            ha = 'left'
            va = "bottom"
        elif pi / 2 < angle_rad <= pi:
            ha = 'right'
            va = "bottom"
        elif pi < angle_rad <= (3 * pi / 2):
            ha = 'right'
            va = "top"
        else:
            ha = 'left'
            va = "top"
        label.set_verticalalignment(va)
        label.set_horizontalalignment(ha)

    plt.xticks(fontname=_graph_font)

    # Add legend
    leg = plt.legend(bbox_to_anchor=(1.14, 1.05), prop={'family': _graph_font, 'size': 'x-small'})
    leg.set_title(title=df.columns[0], prop={'family': _graph_font})
    for line in leg.get_lines():
        line.set_linewidth(7)
        line.set_solid_capstyle('butt')

    # plt.show()

    return ax


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

    plt.savefig(filename + '.svg', bbox_inches='tight')
    plt.savefig(filename + '.png', bbox_inches='tight')
    plt.close()


# ------------------------------------------------------------------------------
def scoreLastPeriod_type(context, db_con):

    pp = pprint.PrettyPrinter(indent=4)

    if not context['type']:
        return []

    print("Score last period per value of {} ({})".format(context['type'], context['end_period_str']))

    df_mw = []

    for tbl in context['tables']:
        query = "SELECT max(md_{4}) as '{4}', {1} from {0} where {2}=={3} and md_{4}!='<unknown>' group by md_{4} order by md_{4} desc".format(
            tbl, qry_items_score[tbl], context['period_col'], context['end_period'], context['type'])
        df_mw.append(db_con.execute(query).fetchdf())

    df = df_mw[0]
    if len(df_mw)>1:
        df_mw[1].drop(context['type'], axis=1, inplace=True)
        df = pd.concat(df_mw, axis=1)

    print("\tCreating bar graph")
    title = 'Results per {} ({})'.format(context['type'], context['end_period_str'])
    filename = "{}/Scores-overall-per-{}".format(context['output_dir'], context['type'])
    p = createBarGraph(df, title=title, palette=paletteSector)
    plt.savefig(filename + '.svg', bbox_inches='tight')
    plt.savefig(filename + '.png', bbox_inches='tight')
    plt.close()

    # SPIDER PLOT
    print("\tCreating spider plot")
    # if 'score(web)' in df.columns:
    #     df.drop('score(web)', axis=1, inplace=True)
    # if 'score(mail)' in df.columns:
    #     df.drop('score(mail)', axis=1, inplace=True)
    title = "Results per {} ({})".format(context['type'], context['end_period_str'])

    createSpiderPlot(df, title=title, palette=paletteSector, fill=False)
    filename = "{}/Spiderplot-per-{}".format(context['output_dir'], context['type'])
    plt.savefig(filename + '.svg', bbox_inches='tight')
    plt.savefig(filename + '.png', bbox_inches='tight')
    plt.close()

    # See if we can make individual plots for every type
    # pp.pprint(df)
    for i in range(0, len(df)):
        df1 = df.iloc[i,:].to_frame().transpose()
        type = df.iloc[i,0]
        print("\tCreating spider plot ({})".format(type))
        title = "Results for {} ({})".format(type, context['end_period_str'])
        pal_type = ('#808080', paletteSector[i % len(paletteSector)])
        createSpiderPlot(df1, title=title, palette=pal_type, fill=True)
        filename = "{}/Spiderplot-{}".format(context['output_dir'], type)
        plt.savefig(filename + '.svg', bbox_inches='tight')
        plt.savefig(filename + '.png', bbox_inches='tight')
        plt.close()


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

        plt.savefig(filename + '.png', bbox_inches='tight')
        plt.savefig(filename + '.svg', bbox_inches='tight')


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

        # Multiply everything but the score by 2 for the latest one
        # Then deduct the previous one from that
        # This will give possible 'passed' values of:
        #
        #  prev latest*2    l*2-p   Meaning
        #   1      0         -1     Worse than previous
        #   0      0          0     Same as previous (0)
        #   1      1          1     Same as previous (1)
        #   0      1          2     Better than previous
        #
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

        # pp.pprint(df)
        # Find the top/bottom 3 and then extend with all rows with the same improvement/deterioration as the number 3
        # (otherwise nr. 4 may have same improvement as nr. 3, but be excluded on grounds of alphabetical ordering)
        top = 2  # first row is 0
        if df.iloc[top,0] > 0:
            while df.iloc[top, 0] == df.iloc[top+1,0]:
                top += 1

        bot = -3 # last row is -1
        if df.iloc[bot, 0] < 0:
            while df.iloc[bot, 0] == df.iloc[bot-1,0]:
                bot -= 1

        # Make domains a column again (otherwise Heatmap will fail)
        df.reset_index(level=0, inplace=True)

        dfTop = df.head(top+1).copy()
        dfBot = df.tail(abs(bot)).copy()

        print("\tDelta for all ({}, {}-{})".format(tbl, context['prev_period_str'], context['end_period_str']))
        p = createHeatmap(
            df,
            title='Delta ({0}, {1}-{2})'.format(tbl, context['prev_period_str'], context['end_period_str']),
            incsign=True)
        plt.savefig('{}/Delta-all-{}.png'.format(context['output_dir'], tbl), bbox_inches='tight')
        plt.savefig('{}/Delta-all-{}.svg'.format(context['output_dir'], tbl), bbox_inches='tight')

        print("\tTop {} ({}, {}-{})".format(len(dfTop),tbl, context['prev_period_str'], context['end_period_str']))
        p = createHeatmap(
            dfTop,
            title="Top {0} ({1}, {2}-{3})".format(len(dfTop), tbl, context['prev_period_str'], context['end_period_str']), incsign=True)
        plt.savefig('{}/Delta-top-{}.png'.format(context['output_dir'], tbl), bbox_inches='tight')
        plt.savefig('{}/Delta-top-{}.svg'.format(context['output_dir'], tbl), bbox_inches='tight')

        print("\tBottom {} ({}, {}-{})".format(len(dfBot), tbl, context['prev_period_str'], context['end_period_str']))
        p = createHeatmap(
            dfBot,
            title="Bottom {0} ({1}, {2}-{3})".format(len(dfBot), tbl, context['prev_period_str'], context['end_period_str']), incsign=True)
        plt.savefig('{}/Delta-bottom-{}.png'.format(context['output_dir'], tbl), bbox_inches='tight')
        plt.savefig('{}/Delta-bottom-{}.svg'.format(context['output_dir'], tbl), bbox_inches='tight')


# ------------------------------------------------------------------------------
def scoreLastPeriods_type(context, db_con):

    pp = pprint.PrettyPrinter(indent=4)

    if not context['type']:
        return []

    ret_dfs = []

    for i, metadata in enumerate(context['type_vals']):
        print("Score last periods for {} = {} ({}-{})".format(context['type'], metadata, context['start_period_str'], context['end_period_str']))
        df_mw = []

        for tbl in context['tables']:
            query = 'SELECT max({3}) as {3},{1} from {0} where {2}>={4} and {2}<={5} and md_{6}=\'{7}\' group by {2} order by {2} asc'.format(
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
        plt.savefig(filename + '.svg', bbox_inches='tight')
        plt.savefig(filename + '.png', bbox_inches='tight')
        plt.close()

        ret_dfs.append({'name': "Scores-{}".format(metadata), 'df': df})

    return ret_dfs


# ------------------------------------------------------------------------------
def detailLastPeriod_type(context, db_con):

    pp = pprint.PrettyPrinter(indent=4)

    if not context['type']:
        return []

    for metadata in context['type_vals']:
        print("Details last periods for {} = {} ({})".format(context['type'], metadata, context['end_period_str']))

        for tbl in context['tables']:
            query = "SELECT {1} from {0} where {2}={3} and md_{4}='{5}' order by score desc, domain asc".format(
                    tbl, qry_items_detail[tbl], context['period_col'], context['end_period'],
                    context['type'], metadata)
            df = db_con.execute(query).fetchdf()

            title = 'Details {} ({}, {})'.format(tbl, metadata, context['end_period_str'])
            filename = "{}/Details-{}-{}".format(context['output_dir'], metadata, tbl)
            p = createHeatmap(df, title=title)

            plt.savefig(filename+'.png', bbox_inches='tight')
            plt.savefig(filename+'.svg', bbox_inches='tight')


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
    if "md_{}".format(args.m) in cols:
        context['type'] = args.m
        q_str = "select distinct(md_{0}) as type from {1} where md_{0}!='<unknown>' order by md_{0} desc".format(
            context['type'], context['tables'][0])
        arr = con.execute(q_str).fetchnumpy()
        context['type_vals'] = list(arr['type'])
    else:
        print("No metadata '{0}' found. Not creating graphs based on '{0}'.".format(args.m))

    return context


###############################################################################
def main():
    # Set global settings according to command line arguments
    global VERBOSE, DEBUG, logger, _mweb, _mmail, _graph_font

    parser = parser_add_arguments()
    args = parser.parse_args()

    VERBOSE = args.verbose
    DEBUG = args.debug
    logger = get_logger(args)

    pp = pprint.PrettyPrinter(indent=4)

    # change font

    sns.set()
    # sns.set_style('ticks')
    # sns.set_style('dark')

    matplotlib.rcParams['font.family'] = "sans-serif"

    # Prefer Helvetica or Helvetica Neue if available
    plt.rcParams['font.sans-serif'].insert(0, 'Helvetica Neue')
    plt.rcParams['font.sans-serif'].insert(0, 'Helvetica')

    if args.f:
        plt.rcParams['font.sans-serif'].insert(0, args.f)

    for font in plt.rcParams['font.sans-serif']:
        try:
            font_path = fm.findfont(font, fallback_to_default=False, rebuild_if_missing=True)
            _graph_font = font
            print("Selected font: {}".format(font))
            break
        except ValueError as ve:
            if args.f:
                print(ve)
            continue

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

    scoreLastPeriods(context, con)
    scoreLastPeriod_type(context, con)
    scoreLastPeriods_type(context, con)

    detailLastPeriod(context, con)
    detailLastPeriod_type(context, con)

    if context['prev_period']:
        deltaToPrevious(context, con)

    con.close()


if __name__ == '__main__':
    # Run the main process
    main()
