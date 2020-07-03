#! /usr/bin/env python3

from influxdb import InfluxDBClient, DataFrameClient
import json
import pandas as pd
import pprint
import math
import calendar

from bokeh.io import show, output_file, export_svgs, export_png
from bokeh.models import ColumnDataSource, FactorRange,  ranges, LabelSet, LinearColorMapper
from bokeh.plotting import figure
from bokeh.transform import factor_cmap
from bokeh.core.properties import AngleSpec
from bokeh.core.enums import AngleUnits
from bokeh.palettes import all_palettes
from bokeh.transform import transform

paletteR = list(reversed(all_palettes['OrRd'][9]))[1:]
paletteG = list(reversed(all_palettes['Greens'][9]))[1:]
paletteB = list(reversed(all_palettes['Blues'][9]))[1:]
paletteRG = list(reversed(all_palettes['Purples'][9]))[1:]
paletteBG = list(reversed(all_palettes['GnBu'][9]))[1:]
paletteBR = list(reversed(all_palettes['PuRd'][9]))[1:]
paletteOrg = list(reversed(all_palettes['Oranges'][9]))[1:]
paletteSector = all_palettes['Set2'][8]
paletteHeatmap = ["#c10000","#00a100"]
paletteTop5 = ["#ff0000", "#c10000","#00a100","#00ff00"]

sectors = {"WO":paletteR, "HBO":paletteG, "MBO":paletteB, "UMC":paletteRG, "Research+":paletteBG, "Overig":paletteOrg}

# Get all "type" values from measurement mail
# SHOW TAG VALUES ON "IV-metingen" FROM "mail" WITH KEY = "type"

# Query specific Q, grouped by X (rather than sector)
query_web_lastQ_X = 'SELECT round(mean("score")) AS "score (web)", round(mean("web_ipv6")*100) AS "IPv6 (web)", round(mean("web_dnssec")*100) AS "DNSSEC (web)", round(mean("web_https")*100) AS "TLS" FROM "IV-metingen"."autogen"."web" WHERE "q" = {0} GROUP BY "{1}" ORDER BY time ASC'
query_mail_lastQ_X = 'SELECT round(mean("score")) AS "score (mail)", round(mean("mail_ipv6")*100) AS "IPv6 (mail)", round(mean("mail_dnssec")*100) AS "DNSSEC (mail)", round(mean("mail_starttls_tls_available")*100) AS "STARTTLS", round(mean("mail_auth_spf_policy")*100) AS "SPF", round(mean("mail_auth_dkim_exist")*100) AS "DKIM", round(mean("mail_auth_dmarc_policy")*100) AS "DMARC" FROM "IV-metingen"."autogen"."mail" WHERE "q" = {0} GROUP BY "{1}" ORDER BY time ASC'


# Query last month, grouped by X (rather than sector)
query_web_lastMonth_X = 'SELECT round(mean("score")) AS "score (web)", round(mean("web_ipv6")*100) AS "IPv6 (web)", round(mean("web_dnssec")*100) AS "DNSSEC (web)", round(mean("web_https")*100) AS "TLS" FROM "IV-metingen"."autogen"."web" WHERE "ym" = {0} GROUP BY "{1}" ORDER BY time ASC'
query_mail_lastMonth_X = 'SELECT round(mean("score")) AS "score (mail)", round(mean("mail_ipv6")*100) AS "IPv6 (mail)", round(mean("mail_dnssec")*100) AS "DNSSEC (mail)", round(mean("mail_starttls_tls_available")*100) AS "STARTTLS", round(mean("mail_auth_spf_policy")*100) AS "SPF", round(mean("mail_auth_dkim_exist")*100) AS "DKIM", round(mean("mail_auth_dmarc_policy")*100) AS "DMARC" FROM "IV-metingen"."autogen"."mail" WHERE "ym" = {0} GROUP BY "{1}" ORDER BY time ASC'


# Query for specific Quarter for all domains
query_detail_web_Q = 'SELECT round("score") AS "score", round("web_ipv6") AS "IPv6", round("web_dnssec") AS "DNSSEC", round("web_https") AS "TLS" FROM "IV-metingen"."autogen"."web" WHERE "q" = {0} GROUP BY "domain"'
query_detail_mail_Q = 'SELECT round("score") AS "score", round("mail_ipv6") AS "IPv6", round("mail_dnssec") AS "DNSSEC", round("mail_starttls_tls_available") AS "STARTTLS", round("mail_auth_spf_policy") AS "SPF", round("mail_auth_dkim_exist") AS "DKIM", round("mail_auth_dmarc_policy") AS "DMARC" FROM "IV-metingen"."autogen"."mail" WHERE "q" = {0} GROUP BY "domain"'

# Query for specific month for all domains
query_detail_web_Month = 'SELECT round("score") AS "score", round("web_ipv6") AS "IPv6", round("web_dnssec") AS "DNSSEC", round("web_https") AS "TLS" FROM "IV-metingen"."autogen"."web" WHERE "ym" = {0} GROUP BY "domain"'
query_detail_mail_Month = 'SELECT round("score") AS "score", round("mail_ipv6") AS "IPv6", round("mail_dnssec") AS "DNSSEC", round("mail_starttls_tls_available") AS "STARTTLS", round("mail_auth_spf_policy") AS "SPF", round("mail_auth_dkim_exist") AS "DKIM", round("mail_auth_dmarc_policy") AS "DMARC" FROM "IV-metingen"."autogen"."mail" WHERE "ym" = {0} GROUP BY "domain"'


# Query for specific Quarter and sector (type), for all domains
query_detail_web_Q_Sector = 'SELECT round("score") AS "score", round("web_ipv6") AS "IPv6", round("web_dnssec") AS "DNSSEC", round("web_https") AS "TLS" FROM "IV-metingen"."autogen"."web" WHERE "q" = {0} AND "type"=\'{1}\' GROUP BY "domain"'
query_detail_mail_Q_Sector = 'SELECT round("score") AS "score", round("mail_ipv6") AS "IPv6", round("mail_dnssec") AS "DNSSEC", round("mail_starttls_tls_available") AS "STARTTLS", round("mail_auth_spf_policy") AS "SPF", round("mail_auth_dkim_exist") AS "DKIM", round("mail_auth_dmarc_policy") AS "DMARC" FROM "IV-metingen"."autogen"."mail" WHERE "q" = {0} AND "type"=\'{1}\' GROUP BY "domain"'

# Query details for specific month and sector (type)
query_detail_web_Month_Sector = 'SELECT round("score") AS "score", round("web_ipv6") AS "IPv6", round("web_dnssec") AS "DNSSEC", round("web_https") AS "TLS" FROM "IV-metingen"."autogen"."web" WHERE "ym" = {0} AND "type"=\'{1}\' GROUP BY "domain"'
query_detail_mail_Month_Sector = 'SELECT round("score") AS "score", round("mail_ipv6") AS "IPv6", round("mail_dnssec") AS "DNSSEC", round("mail_starttls_tls_available") AS "STARTTLS", round("mail_auth_spf_policy") AS "SPF", round("mail_auth_dkim_exist") AS "DKIM", round("mail_auth_dmarc_policy") AS "DMARC" FROM "IV-metingen"."autogen"."mail" WHERE "ym" = {0} AND "type"=\'{1}\' GROUP BY "domain"'


# Query last Quarters for all
query_web_lastQs = 'SELECT round(mean("score")) AS "score (web)", round(mean("web_ipv6")*100) AS "IPv6 (web)", round(mean("web_dnssec")*100) AS "DNSSEC (web)", round(mean("web_https")*100) AS "TLS" FROM "IV-metingen"."autogen"."web" WHERE "q" >= {0} AND "q" <= {1} GROUP BY "quarter" ORDER BY time ASC'
query_mail_lastQs = 'SELECT round(mean("score")) AS "score (mail)", round(mean("mail_ipv6")*100) AS "IPv6 (mail)", round(mean("mail_dnssec")*100) AS "DNSSEC (mail)", round(mean("mail_starttls_tls_available")*100) AS "STARTTLS", round(mean("mail_auth_spf_policy")*100) AS "SPF", round(mean("mail_auth_dkim_exist")*100) AS "DKIM", round(mean("mail_auth_dmarc_policy")*100) AS "DMARC" FROM "IV-metingen"."autogen"."mail" WHERE "q" >= {0} AND "q" <= {1} GROUP BY "quarter" ORDER BY time ASC'

# Query last Quarters for all, including tls_present, dmarc present, etc. (to compare with government)
query_web_lastQs_web = 'SELECT round(mean("web_dnssec")*100) AS "DNSSEC", round(mean("web_https_tls_version")*100) AS "TLS", round(mean("web_https")*100) AS "TLS cf NCSC", round(mean("web_https_http_redirect")*100) AS "HTTPS (forced)", round(mean("web_https_http_hsts")*100) AS "HSTS" FROM "IV-metingen"."autogen"."web" WHERE "q" >= {0} AND "q" <= {1} GROUP BY "quarter" ORDER BY time ASC'
#SELECT round(mean("dnssec")*100) AS "DNSSEC", round(mean("web_https_tls_version")*100) AS "TLS", round(mean("tls")*100) AS "TLS cf NCSC", round(mean("web_https_http_redirect")*100) AS "HTTPS (forced)", round(mean("web_https_http_hsts")*100) AS "HSTS" FROM "IV-metingen"."autogen"."web" WHERE "q" >= 20193 AND "q" <= 20202 GROUP BY "quarter" ORDER BY time ASC

query_mail_lastQs_mail = 'SELECT round(mean("mail_auth_dmarc_exist")*100) AS "DMARC",  round(mean("mail_auth_dkim_exist")*100) AS "DKIM", round(mean("mail_auth_spf_exist")*100) AS "SPF",round(mean("mail_auth_dmarc_policy")*100) AS "DMARC Pol.", round(mean("mail_auth_spf_policy")*100) AS "SPF Pol." FROM "IV-metingen"."autogen"."mail" WHERE "q" >= {0} AND "q" <= {1} GROUP BY "quarter" ORDER BY time ASC'

query_mail_lastQs_mail_conn = 'SELECT round(mean("mail_starttls_tls_available")*100) AS "STARTTLS", round(mean("mail_starttls")*100) AS "STARTTLS cf NCSC", round(mean("mail_dnssec")*100) AS "DNSSEC MX", round(mean("mail_starttls_dane_valid")*100) AS "DANE" FROM "IV-metingen"."autogen"."mail" WHERE "q" >= {0} AND "q" <= {1} GROUP BY "quarter" ORDER BY time ASC'



# Query last months for all
query_web_lastMonths = 'SELECT round(mean("score")) AS "score (web)", round(mean("web_ipv6")*100) AS "IPv6 (web)", round(mean("web_dnssec")*100) AS "DNSSEC (web)", round(mean("web_https")*100) AS "TLS" FROM "IV-metingen"."autogen"."web" WHERE "ym" >= {0} AND "ym" <= {1}  GROUP BY "yearmonth" ORDER BY time ASC'
query_mail_lastMonths = 'SELECT round(mean("score")) AS "score (mail)", round(mean("mail_ipv6")*100) AS "IPv6 (mail)", round(mean("mail_dnssec")*100) AS "DNSSEC (mail)", round(mean("mail_starttls_tls_available")*100) AS "STARTTLS", round(mean("mail_auth_spf_policy")*100) AS "SPF", round(mean("mail_auth_dkim_exist")*100) AS "DKIM", round(mean("mail_auth_dmarc_policy")*100) AS "DMARC" FROM "IV-metingen"."autogen"."mail" WHERE "ym" >= {0} AND "ym" <= {1} GROUP BY "yearmonth" ORDER BY time ASC'



query_web_lastQs_sector = 'SELECT round(mean("score")) AS "score (web)", round(mean("web_ipv6")*100) AS "IPv6 (web)", round(mean("web_dnssec")*100) AS "DNSSEC (web)", round(mean("web_https")*100) AS "TLS" FROM "IV-metingen"."autogen"."web" WHERE "q">={0} AND "q"<={1} AND "type"=\'{2}\' GROUP BY "quarter"'
query_mail_lastQs_sector = 'SELECT round(mean("score")) AS "score (mail)", round(mean("mail_ipv6")*100) AS "IPv6 (mail)", round(mean("mail_dnssec")*100) AS "DNSSEC (mail)",round(mean("mail_starttls_tls_available")*100) AS "STARTTLS",round(mean("mail_auth_spf_policy")*100) AS "SPF", round(mean("mail_auth_dkim_exist")*100) AS "DKIM", round(mean("mail_auth_dmarc_policy")*100) AS "DMARC" FROM "IV-metingen"."autogen"."mail" WHERE "q">={0} AND "q"<={1} AND "type"=\'{2}\' GROUP BY "quarter"'

query_web_lastMonths_sector = 'SELECT round(mean("score")) AS "score (web)", round(mean("web_ipv6")*100) AS "IPv6 (web)", round(mean("web_dnssec")*100) AS "DNSSEC (web)", round(mean("web_https")*100) AS "TLS" FROM "IV-metingen"."autogen"."web" WHERE "ym">={0} AND "ym"<={1} AND "type"=\'{2}\' GROUP BY "yearmonth"'
query_mail_lastMonths_sector = 'SELECT round(mean("score")) AS "score (mail)", round(mean("mail_ipv6")*100) AS "IPv6 (mail)", round(mean("mail_dnssec")*100) AS "DNSSEC (mail)",round(mean("mail_starttls_tls_available")*100) AS "STARTTLS",round(mean("mail_auth_spf_policy")*100) AS "SPF", round(mean("mail_auth_dkim_exist")*100) AS "DKIM", round(mean("mail_auth_dmarc_policy")*100) AS "DMARC" FROM "IV-metingen"."autogen"."mail" WHERE "ym">={0} AND "ym"<={1} AND "type"=\'{2}\' GROUP BY "yearmonth"'


def Nr2Q(q):
    return '{0}Q{1}'.format(str(q)[:4], str(q)[-1])

def Nr2M(q):
    return '{0}\'{1}'.format(calendar.month_abbr[int(str(q)[-2:])], str(q)[2:4])


def returnBarGraph(df, title=' ', y_label = 'score/percentage', label_suffix='', palette = paletteR ):

    pp = pprint.PrettyPrinter(indent=4)

    # All but first column are the categories
    categories = list(df.columns)[1:]
    # Content of the first column are the subcategories per category
    subcats =  list(df.iloc[ : ,0])
    x = [ (category, subcat) for category in categories for subcat in subcats ]

    values = []
    for cat in categories:
        values.extend(df[cat].tolist())

    value_labels = []
    for value in values:
        value_labels.append(str(value)+label_suffix)

    source = ColumnDataSource(data=dict(x=x, y=values, labels=value_labels))

    p = figure(x_range=FactorRange(*x), y_range=ranges.Range1d(start=0,end=100), y_minor_ticks=10, y_axis_label=y_label, plot_height=800, plot_width=1280, title=title, title_location='above',  toolbar_location=None, tools="")
    # min_border_top

    labels = LabelSet(x='x', y='y', text='labels', level='glyph', x_offset=0, y_offset=5, angle=90, angle_units='deg', source=source, render_mode='canvas', text_font_size="9pt")

    p.vbar(x='x', top='y', width=0.9, source=source, line_color="white", fill_color=factor_cmap('x', palette=palette, factors=subcats, start=1))

    p.add_layout(labels)

    p.x_range.range_padding = 0.05
    p.title.align = 'center'
    p.title.text_font_size="12pt"
    p.title.text_font_style="bold"
    p.xaxis.major_label_orientation = math.pi/2
    p.xgrid.grid_line_color = None

    return p

def returnHeatmap(df, title=' ', rangemin=0, rangemax=1, palette = paletteHeatmap, incsign=False):
    # Creëer een 'heat map' voor de individuele scores van de instellingen/domeinen
    pp = pprint.PrettyPrinter(indent=4)

    # De score tabel wordt achter het domein gevoegd (bv 'www.surf.nl (97)'), zodat de inhoud zelf alleen 0/1 (rood/groen) is.
    domains = list(df.iloc[ : ,0])

    row = 0
    for domain in domains:
        score = df.iat[row, 1]
        lb='('
        if incsign:
            if (score>0):
                lb='(+'

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

    lentxt = len(max(list(df.index) , key = len))
    lencats = len(df.columns)
    width = 8*lentxt + 25*lencats + 20
    p = figure(title=title, x_range=list(df.columns), y_range=list(reversed(df.index)), x_axis_location="above", plot_width=width, plot_height=25*len(df.index)+150, tools="")

    p.rect(x='category', y='domain', width=1, height=1, source=source, line_color='#ffffff', fill_color=transform('passed', mapper))

    p.title.align = 'right'
    p.title.text_font_size="12pt"
    p.title.text_font_style="bold"

    p.axis.axis_line_color = None
    p.axis.major_tick_line_color = None
    p.axis.major_label_text_font_size = "10pt"
    p.axis.major_label_standoff = 0
    p.xaxis.major_label_orientation = 'vertical'

    return p

#-------------------

def query_to_dataframe(client, query, measurement, doPrint=False, doPrintIntermediate=False):

    pp = pprint.PrettyPrinter(indent=4)

    results = client.query(query)
    if doPrintIntermediate:
        print()
        print('Query:')
        print(query)
        print('Raw results:')
        pp.pprint(results.raw)

    # Get the tags of the results (as a list)
    keys = list(zip(*results.keys()))[1]
    if doPrintIntermediate:
        print()
        print('Keys')
        pp.pprint(keys)

    # These are the measurements itself (list of dictionaries with kvps)
    values = list(results.get_points(measurement = measurement))
    if doPrintIntermediate:
        print()
        print('Values')
        pp.pprint(values)

    # Create a dataframe from the keys
    pd_keys = pd.DataFrame(keys)

    # Create a dataframe from the values
    df1 = pd.DataFrame(values)

    # Merge with the values
    df = pd.concat([pd_keys, df1], axis=1, sort=False)

    # Drop the time column since it is always epoch = 0
    if 'time' in df.columns:
        df.drop('time', axis=1, inplace=True)

    if doPrint:
        print()
        print('Resulting dataframe')
        pp.pprint(df)

    return df

#-------------------
def scoreLastQs(client, startQ=20183, endQ=20193):

    pp = pprint.PrettyPrinter(indent=4)

    print('Scores {0} - {1} for all sectors'.format(Nr2Q(startQ), Nr2Q(endQ)))
    # Take the last X quarters (the one specified and later) for both web and mail
    dfW = query_to_dataframe(client, query_web_lastQs.format(startQ, endQ), 'web', doPrint=False)
    dfM = query_to_dataframe(client, query_mail_lastQs.format(startQ, endQ), 'mail', doPrint=False)

    # Now combine the two dataframes
    # Drop 'quarter' column first on Mail dataframe
    if 'quarter' in dfM.columns:
        dfM.drop('quarter', axis=1, inplace=True)
    df = pd.concat([dfW, dfM], axis=1)


    p = returnBarGraph(df, title='Resultaten O&O ({0}-{1})'.format(Nr2Q(startQ),Nr2Q(endQ)), palette = paletteBR)
    p.output_backend="svg"
    export_svgs(p, filename="Scores-O-en-O.svg")
    export_png(p, filename="Scores-O-en-O.png")
    return

def scoreLastMonths(client, startMonth=201908, endMonth=201910):

    pp = pprint.PrettyPrinter(indent=4)

    print('Scores {0} - {1} for all sectors'.format(Nr2M(startMonth), Nr2M(endMonth)))
    # Take the last X quarters (the one specified and later) for both web and mail
    dfW = query_to_dataframe(client, query_web_lastMonths.format(startMonth, endMonth), 'web', doPrint=False)
    dfM = query_to_dataframe(client, query_mail_lastMonths.format(startMonth, endMonth), 'mail', doPrint=False)

    # Now combine the two dataframes
    # Drop 'quarter' column first on Mail dataframe
    if 'yearmonth' in dfM.columns:
        dfM.drop('yearmonth', axis=1, inplace=True)
    df = pd.concat([dfW, dfM], axis=1)


    p = returnBarGraph(df, title='Resultaten O&O ({0}-{1})'.format(Nr2M(startMonth),Nr2M(endMonth)), palette = paletteBR)
    p.output_backend="svg"
    export_svgs(p, filename="Scores-O-en-O.svg")
    export_png(p, filename="Scores-O-en-O.png")
    return

#-------------------
def scoreLastQsWebIV(client, startQ=20183, endQ=20193):

    pp = pprint.PrettyPrinter(indent=4)

    print('Scores Web IV {0} - {1} for all sectors'.format(Nr2Q(startQ), Nr2Q(endQ)))
    # Take the last X quarters (the one specified and later) for both web and mail
    df = query_to_dataframe(client, query_web_lastQs_web.format(startQ, endQ), 'web', doPrint=False)

    p = returnBarGraph(df, title='Resultaten IV-web O&O ({0}-{1})'.format(Nr2Q(startQ),Nr2Q(endQ)), palette = paletteBR)
    p.output_backend="svg"
    export_svgs(p, filename="Scores-O-en-O-IV-web.svg")
    export_png(p, filename="Scores-O-en-O-IV-web.png")
    return

def scoreLastQsMailIV(client, startQ=20183, endQ=20193):

    pp = pprint.PrettyPrinter(indent=4)

    print('Scores Mail IV {0} - {1} for all sectors'.format(Nr2Q(startQ), Nr2Q(endQ)))
    # Take the last X quarters (the one specified and later) for both web and mail
    df = query_to_dataframe(client, query_mail_lastQs_mail.format(startQ, endQ), 'mail', doPrint=False)

    p = returnBarGraph(df, title='Resultaten IV-mail O&O ({0}-{1})'.format(Nr2Q(startQ),Nr2Q(endQ)), palette = paletteBR)
    p.output_backend="svg"
    export_svgs(p, filename="Scores-O-en-O-IV-mail.svg")
    export_png(p, filename="Scores-O-en-O-IV-mail.png")
    return

def scoreLastQsMailConnIV(client, startQ=20183, endQ=20193):

    pp = pprint.PrettyPrinter(indent=4)

    print('Scores Mail (conn.) IV {0} - {1} for all sectors'.format(Nr2Q(startQ), Nr2Q(endQ)))
    # Take the last X quarters (the one specified and later) for both web and mail
    df = query_to_dataframe(client, query_mail_lastQs_mail_conn.format(startQ, endQ), 'mail', doPrint=False)

    p = returnBarGraph(df, title='Resultaten IV-mail connection O&O ({0}-{1})'.format(Nr2Q(startQ),Nr2Q(endQ)), palette = paletteBR)
    p.output_backend="svg"
    export_svgs(p, filename="Scores-O-en-O-IV-mail-conn.svg")
    export_png(p, filename="Scores-O-en-O-IV-mail-conn.png")
    return

#-------------------
def scoreLastQsPerSector(client, startQ=20183, endQ=20193):

    pp = pprint.PrettyPrinter(indent=4)

    for i, (sector, palette) in enumerate(sectors.items()):
        print('Scores {0} - {1} for {2}'.format(Nr2Q(startQ), Nr2Q(endQ), sector))
        # Take the last X quarters (the one specified and later) for both web and mail
        dfW = query_to_dataframe(client, query_web_lastQs_sector.format(startQ,endQ,sector), 'web', doPrint=False, doPrintIntermediate=False)
        dfM = query_to_dataframe(client, query_mail_lastQs_sector.format(startQ,endQ,sector), 'mail', doPrint=False)

        # Now combine the two dataframes
        # Drop 'quarter' column first on Mail dataframe
        if 'quarter' in dfM.columns:
            dfM.drop('quarter', axis=1, inplace=True)
        df = pd.concat([dfW, dfM], axis=1)

        p = returnBarGraph(df, title='Resultaten {0} ({1}-{2})'.format(sector, Nr2Q(startQ), Nr2Q(endQ)), palette = palette)
        p.output_backend="svg"
        export_svgs(p, filename="Scores-{0}.svg".format(sector))
        export_png(p, filename="Scores-{0}.png".format(sector))
    return

def scoreLastMonthsPerSector(client, startMonth=201908, endMonth=201910):

    pp = pprint.PrettyPrinter(indent=4)

    for i, (sector, palette) in enumerate(sectors.items()):
        print('Scores {0} - {1} for {2}'.format(Nr2M(startMonth), Nr2M(endMonth), sector))
        # Take the last X quarters (the one specified and later) for both web and mail
        dfW = query_to_dataframe(client, query_web_lastMonths_sector.format(startMonth,endMonth,sector), 'web', doPrint=False)
        dfM = query_to_dataframe(client, query_mail_lastMonths_sector.format(startMonth,endMonth,sector), 'mail', doPrint=False)

        # Now combine the two dataframes
        # Drop 'quarter' column first on Mail dataframe
        if 'yearmonth' in dfM.columns:
            dfM.drop('yearmonth', axis=1, inplace=True)
        df = pd.concat([dfW, dfM], axis=1)

        p = returnBarGraph(df, title='Resultaten {0} ({1}-{2})'.format(sector, Nr2M(startMonth), Nr2M(endMonth)), palette = palette)
        p.output_backend="svg"
        export_svgs(p, filename="Scores-{0}.svg".format(sector))
        export_png(p, filename="Scores-{0}.png".format(sector))
    return

#-------------------

def scoreLastQ_X(client, startQ=20183, groupby='type', palette=paletteHeatmap):

    pp = pprint.PrettyPrinter(indent=4)

    print('Scores of {0} for all sectors (by {1})'.format(Nr2Q(startQ), groupby))
    # Take the quarter for both web and mail
    dfW = query_to_dataframe(client, query_web_lastQ_X.format(startQ, groupby), 'web', doPrint=False)
    dfM = query_to_dataframe(client, query_mail_lastQ_X.format(startQ, groupby), 'mail', doPrint=False)

    # Now combine the two dataframes
    # Drop 'type' column first on Mail dataframe
    if groupby in dfM.columns:
        dfM.drop(groupby, axis=1, inplace=True)
    df = pd.concat([dfW, dfM], axis=1)

    p = returnBarGraph(df, title='Resultaten O&O afhankelijk van {0} ({1})'.format(groupby, Nr2Q(startQ)) , palette = palette)
    p.output_backend="svg"
    export_svgs(p, filename="Scores-O-en-O-per-{}.svg".format(groupby))
    export_png(p, filename="Scores-O-en-O-per-{}.png".format(groupby))
    return


def scoreLastMonth_X(client, startMonth=201910, groupby='type', palette=paletteHeatmap):

    pp = pprint.PrettyPrinter(indent=4)

    print('Scores of {0} for all sectors (per {1})'.format(Nr2M(startMonth), groupby))
    # Take the quarter for both web and mail
    dfW = query_to_dataframe(client, query_web_lastMonth_X.format(startMonth, groupby), 'web', doPrint=False)
    dfM = query_to_dataframe(client, query_mail_lastMonth_X.format(startMonth, groupby), 'mail', doPrint=False)

    # Now combine the two dataframes
    # Drop 'type' column first on Mail dataframe
    if groupby in dfM.columns:
        dfM.drop(groupby, axis=1, inplace=True)
    df = pd.concat([dfW, dfM], axis=1)

    p = returnBarGraph(df, title='Resultaten O&O afhankelijk van {0} ({1})'.format(groupby, Nr2M(startMonth)), palette = palette)
    p.output_backend="svg"
    export_svgs(p, filename="Scores-O-en-O-per-{}.svg".format(groupby))
    export_png(p, filename="Scores-O-en-O-per-{}.png".format(groupby))
    return

#-------------------

def detailsQ(client, startQ=20193):
    pp = pprint.PrettyPrinter(indent=4)

    measurements = {'web':query_detail_web_Q, 'mail':query_detail_mail_Q}


    for i, (measurement, query) in enumerate(measurements.items()):
        print('Detailed {0} scores for ({1})'.format(measurement, Nr2Q(startQ)))
        df = query_to_dataframe(client, query.format(startQ), measurement)
        df.sort_values(by=['score','domain'], ascending=False, inplace=True)
        df.reset_index(inplace=True, drop=True)
        p = returnHeatmap(df, title='Details {0} ({1})'.format(measurement, Nr2Q(startQ)))
        p.output_backend="svg"
        export_svgs(p, filename='Details-O-en-O-{0}.svg'.format(measurement))
        export_png(p, filename='Details-O-en-O-{0}.png'.format(measurement))

def detailsMonth(client, startMonth=201910):
    pp = pprint.PrettyPrinter(indent=4)

    measurements = {'web':query_detail_web_Month, 'mail':query_detail_mail_Month}

    for i, (measurement, query) in enumerate(measurements.items()):
        print('Detailed {0} scores ({1})'.format(measurement, Nr2M(startMonth)))
        df = query_to_dataframe(client, query.format(startMonth), measurement)
        df.sort_values(by=['score','domain'], ascending=False, inplace=True)
        df.reset_index(inplace=True, drop=True)
        p = returnHeatmap(df, title='Details {0} ({1})'.format(measurement, Nr2M(startMonth)))
        p.output_backend="svg"
        export_svgs(p, filename='Details-O-en-O-{0}.svg'.format(measurement))
        export_png(p, filename='Details-O-en-O-{0}.png'.format(measurement))



#-------------------

def detailsQperSector(client, startQ=20193):
    pp = pprint.PrettyPrinter(indent=4)

    measurements = {'web':query_detail_web_Q_Sector, 'mail':query_detail_mail_Q_Sector}


    for sector in sectors.keys():
        for i, (measurement, query) in enumerate(measurements.items()):
            print('Detailed {0} scores for {1} ({2})'.format(measurement, sector, Nr2Q(startQ)))
            df = query_to_dataframe(client, query.format(startQ, sector), measurement)
            df.sort_values(by=['score','domain'], ascending=False, inplace=True)
            df.reset_index(inplace=True, drop=True)
            p = returnHeatmap(df, title='Details {0} ({1}, {2})'.format(measurement, sector, Nr2Q(startQ)))
            p.output_backend="svg"
            export_svgs(p, filename='Details-{0}-{1}.svg'.format(sector,measurement))
            export_png(p, filename='Details-{0}-{1}.png'.format(sector,measurement))


def detailsMonthperSector(client, startMonth=201910):
    pp = pprint.PrettyPrinter(indent=4)

    measurements = {'web':query_detail_web_Month_Sector, 'mail':query_detail_mail_Month_Sector}

    for sector in sectors.keys():
        for i, (measurement, query) in enumerate(measurements.items()):
            print('Detailed {0} scores for {1} ({2})'.format(measurement, sector, Nr2M(startMonth)))
            df = query_to_dataframe(client, query.format(startMonth, sector), measurement)
            df.sort_values(by=['score','domain'], ascending=False, inplace=True)
            df.reset_index(inplace=True, drop=True)
            p = returnHeatmap(df, title='Details {0} ({1}, {2})'.format(measurement, sector, Nr2M(startMonth)))
            p.output_backend="svg"
            export_svgs(p, filename='Details-{0}-{1}.svg'.format(sector,measurement))
            export_png(p, filename='Details-{0}-{1}.png'.format(sector,measurement))

#-------------------

def topImproversQ(client, firstQ=20192, lastQ=20193, topX=5):
    pp = pprint.PrettyPrinter(indent=4)

    measurements = {'web':query_detail_web_Q, 'mail':query_detail_mail_Q}

    for i, (measurement, query) in enumerate(measurements.items()):
        print('Top {0} improvers {1} ({2}-{3})'.format(topX, measurement, Nr2Q(firstQ), Nr2Q(lastQ)))

        df1 = query_to_dataframe(client, query.format(firstQ), measurement)
        df1.sort_values(by=['domain'], ascending=True, inplace=True)

        df2 = query_to_dataframe(client, query.format(lastQ), measurement)
        df2.sort_values(by=['domain'], ascending=True, inplace=True)

        # Multiply everything but the score by 2 for the last one
        # Then deduct the first one from the second
        # This will give possible 'passed' values of:
        # 0->false, 1->was True and is True, 2-> was False but now True
        # for this to work we have to set the domain as index
        # (so they will be ignored for the subtraction)
        df1 = df1.set_index('domain')
        df2 = df2.set_index('domain')
        df2 = df2*2
        df2.score = round(df2.score/2)
        df2 = df2.astype({"score": int})
        df = df2.subtract(df1)
        df['score'].fillna(0, inplace=True)
        df = df.astype({"score": int})

        # Now sort by score
        df.sort_values(by=['score','domain'], ascending=False, inplace=True)

        #Drop all after the first five (we want a top 5)
        if topX > 0:
            df.drop(df.index[topX:], inplace=True)
        # Make domains a column again (otherwise Heatmap will fail)
        df.reset_index(level=0, inplace=True)

        # Create a heatmap for this
        p = returnHeatmap(df, palette=paletteTop5, rangemin=-1, rangemax=2, title='Top {0} ({1}, {2}-{3})'.format(topX, measurement, Nr2Q(firstQ), Nr2Q(lastQ)), incsign=True)
        p.output_backend="svg"
        export_svgs(p, filename='Top{0} {1}.svg'.format(topX,measurement))
        export_png(p, filename='Top{0} {1}.png'.format(topX,measurement))

def topImproversMonth(client, firstMonth=201909, lastMonth=201910, topX=5):
    pp = pprint.PrettyPrinter(indent=4)

    measurements = {'web':query_detail_web_Month, 'mail':query_detail_mail_Month}

    for i, (measurement, query) in enumerate(measurements.items()):
        print('Top {0} improvers {1} ({2} - {3})'.format(topX, measurement, Nr2M(firstMonth), Nr2M(lastMonth)))

        df1 = query_to_dataframe(client, query.format(firstMonth), measurement)
        df1.sort_values(by=['domain'], ascending=True, inplace=True)
        df2 = query_to_dataframe(client, query.format(lastMonth), measurement)
        df2.sort_values(by=['domain'], ascending=True, inplace=True)

        # Multiply everything but the score by 2 for the last one
        # Then deduct the first one from the second
        # This will give possible 'passed' values of:
        # 0->false, 1->was True and is True, 2-> was False but now True
        # for this to work we have to set the domain as index
        # (so they will be ignored for the subtraction)
        df1 = df1.set_index('domain')
        df2 = df2.set_index('domain')
        df2 = df2*2
        df2.score = round(df2.score/2)
        df2 = df2.astype({"score": int})
        df = df2.subtract(df1)

        # Now sort by score
        df.sort_values(by=['score','domain'], ascending=False, inplace=True)

        #Drop all after the first five (we want a top 5)
        if topX > 0:
            df.drop(df.index[topX:], inplace=True)
        # Make domains a column again (otherwise Heatmap will fail)
        df.reset_index(level=0, inplace=True)

        # Create a heatmap for this
        p = returnHeatmap(df, palette=paletteTop5, rangemin=-1, rangemax=2, title='Top {0} ({1}, {2}-{3})'.format(topX, measurement, Nr2M(firstMonth), Nr2M(lastMonth)), incsign=True)
        p.output_backend="svg"
        export_svgs(p, filename='Top{0} {1}.svg'.format(topX,measurement))
        export_png(p, filename='Top{0} {1}.png'.format(topX,measurement))


#-------------------

# Quarterly reports
def quarterly_reports(client, startQ=20184, endQ=20193, improveStartQ=20192):
    scoreLastQs(client=client, startQ=startQ, endQ=endQ)
    scoreLastQsWebIV(client=client, startQ=startQ, endQ=endQ)
    scoreLastQsMailIV(client=client, startQ=startQ, endQ=endQ)
    scoreLastQsMailConnIV(client=client, startQ=startQ, endQ=endQ)

  ##    scoreLastQ_X(client=client, startQ=endQ, groupby='SURFmailfilter', palette = paletteHeatmap)
  ##    scoreLastQ_X(client=client, startQ=endQ, groupby='SURFdomeinen', palette = paletteHeatmap)
    scoreLastQ_X(client=client, startQ=endQ, groupby='type', palette = paletteSector)
    scoreLastQsPerSector(client=client, startQ=startQ, endQ=endQ)
    detailsQperSector(client=client, startQ=endQ)
    detailsQ(client=client, startQ=endQ)
    topImproversQ(client, firstQ=improveStartQ, lastQ=endQ, topX=5)
    topImproversQ(client, firstQ=improveStartQ, lastQ=endQ, topX=0)
    return

# Monthly reports
def monthly_reports(client, startMonth=201807, endMonth=201910, improveStartMonth=201909):
    scoreLastMonths(client=client, startMonth=startMonth, endMonth=endMonth)
##    scoreLastMonth_X(client=client, startMonth=endMonth, groupby='SURFdomeinen', palette = paletteHeatmap)
##    scoreLastMonth_X(client=client, startMonth=endMonth, groupby='SURFmailfilter', palette = paletteHeatmap)
    scoreLastMonth_X(client=client, startMonth=endMonth, groupby='type', palette = paletteSector)
    scoreLastMonthsPerSector(client=client, startMonth=startMonth, endMonth=endMonth)
    detailsMonthperSector(client=client, startMonth=endMonth)
    detailsMonth(client=client, startMonth=endMonth)
    topImproversMonth(client, firstMonth=improveStartMonth, lastMonth=endMonth, topX=5)
    topImproversMonth(client, firstMonth=improveStartMonth, lastMonth=endMonth, topX=0)
    return

####################################

pp = pprint.PrettyPrinter(indent=4)

client = InfluxDBClient(host='localhost', port = 8086)
client.switch_database('IV-metingen')

# Quarterly reports
quarterly_reports(client, startQ=20192, endQ=20202, improveStartQ=20201)

# Monthly reports
#monthly_reports(client, startMonth=202002, endMonth=202006, improveStartMonth=202005)

#scoreLatestPerSector(client)
