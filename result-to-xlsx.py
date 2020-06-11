#! /usr/bin/env python3

import sys
import os
import csv
import requests
import json
import datetime
import time
import pandas as pd
import numpy as np
import math
import pprint

def openJSON(filename):
    """open the JSON (result) file and return it as a json structure"""
    data = {}
    with open(filename) as f:
        data = json.load(f)
    return data

def getMeasurementType1_1(domains):
    """Determine wether measurement results are for web or mail"""
    measurementType = 'web'
    status = 'failed'
    for testDomain in domains:
        if (testDomain['status'] == 'ok'):
            for category in testDomain['categories']:
                if (category['category'] == 'auth'):
                    measurementType = 'mail'
            break

    return measurementType

def getMeasurementType2_0(domains):
    """Determine wether measurement results are for web or mail"""
    measurementType = 'web'
    status = 'failed'
    for testDomain in domains:
        if (domains[testDomain]['status'] == 'ok'):
            for category in domains[testDomain]['results']['categories']:
                if (category.startswith('mail_')):
                    measurementType = 'mail'
                if (category.startswith('web_')):
                    measurementType = 'web'
            break

    return measurementType


def JSONtoDF1_1(data, domains_metadata, columns_to_add=[]):
    """Rework the JSON results to a DataFrame with the added metadata from the domains file"""

    pp = pprint.PrettyPrinter(indent=4)

    df = pd.DataFrame()
    for cta in columns_to_add:
        if cta in domains_metadata.columns:
            df[cta] = np.nan
        else:
            print('Column \'{0}\' could not be found in domains metadata and will be ignored'.format(cta))

    df['domain'] = ''
    df['status'] = ''
    df['score'] = int(0)

    payload = data['data']

    domains = payload['domains']

    # Set index on pandas dataframe as the measurement type.
    # That column contains the domains as used in the measurements
    measurementType = getMeasurementType1_1(domains)
    if not domains_metadata.empty:
        domains_metadata = domains_metadata.set_index(measurementType, inplace=False)

    for domainresults in domains:
        domain = domainresults['domain']
        df = df.append({'domain':domain}, ignore_index=True)

        # add the additional metadata
        if not domains_metadata.empty:
            if domain in domains_metadata.index :
                for md in columns_to_add:
                    # check if metadata column exists first
                    if md in domains_metadata.columns:
                        df.iloc[-1, df.columns.get_loc(md)] = domains_metadata.at[domain, md]

        if 'status' in domainresults:
            df.iloc[-1, df.columns.get_loc('status')] = domainresults['status']

        if 'score' in domainresults:
            df.iloc[-1, df.columns.get_loc('score')] = int(domainresults['score'])

        if 'categories' in domainresults:
            dom_cats = domainresults['categories']
            for cats in dom_cats:
                # See if column already in resulting DataFrame
                cat = cats['category']
                if not cat in df.columns:
                    df[cat] = np.nan

                df.iloc[-1, df.columns.get_loc(cat)] = int(cats['passed'])

        if 'views' in domainresults:
            dom_views = domainresults['views']
            for views in dom_views:
                # See if column already in resulting DataFrame
                view = views['name']
                if not view in df.columns:
                    df[view] = np.nan

                df.iloc[-1, df.columns.get_loc(view)] = int(views['result'])

        # Finally add the link as well
        if not 'link' in df.columns:
            df['link'] = np.nan

        if 'link' in domainresults:
            df.iloc[-1, df.columns.get_loc('link')] = domainresults['link']

    return df


def JSONtoDF2_0(data, domains_metadata, columns_to_add=[]):
    """Rework the JSON results to a DataFrame with the added metadata from the domains file"""

    pp = pprint.PrettyPrinter(indent=4)

    df = pd.DataFrame()
    for cta in columns_to_add:
        if cta in domains_metadata.columns:
            df[cta] = np.nan
        else:
            print('Column \'{0}\' could not be found in domains metadata and will be ignored'.format(cta))

    df['domain'] = ''
    df['status'] = ''
    df['score'] = int(0)

    domains = data['domains']

    # Set index on pandas dataframe as the measurement type.
    # That column contains the domains as used in the measurements
    measurementType = getMeasurementType2_0(domains)
    if not domains_metadata.empty:
        domains_metadata = domains_metadata.set_index(measurementType, inplace=False)

    for domainname in domains:
        domainresults = domains[domainname]
        df = df.append({'domain':domainname}, ignore_index=True)

        # add the additional metadata
        if not domains_metadata.empty:
            if domainname in domains_metadata.index :
                for md in columns_to_add:
                    # check if metadata column exists first
                    if md in domains_metadata.columns:
                        df.iloc[-1, df.columns.get_loc(md)] = domains_metadata.at[domainname, md]

        if 'status' in domainresults:
            df.iloc[-1, df.columns.get_loc('status')] = domainresults['status']

        if 'scoring' in domainresults:
            df.iloc[-1, df.columns.get_loc('score')] = int(domainresults['scoring']['percentage'])

        if 'results' in domainresults:
            dom_cats = domainresults['results']['categories']
            for cats in dom_cats:
                # See if column already in resulting DataFrame
                if not cats in df.columns:
                    df[cats] = np.nan
                df.iloc[-1, df.columns.get_loc(cats)] = dom_cats[cats]['status']

            if 'tests' in domainresults['results']:
                dom_views = domainresults['results']['tests']
                for views in dom_views:
                    if not views in df.columns:
                        df[views] = np.nan
                    df.iloc[-1, df.columns.get_loc(views)] = '{} ({})'.format(dom_views[views]['status'], dom_views[views]['verdict'])

        # Finally add the link as well
        if not 'url' in df.columns:
            df['url'] = np.nan

        if 'report' in domainresults:
            df.iloc[-1, df.columns.get_loc('url')] = domainresults['report']['url']

    return df

#############################################

pp = pprint.PrettyPrinter(indent=4)

if len(sys.argv) < 2 :
    print ('Usage: ')
    print (sys.argv[0], ' <JSON results file|JSON results directory> [domains xlsx file] [metadata_column_name[,md_col_name2, ...]]')
    quit(1)

# Create empty dataframe
domains = pd.DataFrame()
domainsFile=''
columns_to_add=[]

if len(sys.argv) > 2 :
    domainsFile = sys.argv[2]
    columns_to_add=['type']

if len(sys.argv) > 3 :
    columns_to_add = sys.argv[3].split(',')

filelist=[]
filename = sys.argv[1]

if os.path.isdir(filename):
    if not filename.endswith("/"):
        filename=filename+'/'
    with os.scandir(filename) as it:
        for entry in it:
            if not entry.name.startswith('.') and entry.is_file() and entry.name.endswith('.json'):
                    filelist.append('{0}{1}'.format(filename, entry.name))
else:
    filelist.append(filename)

try:
    if not domainsFile == '':
        domains = pd.read_excel(domainsFile, sheet_name=0)
except Exception as e:
    print("error processing domains Excel file: {}".format(e))
    exit(1)

for fn in filelist:
    print('Processing {0}'.format(fn))
    data = openJSON(fn)

    api_version = '2.0'
    if 'data' in data:
        api_version = data['data']['api-version']

    if api_version == '1.1' or api_version == '1.0':
        df = JSONtoDF1_1(data, domains, columns_to_add)
    else:
        df = JSONtoDF2_0(data, domains, columns_to_add)

    df.sort_values(by=['score','domain'], ascending=False, inplace=True)
    # Write the DataFrame to a file of the same name but with xlsx extension
    fnr = fn[0:-5]+'.xlsx'
    print('Writing to {0}'.format(fnr))
    df.to_excel (fnr, index = None, header=True)


# All done
