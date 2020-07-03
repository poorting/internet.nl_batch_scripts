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

def readCredentials(machine = 'batch.internet.nl', filename = 'credentials'):
    """Find login  password for machine from a netrc formatted file"""
    credentials = {'login':'', 'password':''}
    words=[];
    with open(filename, 'r') as creds:
        for line in creds:
            line = line.strip()
            words = words + line.split()

    for i in range(0, len(words), 6):
        endpoint = words[i:i+6]
        if endpoint[1].endswith(machine):
            credentials[endpoint[2]] = endpoint[3]
            credentials[endpoint[4]] = endpoint[5]
            break;

    return credentials

def openJSON(filename):
    """open the JSON (result) file and return it as a json structure"""
    data = {}
    with open(filename) as f:
        data = json.load(f)
    return data

def getAPIversion(data):
    api_version = '2.0'
    if 'data' in data:
        api_version = data['data']['api-version']
    return api_version

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

def JSONtoDF(data, domains_metadata, columns_to_add=[]):
    api_version = getAPIversion(data)

    if api_version == '1.1' or api_version == '1.0':
        return JSONtoDF1_1(data, domains_metadata, columns_to_add)
    else:
        return JSONtoDF2_0(data, domains_metadata, columns_to_add)


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
