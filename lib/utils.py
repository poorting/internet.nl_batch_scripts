#! /usr/bin/env python3

import getpass
import configparser
import logging
import collections
import json
import datetime
import sys
import calendar

import pandas as pd
import numpy as np
import pprint

logger = logging.getLogger('__main__')

TYPE_UNKNOWN = '<unknown>'
categories = {
    "mail": ["mail_ipv6", "mail_dnssec", "mail_auth", "mail_starttls"],
    "web": ["web_ipv6", "web_dnssec", "web_https", "web_appsecpriv"],
             }

specific_tests = {
    "mail": [
        "mail_auth_dkim_exist",
        "mail_auth_dmarc_exist",
        "mail_auth_dmarc_policy",
        "mail_auth_spf_exist",
        "mail_auth_spf_policy",
        "mail_starttls_dane_valid",
        "mail_starttls_tls_available",
    ],
    "web": [
        "web_https_tls_version",
        "web_https_http_redirect",
        "web_https_http_hsts",
    ]
}

# Translation of old (API1.1) measurement categories to new (API2.0) names
v1_to_2_cats = {
    "mail": {
        "ipv6": "mail_ipv6",
        "dnssec": "mail_dnssec",
        "auth": "mail_auth",
        "tls": "mail_starttls",
    },
    "web": {
        "ipv6": "web_ipv6",
        "dnssec": "web_dnssec",
        "tls": "web_https",
        "appsecpriv": "web_appsecpriv",
    }
}

v2_to_1_cats = {
    "mail": {
        "mail_ipv6": "ipv6",
        "mail_dnssec": "dnssec",
        "mail_auth": "auth",
        "mail_starttls": "tls",
    },
    "web": {
        "web_ipv6": "ipv6",
        "web_dnssec": "dnssec",
        "web_https": "tls",
        "web_appsecpriv": "appsecpriv",
    }
}


# ------------------------------------------------------------------------------
def _askCredentials(credentials):
    credentials['username'] = getpass.getpass(prompt='Username: ')
    credentials['password'] = getpass.getpass()
    return credentials


# ------------------------------------------------------------------------------
def getCredentials(option=None):
    credentials = {'endpoint': 'https://batch.internet.nl/api/batch/v2/requests',
                   'username': '',
                   'password': ''}
    config = configparser.ConfigParser({}, collections.OrderedDict)
    config.read('batch.conf')
    if len(config.sections()) == 0:
        # No conf file found, or empty
        # So ask for username and password
        return _askCredentials(credentials)

    sections = config.sections()
    if len(sections) == 0:
        # No sections in conf file
        return _askCredentials(credentials)

    if option is None:
        # Nothing specified on the command line, use first one
        section = config[sections[0]]
    else:
        if option in sections:
            section = config[option]
        else:
            # Specified section not found
            return _askCredentials(credentials)

    endpoint = section.get('endpoint', 'https://batch.internet.nl/api/batch/v2/requests')
    username = section.get('username')
    password = section.get('password')

    if not username:
        username = getpass.getpass(prompt='Username: ')
    if not password:
        password = getpass.getpass()

    return {'endpoint': endpoint, 'username': username, 'password': password}


# ------------------------------------------------------------------------------
def Nr2Name(q):
    # return '{0}Q{1}'.format(str(q)[:4], str(q)[-1])
    if len(str(q)) > 5:
        return _Nr2M(q)
    else:
        return _Nr2Q(q)


# ------------------------------------------------------------------------------
def _Nr2Q(q):
    return '{0}Q{1}'.format(str(q)[:4], str(q)[-1])


# ------------------------------------------------------------------------------
def _Nr2M(q):
    return '{0}\'{1}'.format(calendar.month_abbr[int(str(q)[-2:])], str(q)[2:4])


# ------------------------------------------------------------------------------
def openJSON(filename):
    """open the JSON (result) file and return it as a json structure"""
    data = {}
    with open(filename) as f:
        data = json.load(f)
    return data


# ------------------------------------------------------------------------------
def getAPIversion(data):
    api_version = '2.0'
    if 'data' in data:
        api_version = data['data']['api-version']
    return api_version


# ------------------------------------------------------------------------------
def getMeasurementType(data):
    api_version = getAPIversion(data)

    if api_version == '1.1' or api_version == '1.0':
        return _getMeasurementType1_1(data['data']['domains'])
    else:
        return _getMeasurementType2_0(data['domains'])


# ------------------------------------------------------------------------------
def _getMeasurementType1_1(domains):
    """Determine whether measurement results are for web or mail"""
    measurementType = 'web'
    status = 'failed'
    for testDomain in domains:
        if (testDomain['status'] == 'ok'):
            for category in testDomain['categories']:
                if (category['category'] == 'auth'):
                    measurementType = 'mail'
            break

    return measurementType


# ------------------------------------------------------------------------------
def _getMeasurementType2_0(domains):
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


# ------------------------------------------------------------------------------
def JSONtoDF(data, domains_metadata, columns_to_add=[]):
    api_version = getAPIversion(data)

    if api_version == '1.1' or api_version == '1.0':
        return _JSONtoDF1_1(data, domains_metadata, columns_to_add)
    else:
        return _JSONtoDF2_0(data, domains_metadata, columns_to_add)


# ------------------------------------------------------------------------------
def _JSONtoDF1_1(data, domains_metadata, columns_to_add=[]):
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
    measurementType = _getMeasurementType1_1(domains)
    if not domains_metadata.empty:
        domains_metadata = domains_metadata.set_index(measurementType, inplace=False)

    for domainresults in domains:
        domain = domainresults['domain']
        df = df.append({'domain': domain}, ignore_index=True)

        # add the additional metadata
        if not domains_metadata.empty:
            if domain in domains_metadata.index:
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


# ------------------------------------------------------------------------------
def _JSONtoDF2_0(data, domains_metadata, columns_to_add=[]):
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
    measurementType = _getMeasurementType2_0(domains)
    if not domains_metadata.empty:
        domains_metadata = domains_metadata.set_index(measurementType, inplace=False)

    for domainname in domains:
        domainresults = domains[domainname]
        df = df.append({'domain': domainname}, ignore_index=True)

        # add the additional metadata
        if not domains_metadata.empty:
            if domainname in domains_metadata.index:
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
                    df.iloc[-1, df.columns.get_loc(views)] = '{} ({})'.format(dom_views[views]['status'],
                                                                              dom_views[views]['verdict'])

        # Finally add the link as well
        if not 'url' in df.columns:
            df['url'] = np.nan

        if 'report' in domainresults:
            df.iloc[-1, df.columns.get_loc('url')] = domainresults['report']['url']

    return df


# ------------------------------------------------------------------------------
def JSONtoCSV(data, domains_metadata, columns_to_add=[]):

    api_version = getAPIversion(data)

    if api_version == '1.1' or api_version == '1.0':
        return _JSONtoCSV1_1(data, domains_metadata, columns_to_add=columns_to_add)
    else:
        return _JSONtoCSV2_0(data, domains_metadata, columns_to_add=columns_to_add)


# ------------------------------------------------------------------------------
def _JSONtoCSV1_1(data, domains_metadata, columns_to_add):
    """Rework the top-level JSON results to a CSV file with the added metadata from the domains file"""

    logger.info("Structure of this file is API 1.1")

    header = ''
    body = []

    pp = pprint.PrettyPrinter(indent=4)

    payload = data['data']
    domains = payload['domains']

    submit_date = datetime.datetime.strptime(payload['submission-date'], '%Y-%m-%dT%H:%M:%S.%f%z')
    # submit_date = payload['submission-date']

    measurementType = _getMeasurementType1_1(domains)
    logger.info("Measurement type: {}".format(measurementType))
    if not domains_metadata.empty:
        domains_metadata = domains_metadata.set_index(measurementType, inplace=False)

    for domainresults in domains:
        # line= [{"submit_date": submit_date},]
        line = [{"submit_date": payload['submission-date']}]
        domainname = domainresults['domain']
        line.append({'domain': domainname})

        quarters = [4, 7, 10, 1]
        if (submit_date.date().month in quarters):
            Q = quarters.index(submit_date.date().month) + 1
            YR = submit_date.date().year
            if Q == 4:
                YR = YR - 1
            # print(',quarter={0}Q{1}'.format(YR, Q), end='')
            line.append({'q': int('{0}{1}'.format(YR, Q))})
            line.append({'quarter': '{0}Q{1}'.format(YR, Q)})
        else:
            line.append({'q': 0})
            line.append({'quarter': ''})

        line.append({'ym': int('{}'.format(submit_date.strftime('%Y%m')))})
        line.append({'yearmonth': Nr2Name(int('{}'.format(submit_date.strftime('%Y%m'))))})

        # add the additional metadata
        if not domains_metadata.empty:
            if domainname in domains_metadata.index:
                for md in columns_to_add:
                    # check if metadata column exists first
                    if md in domains_metadata.columns:
                        line.append({'md_{}'.format(md): domains_metadata.at[domainname, md]})
                    else:
                        line.append({'md_{}'.format(md): TYPE_UNKNOWN})
            else:
                for md in columns_to_add:
                    line.append({'md_{}'.format(md): TYPE_UNKNOWN})

        if 'status' in domainresults:
            line.append({'status': domainresults['status']})
        else:
            line.append({'status': ''})

        if 'score' in domainresults:
            line.append({'score': domainresults['score']})
        else:
            line.append({'score': 0})

        if 'categories' in domainresults:
            dom_cats = domainresults['categories']
            for cat in categories[measurementType]:
                res = 0
                for dom_cat in dom_cats:
                    if v2_to_1_cats[measurementType][cat] == dom_cat['category']:
                        res = int(dom_cat['passed'])
                        break
                line.append({cat: res})
        else:
            for cat in categories[measurementType]:
                line.append({cat: 0})

        if 'views' in domainresults:
            dom_views = domainresults['views']
            for test in specific_tests[measurementType]:
                res = 0
                for view in dom_views:
                    if view['name'] == test:
                        res = int(view['result'])
                        break
                line.append({test: res})
        else:
            for test in specific_tests[measurementType]:
                line.append({test: 0})

        if 'link' in domainresults:
            line.append({'url': domainresults['link']})
        else:
            line.append({'url': ''})

        # pp.pprint(line)
        hdr = []
        bdy = []
        for it in line:
            hdr.append(str(list(it.keys())[0]))
            bdy.append(str(list(it.values())[0]))

        header = ','.join(hdr)
        body.append(','.join(bdy))

    body = '\n'.join(body)
    # print(header)
    # print(body)

    return {"header": header, "body": body}


# ------------------------------------------------------------------------------
def _JSONtoCSV2_0(data, domains_metadata, columns_to_add):
    """Rework the top-level JSON results to a DataFrame with the added metadata from the domains file"""

    logger.info("Structure of this file is API 2.0")

    header = ''
    body = []
    pp = pprint.PrettyPrinter(indent=4)

    submit_date = datetime.datetime.strptime(data['request']['submit_date'], '%Y-%m-%dT%H:%M:%S.%f%z')
    domains = data['domains']

    measurementType = _getMeasurementType2_0(domains)
    logger.info("Measurement type: {}".format(measurementType))
    if not domains_metadata.empty:
        domains_metadata = domains_metadata.set_index(measurementType, inplace=False)
        # pp.pprint(domains_metadata)

    for domainname in domains:
        domainresults = domains[domainname]
        # line= [{"submit_date": submit_date},]
        line = [{"submit_date": data['request']['submit_date']}, {'domain': domainname}]

        quarters = [4, 7, 10, 1]
        if (submit_date.date().month in quarters):
            Q = quarters.index(submit_date.date().month) + 1
            YR = submit_date.date().year
            if Q == 4:
                YR = YR - 1
            # print(',quarter={0}Q{1}'.format(YR, Q), end='')
            line.append({'q': int('{0}{1}'.format(YR, Q))})
            line.append({'quarter': '{0}Q{1}'.format(YR, Q)})
        else:
            line.append({'q': 0})
            line.append({'quarter': ''})

        line.append({'ym': int('{}'.format(submit_date.strftime('%Y%m')))})
        line.append({'yearmonth': Nr2Name(int('{}'.format(submit_date.strftime('%Y%m'))))})

        # add the additional metadata
        if not domains_metadata.empty:
            if domainname in domains_metadata.index:
                for md in columns_to_add:
                    # check if metadata column exists first
                    if md in domains_metadata.columns:
                        line.append({'md_{}'.format(md): domains_metadata.at[domainname, md]})
                    else:
                        line.append({'md_{}'.format(md): TYPE_UNKNOWN})
            else:
                logger.info("'{}' is NOT in metadata".format(domainname))
                for md in columns_to_add:
                    line.append({'md_{}'.format(md): TYPE_UNKNOWN})

        if 'status' in domainresults:
            line.append({'status': domainresults['status']})
        else:
            line.append({'status': ''})

        if 'scoring' in domainresults:
            line.append({'score': int(domainresults['scoring']['percentage'])})
        else:
            line.append({'score': 0})

        # if 'categories' in domainresults:
        if 'results' in domainresults:
            dom_cats = domainresults['results']['categories']
            for cat in categories[measurementType]:
                if cat in dom_cats:
                    line.append({cat: int(dom_cats[cat]['status'] == 'passed')})
                else:
                    line.append({cat: 0})
            if 'tests' in domainresults['results']:
                dom_views = domainresults['results']['tests']
                for test in specific_tests[measurementType]:
                    res = 0
                    for view in dom_views:
                        if view == test:
                            # res = int(view['result'])
                            res = int(dom_views[view]['status'] == 'passed' or dom_views[view]['status'] == 'warning')
                            break
                    line.append({test: res})

        else:
            for cat in categories[measurementType]:
                line.append({cat: 0})

        # Finally add the link as well
        if 'report' in domainresults:
            line.append({'url': domainresults['report']['url']})
        else:
            line.append({'url': ''})

        # pp.pprint(line)
        hdr = []
        bdy = []
        for it in line:
            hdr.append(str(list(it.keys())[0]))
            bdy.append(str(list(it.values())[0]))

        header = ','.join(hdr)
        body.append(','.join(bdy))

    body = '\n'.join(body)
    # print(header)
    # print(body)

    return {"header": header, "body": body}


# ------------------------------------------------------------------------------
def JSONtoCSVall(data, domains_metadata, columns_to_add=[]):

    api_version = getAPIversion(data)

    if api_version == '1.1' or api_version == '1.0':
        return _JSONtoCSVall1_1(data, domains_metadata, columns_to_add=columns_to_add)
    else:
        return _JSONtoCSVall2_0(data, domains_metadata, columns_to_add=columns_to_add)


# ------------------------------------------------------------------------------
def _JSONtoCSVall1_1(data, domains_metadata, columns_to_add):
    """Rework the top-level JSON results to a CSV file with the added metadata from the domains file"""

    logger.info("Structure of this file is API 1.1")

    header = ''
    body = []

    pp = pprint.PrettyPrinter(indent=4)

    payload = data['data']
    domains = payload['domains']

    submit_date = datetime.datetime.strptime(payload['submission-date'], '%Y-%m-%dT%H:%M:%S.%f%z')
    # submit_date = payload['submission-date']

    measurementType = _getMeasurementType1_1(domains)
    logger.info("Measurement type: {}".format(measurementType))
    if not domains_metadata.empty:
        domains_metadata = domains_metadata.set_index(measurementType, inplace=False)

    for domainresults in domains:
        # line= [{"submit_date": submit_date},]
        line = [{"submit_date": payload['submission-date']}]
        domainname = domainresults['domain']
        line.append({'domain': domainname})

        quarters = [4, 7, 10, 1]
        if (submit_date.date().month in quarters):
            Q = quarters.index(submit_date.date().month) + 1
            YR = submit_date.date().year
            if Q == 4:
                YR = YR - 1
            # print(',quarter={0}Q{1}'.format(YR, Q), end='')
            line.append({'q': int('{0}{1}'.format(YR, Q))})
            line.append({'quarter': '{0}Q{1}'.format(YR, Q)})
        else:
            line.append({'q': 0})
            line.append({'quarter': ''})

        line.append({'ym': int('{}'.format(submit_date.strftime('%Y%m')))})
        line.append({'yearmonth': Nr2Name(int('{}'.format(submit_date.strftime('%Y%m'))))})

        # add the additional metadata
        if not domains_metadata.empty:
            if domainname in domains_metadata.index:
                for md in columns_to_add:
                    # check if metadata column exists first
                    if md in domains_metadata.columns:
                        line.append({'{}'.format(md): domains_metadata.at[domainname, md]})
                    else:
                        line.append({'{}'.format(md): TYPE_UNKNOWN})
            else:
                for md in columns_to_add:
                    line.append({'{}'.format(md): TYPE_UNKNOWN})

        if 'status' in domainresults:
            line.append({'status': domainresults['status']})
        else:
            line.append({'status': ''})

        if 'score' in domainresults:
            line.append({'score': domainresults['score']})
        else:
            line.append({'score': 0})

        if 'categories' in domainresults:
            dom_cats = domainresults['categories']
            for dom_cat in dom_cats:
                line.append({dom_cat['category']: int(dom_cat['passed'])})

        if 'views' in domainresults:
            dom_views = domainresults['views']
            for view in dom_views:
                line.append({view['name']: int(view['result'])})

        if 'link' in domainresults:
            line.append({'url': domainresults['link']})
        else:
            line.append({'url': ''})

        # pp.pprint(line)
        hdr = []
        bdy = []
        for it in line:
            hdr.append(str(list(it.keys())[0]))
            bdy.append(str(list(it.values())[0]))

        header = ','.join(hdr)
        body.append(','.join(bdy))

    body = '\n'.join(body)
    # print(header)
    # print(body)

    return {"header": header, "body": body}


# ------------------------------------------------------------------------------
def _JSONtoCSVall2_0(data, domains_metadata, columns_to_add):
    """Rework the top-level JSON results to a DataFrame with the added metadata from the domains file"""

    logger.info("Structure of this file is API 2.0")

    header = []
    body = []
    pp = pprint.PrettyPrinter(indent=4)

    submit_date = datetime.datetime.strptime(data['request']['submit_date'], '%Y-%m-%dT%H:%M:%S.%f%z')
    domains = data['domains']

    measurementType = _getMeasurementType2_0(domains)
    logger.info("Measurement type: {}".format(measurementType))

    if not domains_metadata.empty:
        domains_metadata = domains_metadata.set_index(measurementType, inplace=False)

    for domainname in domains:
        domainresults = domains[domainname]
        # line= [{"submit_date": submit_date},]
        line = [{"submit_date": data['request']['submit_date']}, {'domain': domainname}]

        quarters = [4, 7, 10, 1]
        if (submit_date.date().month in quarters):
            Q = quarters.index(submit_date.date().month) + 1
            YR = submit_date.date().year
            if Q == 4:
                YR = YR - 1
            # print(',quarter={0}Q{1}'.format(YR, Q), end='')
            line.append({'q': int('{0}{1}'.format(YR, Q))})
            line.append({'quarter': '{0}Q{1}'.format(YR, Q)})
        else:
            line.append({'q': 0})
            line.append({'quarter': ''})

        line.append({'ym': int('{}'.format(submit_date.strftime('%Y%m')))})
        line.append({'yearmonth': Nr2Name(int('{}'.format(submit_date.strftime('%Y%m'))))})

        # add the additional metadata
        if not domains_metadata.empty:
            if domainname in domains_metadata.index:
                for md in columns_to_add:
                    # check if metadata column exists first
                    if md in domains_metadata.columns:
                        line.append({'{}'.format(md): domains_metadata.at[domainname, md]})
                    else:
                        line.append({'{}'.format(md): TYPE_UNKNOWN})
            else:
                for md in columns_to_add:
                    line.append({'{}'.format(md): TYPE_UNKNOWN})

        if 'status' in domainresults:
            line.append({'status': domainresults['status']})
        else:
            line.append({'status': ''})

        if 'scoring' in domainresults:
            line.append({'score': int(domainresults['scoring']['percentage'])})
        else:
            line.append({'score': 0})

        # if 'categories' in domainresults:
        if 'results' in domainresults:
            dom_cats = domainresults['results']['categories']
            for cat in dom_cats:
                line.append({cat: int(dom_cats[cat]['status'] == 'passed')})

            if 'tests' in domainresults['results']:
                dom_views = domainresults['results']['tests']
                for view in dom_views:
                    line.append({view: int(dom_views[view]['status'] == 'passed' or dom_views[view]['status'] == 'warning')})

        # Finally add the link as well
        if 'report' in domainresults:
            line.append({'url': domainresults['report']['url']})
        else:
            line.append({'url': ''})

        # pp.pprint(line)
        hdr = []
        bdy = []
        for it in line:
            hdr.append(str(list(it.keys())[0]))
            bdy.append(str(list(it.values())[0]))

        header.append(','.join(hdr))
        body.append(','.join(bdy))

    body = '\n'.join(body)
    header = header[0]
    # print(header)
    # print(body)

    return {"header": header, "body": body}

