#! /usr/bin/env python3

import sys
import os
import datetime
import pandas as pd
import pprint
from lib import utils as ut

v1_to_2_cats = {
    "mail" : {
        "ipv6"      : "mail_ipv6",
        "dnssec"    : "mail_dnssec",
        "auth"      : "mail_auth",
        "tls"       : "mail_starttls"
    },
    "web" : {
        "ipv6"      : "web_ipv6",
        "dnssec"    : "web_dnssec",
        "tls"       : "web_https",
        "appsecpriv": "web_appsecpriv"
    }
}

# The specific tests to push into the influx database as well
# (only the top results will be pushed normally)
specific_tests = {
    "mail" :[
        "mail_auth_dkim_exist",
        "mail_auth_dmarc_exist",
        "mail_auth_dmarc_policy",
        "mail_auth_spf_exist",
        "mail_auth_spf_policy",
        "mail_starttls_dane_valid",
        "mail_starttls_tls_available",
        ],
    "web" :[
        "web_https_tls_version",
        "web_https_http_redirect",
        "web_https_http_hsts",
    ]
}

def JSONtoInflux(data, domains_metadata, columns_to_add=['type']):
    api_version = ut.getAPIversion(data)

    if api_version == '1.1' or api_version == '1.0':
        JSONtoInflux1_1(data, domains_metadata, columns_to_add)
    else:
        JSONtoInflux2_0(data, domains_metadata, columns_to_add)



def JSONtoInflux1_1(data, domains_metadata, columns_to_add=['type']):
    """Display the results as a CSV file ready for import by influxdb"""

#    pp = pprint.PrettyPrinter(indent=4)

    payload = data['data']

    domains = payload['domains']

    # Set index on pandas dataframe as the measurement type.
    # That column contains the domains as used in the measurements
    measurementType = ut.getMeasurementType1_1(domains)
    if not domains_metadata.empty:
        domains_metadata = domains_metadata.set_index(measurementType, inplace=False)

#    pp.pprint(domains_metadata)

    timestamp_dt = datetime.datetime.strptime(payload['submission-date'], '%Y-%m-%dT%H:%M:%S.%f%z')
    # Rework it to the first of the month for proper alignment

    for domainresults in domains:
        domain = domainresults['domain']

        # do we need type?
        tagset={}
        if not domains_metadata.empty:
            if domain in domains_metadata.index :
                for md in columns_to_add:
                    tagset[md] = domains_metadata.at[domain, md]

        print('{0},year={1},month={2},yearmonth={3},domain={4}'.format(measurementType, timestamp_dt.date().year, timestamp_dt.date().month, timestamp_dt.strftime('%Y-%m'), domain), end='')
        for tag in tagset:
            print(',{0}={1}'.format(tag, tagset[tag]), end='')

        fieldset={}
        # Since we can't do comparison on tags (other then (in)equality) we have to include a field for this as well.
        # Will make queries (much) slower, but that doesn't matter much in this use case
        fieldset['ym'] = '{}i'.format(timestamp_dt.strftime('%Y%m'))

        quarters=[4,7,10,1]
        if (timestamp_dt.date().month in quarters):
            Q = quarters.index(timestamp_dt.date().month)+1
            YR = timestamp_dt.date().year
            if Q == 4:
                YR = YR -1
            print(',quarter={0}Q{1}'.format(YR, Q), end='')
            # Since we can't do comparison on tags (other then (in)equality) we have to include a field for this as well.
            # Will make queries slower, but that doesn't matter much in this use case

            fieldset['q']='{0}{1}i'.format(YR, Q)

        # Now print a space, everything after this are the fields (== measurements)
        print(' ', end='')

        fieldset['status'] = '"{}"'.format(domainresults['status'])
        if 'link' in domainresults:
            fieldset['url'] = '"{}"'.format(domainresults['link'])
        if 'score' in domainresults:
            fieldset['score'] = '{}i'.format(domainresults['score'])

        if 'categories' in domainresults:
            dom_cats = domainresults['categories']
            for cats in dom_cats:
                fieldset[v1_to_2_cats[measurementType][cats['category']]] = str(int(cats['passed']))+'i'

        if 'views' in domainresults:
            dom_views = domainresults['views']
            for views in dom_views:
                if views['name'] in specific_tests[measurementType]:
                    fieldset[views['name']] = str(int(views['result']))+'i'

        fldcnt = len(fieldset)
        for field in fieldset:
            print('{0}={1}'.format(field, fieldset[field]), end='')
            fldcnt = fldcnt-1
            if fldcnt>0:
                print(',',end='')

        print(' {}000000000'.format(int(timestamp_dt.timestamp())))


def JSONtoInflux2_0(data, domains_metadata, columns_to_add=['type']):
    """Display the results as a CSV file ready for import by influxdb"""

    pp = pprint.PrettyPrinter(indent=4)

    domains = data['domains']

    # Set index on pandas dataframe as the measurement type.
    # That column contains the domains as used in the measurements
    measurementType = ut.getMeasurementType2_0(domains)
    if not domains_metadata.empty:
        domains_metadata = domains_metadata.set_index(measurementType, inplace=False)

#    pp.pprint(domains_metadata)

    timestamp_dt = datetime.datetime.strptime(data['request']['finished_date'], '%Y-%m-%dT%H:%M:%S.%f%z')
    # Rework it to the first of the month for proper alignment

    for domainname in domains:
        domainresults = domains[domainname]

        # do we need type?
        tagset={}
        if not domains_metadata.empty:
            if domainname in domains_metadata.index :
                for md in columns_to_add:
                    tagset[md] = domains_metadata.at[domainname, md]

        print('{0},year={1},month={2},yearmonth={3},domain={4}'.format(measurementType, timestamp_dt.date().year, timestamp_dt.date().month, timestamp_dt.strftime('%Y-%m'), domainname), end='')
        for tag in tagset:
            print(',{0}={1}'.format(tag, tagset[tag]), end='')

        fieldset={}
        # Since we can't do comparison on tags (other then (in)equality) we have to include a field for this as well.
        # Will make queries (much) slower, but that doesn't matter much in this use case
        fieldset['ym'] = '{}i'.format(timestamp_dt.strftime('%Y%m'))

        quarters=[4,7,10,1]
        if (timestamp_dt.date().month in quarters):
            Q = quarters.index(timestamp_dt.date().month)+1
            YR = timestamp_dt.date().year
            if Q == 4:
                YR = YR -1
            print(',quarter={0}Q{1}'.format(YR, Q), end='')
            # Since we can't do comparison on tags (other then (in)equality) we have to include a field for this as well.
            # Will make queries slower, but that doesn't matter much in this use case

            fieldset['q']='{0}{1}i'.format(YR, Q)

        # Now print a space, everything after this are the fields (== measurements)
        print(' ', end='')

        fieldset['status'] = '"{}"'.format(domainresults['status'])
        if 'report' in domainresults:
            fieldset['url'] = '"{}"'.format(domainresults['report']['url'])
        if 'scoring' in domainresults:
            fieldset['score'] = '{}i'.format(domainresults['scoring']['percentage'])

        if 'results' in domainresults:
            dom_cats = domainresults['results']['categories']
            for cats in dom_cats:
                fieldset[cats] = str(int(dom_cats[cats]['status']=='passed'))+'i'

            if 'tests' in domainresults['results']:
                dom_views = domainresults['results']['tests']
                for views in dom_views:
                    if views in specific_tests[measurementType]:
#                        fieldset[views] = str(int(dom_views[views]['status'] == 'passed'))+'i'
# Also Warning should count as a pass
                        fieldset[views] = str(int(dom_views[views]['status']=='passed' or dom_views[views]['status']=='warning'))+'i'

        fldcnt = len(fieldset)
        for field in fieldset:
            print('{0}={1}'.format(field, fieldset[field]), end='')
            fldcnt = fldcnt-1
            if fldcnt>0:
                print(',',end='')

        print(' {}000000000'.format(int(timestamp_dt.timestamp())))


#############################################

pp = pprint.PrettyPrinter(indent=4)

if len(sys.argv) < 2 :
    print('Usage: ')
    print(sys.argv[0], ' <JSON results file|JSON results directory> [domains XLSX file] [metadata_column_name=\'type\'[,md_col_name2, ...]]')
    print('\nConverts internet.nl API json results to influxdb line format (to stdout)\n')
    quit(1)

# Create empty dataframe
domains = pd.DataFrame()
domainsFile=''
columns_to_add=[]

if len(sys.argv) > 2 :
    domainsFile = sys.argv[2]

if len(sys.argv) > 3 :
    columns_to_add = sys.argv[3].split(',')

#pp.pprint(columns_to_add)

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

#pp.pprint(filelist)
try:
    if not domainsFile == '':
        domains = pd.read_excel(domainsFile, sheet_name=0)
except Exception as e:
    print("error processing domains Excel file: {}".format(e))
    exit(1)

#pp.pprint(domains.head(10))

for fn in filelist:
    data = ut.openJSON(fn)
    JSONtoInflux(data, domains, columns_to_add)
# All done
