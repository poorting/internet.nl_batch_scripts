#! /usr/bin/env python3

import sys
import stat
import os
import csv
import requests
import json
import pandas as pd
import math
from lib import utils as ut
import pprint

def filterDomains(allDomains, name= 'test', type = 'web'):
    """Retrieve all domains of a specific type (mail or web) from domains Dataframe"""
    typeDomains = {}
    typeDomains['name'] = name
    typeDomains['type'] = type

    pp = pprint.PrettyPrinter(indent=4)

    # Simply get the column with the same name as the measurement type
    if type in allDomains.columns:
        domains = allDomains[type].dropna().tolist()
        typeDomains['domains'] = domains
    return typeDomains

#############################################
pp = pprint.PrettyPrinter(indent=4)

if len(sys.argv) < 2 :
    print ('Usage: ')
    print (sys.argv[0], ' <mail|web> [name = \'Test\'] [domains file = \'domains/domains.xlsx\'] [sheet_name=0]', )
    quit(1)


domainsfile = 'domains/domains.xlsx'
name = 'Test'
batchapi = 'https://batch.internet.nl/api/batch/v2'
sheet_name = 0
allDomains = {}
domains = pd.DataFrame()

type = sys.argv[1]
if len(sys.argv) > 2 :
    name = sys.argv[2]


if len(sys.argv) > 3 :
    domainsfile = sys.argv[3]

if len(sys.argv) > 4 :
    sheet_name = sys.argv[4]

try:
    credentials = ut.readCredentials()
except Exception as e:
    print("error opening/processing credentials file: {}".format(e))
    exit(1)

try:
    domains = pd.read_excel(domainsfile, sheet_name=sheet_name)
except Exception as e:
    print("error processing domains Excel file: {}".format(e))
    exit(1)

submitDomains = filterDomains(domains, type=type, name=name)

if 'domains' in submitDomains:

    r = requests.post(batchapi+'/requests', json=submitDomains, auth=(credentials['login'], credentials['password']) )

    if r.status_code == 200 or r.status_code == 201:
        print('Result = {} - {})'.format(r.status_code, r.json()))
        # Poll status with get-request script
    else:
        print('Something went wrong! (Error = {} - {})'.format(r.status_code, r.text))
else:
    pp.pprint("NO domains (column) found of type/name \'{}\' !".format(type))


# All done
