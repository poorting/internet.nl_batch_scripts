#! /usr/bin/env python3

import sys
import stat
import os
import csv
import requests
import json
import pandas as pd
import math

import pprint

def filterDomains(allDomains, name= 'test', type = 'web'):
    """Retrieve all domains of a specific type (mail or web) from domains Dataframe"""
    typeDomains = {}
    typeDomains['name'] = name
#    typeDomains['domains'] = []

    pp = pprint.PrettyPrinter(indent=4)

    # Simply get the column with the same name as the measurement type
    if type in allDomains.columns:
        domains = allDomains[type].dropna().tolist()
        typeDomains['domains'] = domains
    return typeDomains

def readCredentials(filename = 'credentials'):
    """Find login  password for batch.internet.nl from a netrc formatted file"""
    credentials = {'login':'', 'password':''}
    words=[];
    with open(filename, 'r') as creds:
        for line in creds:
            line = line.strip()
            words = words + line.split()

    for i in range(0, len(words), 6):
        endpoint = words[i:i+6]
        if endpoint[1].endswith('batch.internet.nl'):
            credentials[endpoint[2]] = endpoint[3]
            credentials[endpoint[4]] = endpoint[5]
            break;

    return credentials

def writeGetResults(name, url):
    r"""Writes the file with the curl command to retrieve the results"""
    with open(name, 'w') as getres:
        getres.write('#!/bin/bash\n')
        getres.write('curl -s --netrc-file credentials {}'.format(url))
        getres.close()
    # Make the file executable
    st = os.stat(name)
    os.chmod(name, st.st_mode | stat.S_IEXEC)

#############################################
pp = pprint.PrettyPrinter(indent=4)

if len(sys.argv) < 2 :
    print ('Usage: ')
    print (sys.argv[0], ' <mail|web> [name = \'Test\'] [domains file = \'domains/domains.xlsx\']', )
    quit(1)


domainsfile = 'domains/domains.xlsx'
name = 'Test'
web = 'https://batch.internet.nl/api/batch/v1.1/web/'
mail = 'https://batch.internet.nl/api/batch/v1.1/mail/'
reqapi = web
allDomains = {}
domains = pd.DataFrame()

type = sys.argv[1]
if len(sys.argv) > 2 :
    name = sys.argv[2]


if len(sys.argv) > 3 :
    domainsfile = sys.argv[3]

try:
    credentials = readCredentials()
except Exception as e:
    print("error opening/processing credentials file: {}".format(e))
    exit(1)

try:
    domains = pd.read_excel(domainsfile, sheet_name=0)
except Exception as e:
    print("error processing domains Excel file: {}".format(e))
    exit(1)

submitDomains = filterDomains(domains, type=type, name=name)

if type == 'mail':
    reqapi = mail

if 'domains' in submitDomains:
    r = requests.post(reqapi, json=submitDomains, auth=(credentials['login'], credentials['password']) )

    if r.status_code == 200 or r.status_code == 201:
        print('{}\n{}'.format(name, r.json()['data']['results']))
        try:
            writeGetResults('getResult-{}-{}'.format(name, type), r.json()['data']['results'])
            print('Retrieve results by calling ./getResult-{}-{}'.format(name, type))
        except Exception as e:
            print("error writing getresults file: {}".format(e))
    else:
        print('Something went wrong! (Error = {})'.format(r.status_code))
else:
    pp.pprint("NO domains (column) found of type/name \'{}\' !".format(type))


# All done
