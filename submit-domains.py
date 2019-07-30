#! /usr/bin/env python3

import sys
import stat
import os
import csv
import requests
import json

import pprint

def fillDomains(filename, domains):
    """Fill domains dictionary from a CSV file and return it."""
    with open(filename, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=';', quotechar='|')
        # skip row containing column headers
        row = reader.__next__()
            #        if row:
        for row in reader:
            if row[1]:
                if not row[1] in domains:
                    domains[row[1]] = {}
                domains[row[1]]['mail'] = True
                domains[row[1]]['type'] = row[2]
                if row[3]:
                    domains[row[1]]['name'] = row[3]

            if row[0]:
                if not row[0] in domains:
                    domains[row[0]] = {}
                domains[row[0]]['web'] = True
                domains[row[0]]['type'] = row[2]
                if row[3]:
                    domains[row[0]]['name'] = row[3]
    return domains;

def filterDomains(allDomains, name= 'test', type = 'web'):
    """Retrieve all domains of a specific type (mail or web) from domains dictionary"""
    typeDomains = {}
    typeDomains['name'] = name
    typeDomains['domains'] = []

    for domain, metadata in allDomains.items():
        if type in metadata:
            typeDomains['domains'].append(domain)

    return typeDomains

def readCredentials(filename = 'credentials'):
    """Find login  password for batch.internet.nl from a netrc formatted file"""
    credentials = {'login': '', 'password':''}
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

if len(sys.argv) < 2 :
    print ('Usage: ')
    print (sys.argv[0], ' <mail|web> [name = \'Test\'] [domains CSV file = \'domains/domains.csv\']', )
    quit(1)


domainsfile = 'domains/domains.csv'
name = 'Test'
web = 'https://batch.internet.nl/api/batch/v1.1/web/'
mail = 'https://batch.internet.nl/api/batch/v1.1/mail/'
reqapi = web
allDomains = {}

pp = pprint.PrettyPrinter(indent=4)

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
    allDomains = fillDomains(domainsfile, allDomains)
except Exception as e:
    print("error processing domains CSV file: {}".format(e))
    exit(1)

submitDomains = filterDomains(allDomains, type=type, name=name)

#pp.pprint(submitDomains)

if type == 'mail':
    reqapi = mail

r = requests.post(reqapi, json=submitDomains, auth=(credentials['login'], credentials['password']) )

if r.status_code == 200 or r.status_code == 201:
    print('{}\n{}'.format(name, r.json()['data']['results']))
    try:
        writeGetResults('getResult-{}-{}'.format(name, type), r.json()['data']['results'])
    except Exception as e:
        print("error writing getresults file: {}".format(e))
else:
    print('Something went wrong! (Error = {})'.format(r.status_code))

# All done
