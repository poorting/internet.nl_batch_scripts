#! /usr/bin/env python3

import sys
import os
import csv
import requests
import json

import pprint

standard_keys = ['domain', 'score', 'status']

categories_keys_ = { 'mail' : ['ipv6','dnssec', 'auth', 'tls'],
    'web' : ['ipv6','dnssec', 'tls']}

# Old views
#views_keys_ = {'mail' : ['tls_available','dkim', 'dmarc', 'spf'],
#    'web' : ['tls_available', 'tls_ncsc_web']}

# new views (July 2018)
# for web the category 'tls' is equal to the previous view 'tls_ncsc_web',
# so essentially views for web are no longer needed
views_keys_ = {'mail' : ['mail_starttls_tls_available','mail_auth_dkim_exist', 'mail_auth_dmarc_policy', 'mail_auth_spf_policy'],
    'web' : []}




def openJSON(filename):
    """open the JSON (result) file and return it as a json structure"""
    data = {}
    with open(filename) as f:
        data = json.load(f)
    return data

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

def getType(domain, allDomains):
    """get the type of the domain and return it"""
    domType = '*unknown*'
    if domain in allDomains:
        domEntry = allDomains[domain]
        if 'type' in domEntry:
            domType = domEntry['type']
    return domType

def getMeasurementType(domains):
    """Determine wether measurement results are for web or mail"""
    measurementType = 'web'
    testDomain = domains[0]
    for category in testDomain['categories']:
        if (category['category'] == 'auth'):
            measurementType = 'mail'
            break

    return measurementType

def displayJSON(data, allDomains):
    """Display the results as a CSV file with headers for easy import"""
    payload = data['data']
    #    print('api-version: {}'.format(payload['api-version']))
    #    print('name: {}'.format(payload['name']))
    #    print('submission-date: {}'.format(payload['submission-date']))
    #    print('finished-date: {}'.format(payload['finished-date']))

    domains = payload['domains']

    datakey = getMeasurementType(domains)

    if not datakey in categories_keys_:
        return

    if allDomains:
        print('type;', end='')

    cat_keys = categories_keys_[datakey]
    view_keys = views_keys_[datakey]

    for v in standard_keys:
        print('{};'.format(v), end='')
    for v in cat_keys:
        print('{};'.format(v), end='')
    for v in view_keys:
        print('{};'.format(v), end='')
    print()

    for domain in domains:
        # do we need type?
        if allDomains:
            if domain['domain'] in allDomains:
                print('{};'.format(allDomains[domain['domain']]['type']), end='')
            else:
                print('<UNKNOWN>;', end='')
        # Omwerken
        csv = {}
        for v in standard_keys:
            csv[v] = domain[v]
        dom_cats = domain['categories']
        for cats in dom_cats:
            csv[cats['category']] = cats['passed']
        dom_views = domain['views']
        for views in dom_views:
            csv[views['name']] = views['result']

        for v in standard_keys:
            if v in csv:
                print('{};'.format(csv[v]), end='')
            else :
                print('*;', end='')
        for v in cat_keys:
            if v in csv:
                print('{};'.format(int(csv[v])), end='')
            else :
                print('*;', end='')
        for v in view_keys:
            if v in csv:
                print('{};'.format(int(csv[v])), end='')
            else :
                print('*;', end='')
        print()


#############################################

if len(sys.argv) < 2 :
    print ('Usage: ')
    print (sys.argv[0], ' <JSON results file> [domains CSV file]')
    quit(1)

allDomains = {}
domainsFile=''
pp = pprint.PrettyPrinter(indent=4)

if len(sys.argv) > 2 :
    domainsFile = sys.argv[2]

    try:
        data = openJSON(sys.argv[1])
    except Exception as e:
        print("Error reading file:{}\r\n Did you specify a JSON file?".format(sys.argv[1]))
        print("Error is:{}".format(e))
    else:
        if not domainsFile == '':
            allDomains = fillDomains(domainsFile, allDomains)

        displayJSON(data, allDomains)

# All done
