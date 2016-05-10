#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""shards-management

Usage:
    shards-management.py replicas [--config=CONFIG_FILE] [--cluster=CLUSTER]
    shards-management.py replicas get <number> [--config=CONFIG_FILE] [--cluster=CLUSTER]
    shards-management.py replicas change <number> [--config=CONFIG_FILE] [--cluster=CLUSTER]

Options:
    action: status, replicas
    --cluster: name of the cluster to test
    --config: configuration file to use (see clusters.yaml.example)

Examples:
    shards-management.py status --cluster=production

    Get an overview of replicas:
        shards-management.py replicas --cluster=production

    Get the indexes with 2 replicas:
        shards-management.py replicas get 2
"""

# Test for Python 3 presence
import sys
if (sys.version_info[0] < 3):
    raise Exception("Python 3 needed. Version found is " + str(sys.version_info[0]) + "." + str(sys.version_info[1]) + "." + str(sys.version_info[2]))

# External modules
from docopt import docopt

# Local helpers
from elastic_utils import get_es_client, parse_cat_indices

def print_status(client):
    print(client.cat.shards())

def print_replicas_number(client, details, number):
    indices = parse_cat_indices(client.cat.indices())
    results = {}
    for (index, is_closed) in indices:
        settings = client.indices.get_settings(index)[index]['settings']
        nr_of_replicas = settings['index']['number_of_replicas']
        if not nr_of_replicas in results:
            results[nr_of_replicas] = []
        results[nr_of_replicas].append(index)
    if details:
        if number:
            for index in results[number]:
                print('%s: %s replicas' % (index, number))
        else:
            for number in results:
                for index in results[number]:
                    print('%s: %s replicas' % (index, number))
    else:
        for number in results:
            print('%s indices with %s replicas' % (len(results[number]), number))

def change_replicas_number(client, number):
    indices = parse_cat_indices(client.cat.indices())
    for (index, is_closed) in indices:
        if is_closed:
            print('Skip closed index %s' % index)
            continue
        settings = client.indices.get_settings(index)[index]['settings']
        nr_of_replicas = settings['index']['number_of_replicas']
        if int(number) == int(nr_of_replicas):
            print('Skip index %s (number of replica is already %s)' % (index, number))
            continue
        print('Change the number of replicas for %s' % index)
        client.indices.put_settings({"number_of_replicas" : int(number)}, index)

def main():
    # Parse arguments
    arguments = docopt(__doc__, version='0.1')

    client = get_es_client(arguments)

    # Dispatch to the actual action
    if arguments['status']:
        print_status(client)
    elif arguments['replicas']:
        if arguments['change']:
            change_replicas_number(client, arguments['<number>'])
        else:
            print_replicas_number(client, arguments['get'], arguments['<number>'])

if __name__ == '__main__':
    main()
