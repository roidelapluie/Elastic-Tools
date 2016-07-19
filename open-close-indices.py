#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""open-close-indexes

Usage:
    open-close-indices.py <action> [--start-date=STARTDATE] [--end-date=ENDDATE] [--config=CONFIG_FILE] [--cluster=CLUSTER] [--username=USERNAME] [--help]

Options:
    action status, close or open
    --start-date select indexes from that date (dd-mm-YYYY)
    --end-date select indexes until that date (dd-mm-YYYY)
    --cluster name of the cluster to test
    --config configuration file to use (see clusters.yaml.example)
    --username username

Examples:
    open-close-indices.py status --cluster=production
    open-close-indices.py close --end-date=31-03-2016 --cluster=production
    open-close-indices.py open --cluster=test
    open-close-indices.py close --end-date=$(date +%d-%m-%Y -d '31 days ago') --cluster=production
"""

# Test for Python 3 presence
import sys
if (sys.version_info[0] < 3):
    raise Exception("Python 3 needed. Version found is " + str(sys.version_info[0]) + "." + str(sys.version_info[1]) + "." + str(sys.version_info[2]))

# External modules
from docopt import docopt

# Local helpers
from elastic_utils import print_calendar, find_index_dates, parse_cat_indices, parse_arguments,\
                          close_index_by_name, open_index_by_name, print_legend, get_es_client

def open_close_indices(client, arguments, opening=True):
    start, end = parse_arguments(arguments)
    if not end and not opening:
        # Avoid closing everything
        raise Exception('You need at least an end date to close indices')
    indices = parse_cat_indices(client.cat.indices())
    todo = []
    for index_name, closed in indices:
        dates = find_index_dates(index_name)
        if dates == [] or not index_name:
            continue
        if start and min(dates) < start:
            continue
        if end and max(dates) > end:
            continue
        if closed == opening:
            todo.append(index_name)
    n=0
    m=len(todo)
    for index_name in todo:
        n+=1
        if opening:
            print("%s/%s: Opening index %s" % (n,m,index_name))
            open_index_by_name(client, index_name)
        else:
            print("%s/%s: Closing index %s" % (n,m,index_name))
            close_index_by_name(client, index_name)
    print_status(client, arguments)

def print_status(client, arguments):
    """Prints the status of the indexes (open, closed)
    """
    open_days = set()
    closed_days = set()
    results = {}
    open_indices = 0
    closed_indices = 0
    start, end = parse_arguments(arguments)
    for index_name, closed in parse_cat_indices(client.cat.indices()):
        dates = find_index_dates(index_name)
        if dates == [] or not index_name:
            continue
        if start and min(dates) < start:
            continue
        if end and max(dates) > end:
            continue
        if dates != []:
            if closed:
                closed_indices += 1
                closed_days |= set(dates)
            else:
                open_indices += 1
                open_days |= set(dates)
    for day in open_days | closed_days:
        key = '%s-%s' % (day.year, day.month)
        if key not in results:
            results[key] = {'open': [], 'closed': []}
        if day in open_days:
            results[key]['open'].append(day.day)
        if day in closed_days:
            results[key]['closed'].append(day.day)
    for yearmonth in sorted(results):
        year, month = yearmonth.split('-')
        print_calendar(year, month, results[yearmonth]['open'], results[yearmonth]['closed'])
    print_legend()
    print()
    print("Status:")
    print("Open indices (with a date): %s" % open_indices)
    print("Closed indices (with a date): %s" % closed_indices)

def main():
    arguments = docopt(__doc__, version='0.1')
    client = get_es_client(arguments)

    # Dispatch to the actual action
    if arguments['<action>']  == 'status':
        print_status(client, arguments)
    elif arguments['<action>']  == 'open':
        open_close_indices(client, arguments)
    elif arguments['<action>']  == 'close':
        open_close_indices(client, arguments, False)
    else:
        raise Exception('Unkown action: %s' % arguments['<action>'])

if __name__ == '__main__':
    main()
