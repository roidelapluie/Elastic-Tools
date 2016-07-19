#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""open-close-indexes

Usage:
    find-mapping-conflicts.py [--config=CONFIG_FILE] [--cluster=CLUSTER] [--username=USERNAME] [--help]

Options:
    --cluster name of the cluster to test
    --config configuration file to use (see clusters.yaml.example)
    --username username

Examples:
    find-mapping-conflicts.py --cluster=production
"""

# Test for Python 3 presence
import sys
if (sys.version_info[0] < 3):
    raise Exception("Python 3 needed. Version found is " + str(sys.version_info[0]) + "." + str(sys.version_info[1]) + "." + str(sys.version_info[2]))

# External modules
from docopt import docopt

# Local helpers
from elastic_utils import parse_cat_indices, get_es_client, get_index_mapping, colorize

def find_r_properties(fields, prefix=""):
    results = []
    to_analyze = []
    if 'properties' in fields:
        to_analyze.append(fields['properties'])
    if 'fields' in fields:
        to_analyze.append(fields['fields'])
    for analyzed in to_analyze:
        for prop in analyzed:
            if 'type' in analyzed[prop]:
                results.append(
                    ('%s%s' % (prefix, prop), analyzed[prop]['type'])
                )
            results += find_r_properties(analyzed[prop], '%s%s.' % (prefix, prop))
    return results





def find_mapping_conflicts(client):
    mappings = {}
    for index_name, closed in parse_cat_indices(client.cat.indices()):
        if not closed and index_name.startswith('logstash-'):
            for mapping, fields in get_index_mapping(client, index_name)[index_name]['mappings'].items():
                if 'properties' in fields:
                    mappings[index_name] = {}
                    for key, field_type in find_r_properties(fields):
                        mappings[index_name][key] = field_type
    final_results = {}
    for index in mappings:
        for key, field_type in mappings[index].items():
            if not key in final_results:
                final_results[key] = {}
            if not field_type in final_results[key]:
                final_results[key][field_type] = []
            final_results[key][field_type].append(index)
    for key, types in final_results.items():
        if len(types.keys()) == 1:
            print('%s is %s in %s indices [%s]' % (key,
                                                    list(types.keys())[0],
                                                    len(list(types.values())[0]),
                                                    colorize(' OK  ', 32)))
        else:
            for field_type, indices in types.items():
                print('%s is %s in %s indices [%s]' % (key,
                                                        field_type,
                                                        len(indices),
                                                        colorize(' NOK ', 31)))
                for i in indices:
                    print(' - %s' % i)



def main():
    arguments = docopt(__doc__, version='0.1')
    client = get_es_client(arguments)
    find_mapping_conflicts(client)



if __name__ == '__main__':
    main()
