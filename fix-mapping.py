#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""find-mapping.py

Usage:
    fix-mapping <old-index> <new-index> <mapping-name> <field>... [--config=CONFIG_FILE] [--cluster=CLUSTER] [--help]

Options:
    old-index: name of the existing index
    new-index: name of the new index
    mapping-name: name of the mapping
    field: field definition. name=type

Examples:
    python fix-mapping.py logstash-suricata-2016.04.21 logstash-suricata-geo-2016.04.21 suricata src.geo.location=geo_point dest.geo.location=geo_point
"""

# Test for Python 3 presence
import sys
if (sys.version_info[0] < 3):
    raise Exception("Python 3 needed. Version found is " + str(sys.version_info[0]) + "." + str(sys.version_info[1]) + "." + str(sys.version_info[2]))

# External modules
from docopt import docopt

# Local helpers
from elastic_utils import get_es_client, reindex, add_mapping

def fix_mapping(client, arguments):
    ni = arguments['<new-index>']
    oi = arguments['<old-index>']
    mn = arguments['<mapping-name>']
    fields = arguments['<field>']
    add_mapping(client, ni, mn, fields)
    reindex(client, oi, ni)

def main():
    # Parse arguments
    arguments = docopt(__doc__, version='0.1')

    client = get_es_client(arguments)

    # Dispatch to the actual action
    fix_mapping(client, arguments)

if __name__ == '__main__':
    main()
