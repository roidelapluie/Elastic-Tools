# general helpers
YELLOW="1;43"
RED="1;41"
GREEN="1;42"

import re, calendar, datetime
import yaml, os
import time
from elasticsearch import Elasticsearch

from getpass import getpass

def colorize_day(textcalendar, day, color):
    """
    Takes a calendar, a day number and a colors
    and returns the calendar with the day in such a color.
    """
    if day < 10:
        regex = r"(\s%s)\b" % day
    else:
        regex = r"\b(%s)\b" % day

    return re.sub(regex, "\x1b[%sm\\1\x1b[0m" % color,  textcalendar)

def colorize(text, color):
    """
    Takes a calendar, a day number and a colors
    and returns the calendar with the day in such a color.
    """
    return "\x1b[%sm%s\x1b[0m" % (color,  text)


def make_calendar(year, month, opened, closed):
    """
    Takes a year, a month, and a list of open and closed days.
    Returns the calendar in str with colors
    """
    c = calendar.TextCalendar()
    year, month = int(year), int(month)
    textcalendar = c.formatmonth(year, month)
    for d in c.itermonthdays(year, month):
        if d in opened:
            if d in closed:
                textcalendar = colorize_day(textcalendar, d, YELLOW)
            else:
                textcalendar = colorize_day(textcalendar, d, GREEN)
        elif d in closed:
            textcalendar = colorize_day(textcalendar, d, RED)
    return textcalendar

def find_index_dates(index_name):
    """
    Look inside an index name for a date
    """
    day_match = re.search(r'\d{4}\.\d{2}\.\d{2}$', index_name)
    week_match = re.search(r'\d{4}\.\d{2}$', index_name)
    if day_match:
        return [datetime.datetime.strptime(day_match.group(), '%Y.%m.%d').date()]
    elif week_match:
        orig_date = datetime.datetime.strptime(week_match.group() + '.1', '%Y.%W.%w').date()
        days = []
        for x in range(0,7):
            dow = orig_date + datetime.timedelta(days=x)
            days.append(dow)
        return days
    else:
        return []

def print_calendar(*args):
    """
    Print the colored calendar made by make_calendar
    """
    print(make_calendar(*args))

def print_legend():
    """Print human readable color map to read the calendar"""
    print()
    print("Color code:")
    print("\x1b[%sm31\x1b[0m Day with open indices" % GREEN)
    print("\x1b[%sm31\x1b[0m Day with closed indices" % RED)
    print("\x1b[%sm31\x1b[0m Day with open and closed indices" % YELLOW)

def parse_cat_indices(indices_cat):
    """Reads the 'cat' api to get the list of open and closed indexes.
    Returns a list of tuples: (indice_name, is_closed)"""
    results = []
    for line in indices_cat.split('\n'):
        fields = line.split()
        if len(fields) == 9:
            assert fields[1] == 'open', 'Error while parsing opened line: %s' % line
            assert fields[2] is not None
            results.append((fields[2], False))
        elif len(fields) == 2:
            assert fields[0] == 'close', 'Error while parsing closed line: %s' % line
            assert fields[1] is not None
            results.append((fields[1], True))
        elif len(fields) >= 3:
            assert fields[0] in ('red', 'yellow', 'green'), 'Error while parsing partial line: %s' % line
            if fields[1] == 'close':
                results.append((fields[2], True))
            elif fields[1] == 'open':
                results.append((fields[2], False))
            else:
                raise Exception('Error while parsing %s (expecting close or open in 2nd column)' % line)
        elif len(fields) == 0:
            pass
        else:
            raise Exception('Error while parsing %s (incorrect number of fields)' % line)
    return results

def parse_arguments(arguments):
    """parses date arguments, returns a tuple with start and end date"""
    start = None
    end = None
    if arguments['--start-date'] is not None:
        start = datetime.datetime.strptime(arguments['--start-date'], '%d-%m-%Y').date()
    if arguments['--end-date'] is not None:
        end = datetime.datetime.strptime(arguments['--end-date'], '%d-%m-%Y').date()
    if start and end:
        assert start <= end
    return (start, end)


def close_index_by_name(client, index_name):
    """Close an index"""
    client.indices.close(index_name)

def open_index_by_name(client, index_name):
    """Open an index"""
    client.indices.open(index_name)


def get_cluster_config(arguments, option):
    if arguments['--config']:
        config_file = arguments['--config']
    else:
        config_file = 'clusters.yaml'
    if not os.path.isfile(config_file):
        raise Exception('Configuration file not found')
    if arguments[option]:
        cluster_name = arguments[option]
    else:
        cluster_name = 'test'
    clusters = yaml.load(open(config_file, 'r'))
    if not cluster_name in clusters:
        raise Exception('cluster %s not found in %s' % (cluster_name, config_file))
    if '--username' in arguments and arguments['--username']:
        u = arguments['--username']
        p = getpass()
    else:
        return clusters[cluster_name]
    results = []
    for config in clusters[cluster_name]:
        config['http_auth'] = (u,p)
        results.append(config)
    return results

def get_es_client(arguments, option='--cluster'):
    config = get_cluster_config(arguments, option)
    return Elasticsearch(config, timeout=300)

def get_index_mapping(client, index):
    return client.indices.get_mapping(index)

def wait_for_yellow_index(client, index):
    while time.sleep(1):
        indices = client.cat.indices()
        for line in indices.split('\n'):
            fields = line.split()
            if len(fields) >= 3:
                if fields[1] == 'open' and fields[0] in ('yellow', 'green') and fields[2] == index:
                    break


