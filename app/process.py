#!/usr/bin/env python

from __future__ import print_function

import bz2
import os, sys
import random
import re

from datetime import datetime
from multiprocessing import Pool

from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
import pycassa


# TODO Split out tasks into threads.
DATA_DIR = "data/"
# Key-value pattern
kv_pattern = re.compile(r'''^\<http://dbpedia.org/resource/
        (?P<name>\w+)       # Resource name
        (?!.*Date> )        # Don't match birthdate, deathdate
        .*\"(?P<value>.*)   # Value
        \"''', re.VERBOSE)
# Birth and death place
birth_place_pattern = re.compile(r'''^\<http://dbpedia.org/resource/
        (?P<name>\w+)       # Resource name
        .*\>.*\<http://dbpedia.org/ontology/
        (?P<event>birthPlace|deathPlace)\> # Birth or death
        .*\<http://dbpedia.org/resource/
        (?P<loc>.*)       # Location in which event occurred.
        \> .''', re.VERBOSE)
# Birth and death date
birth_date_pattern = re.compile(r'''^\<http://dbpedia.org/resource/
        (?P<name>\w+)       # Resource name
        .*\>.*\<http://dbpedia.org/ontology/
        (?P<event>birthDate|deathDate)\> # Birth or death
        .*\"(?P<date>.*)\"  # Date
        ''', re.VERBOSE)
event_loc_pattern = re.compile(r'''\<http://dbpedia.org/resource/
        (?P<name>\w+)       # Resource name
        .*\>.*(?P<event>lat|long|point) # Lat / Long / Point
        \>.*\"(?P<value>.*)\"
        ''', re.VERBOSE)


def parse(line):
    pool = ConnectionPool("solr", ["localhost:9160"])
    location_cf = ColumnFamily(pool, "location")
    person_cf = ColumnFamily(pool, "person")

    batch = pycassa.batch.Mutator(pool) # default queue_size=100

    count = 0
    for f in [parse_birth_place, parse_birth_date, parse_tags]:
        vals = f(line)
        if vals is not None:
            sys.stdout.write("\rIndexing %s documents." % count)
            sys.stdout.flush()
            count +=1
            batch.insert(person_cf, *vals)

    batch.send()

def match_pattern(pattern):
    """Check for pattern match before processing."""
    def decorator(func):
        def wrapped(line):
            if pattern.match(line):
                vals = pattern.search(line).groupdict()
                for k,v in vals.items():
                    vals[k] = str(v.replace("_", " "))
                return func(vals)
        return wrapped
    return decorator


### Person Parsers ###

@match_pattern(birth_place_pattern)
def parse_birth_place(vals):
    return (vals['name'], {
        "name": vals['name'],
        vals['event']: vals['loc']
        })

@match_pattern(birth_date_pattern)
def parse_birth_date(vals):
    try:
        date = datetime.strptime(vals['date'], "%Y-%m-%d")
    except ValueError:
        date = datetime.now()
    try:
        shortname = vals['name'].split()[0]
    except IndexError:
        shortname = vals['name']

    return (vals['name'], {
        "name": vals['name'],
        # Manually force into UTC time.
        vals['event']: "%sZ" % date.isoformat()
        })

@match_pattern(kv_pattern)
def parse_tags(vals):
    return (vals['name'], {
        "name": vals['name'],
        "%s_tag" % random.randint(0,10): vals['value']
        })

### Location Parsers ###

@match_pattern(event_loc_pattern)
def parse_location(vals):
    if vals['event'] == "point":
        point = vals['value'].split(" ")
        vals['value'] = ','.join(point)
    return (vals['name'], {
        "name": vals['name'],
        vals['event']: vals['value']
        })

@match_pattern(birth_place_pattern)
def parse_birth_place_to_location(vals):
    try:
        shortname = vals['name'].split()[0]
    except IndexError:
        shortname = vals['name']
    # TODO Counter column of births, deaths
    return (vals['loc'], {
        "%s_person" % shortname: vals['name']
        })


if __name__ == "__main__":
    # NOTE: Pycassa not threadsafe; need to create a new
    # connection per thread.
    try:
        filename = sys.argv[1]
    except IndexError:
        filename = "test"
    threadpool = Pool(20)
    #for line in open("data/persondata", "rb"):
        #parse(line)
    with bz2.BZ2File("data/persondata_en.nt.bz2", "rb") as fin:
       threadpool.map(parse, fin, 4)
