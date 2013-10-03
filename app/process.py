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

import pysolr

pool = ConnectionPool("solr", ["localhost:9160"])
location_cf = ColumnFamily(pool, "location")
person_cf = ColumnFamily(pool, "person")


# TODO Split out tasks into threads.
DATA_DIR = "data/"

kv_pattern = re.compile(r'''^\<http://dbpedia.org/resource/
        (?P<name>\w+)       # Resource name
        (?!.*Date> )        # Don't match birthdate, deathdate
        .*\"(?P<value>.*)   # Value
        \"''', re.VERBOSE)

# Birth and death place
event_loc_pattern = re.compile(r'''^\<http://dbpedia.org/resource/
        (?P<name>\w+)       # Resource name
        .*\>.*\<http://dbpedia.org/ontology/
        (?P<event>birthPlace|deathPlace)\> # Birth or death
        .*\<http://dbpedia.org/resource/
        (?P<loc>.*)       # Location in which event occurred.
        \> .''', re.VERBOSE)

# Birth and death date
event_date_pattern = re.compile(r'''^\<http://dbpedia.org/resource/
        (?P<name>\w+)       # Resource name
        .*\>.*\<http://dbpedia.org/ontology/
        (?P<event>birthDate|deathDate)\> # Birth or death
        .*\"(?P<date>.*)\"  # Date
        ''', re.VERBOSE)


def parse_life(line):
    if event_loc_pattern.match(line):
        vals = event_loc_pattern.search(line).groupdict()
        name = vals['name'].replace("_", " ")
        event = vals['event']
        loc = vals['loc'].replace("_", " ")
        try:
            shortname = name.split()[0]
        except IndexError:
            shortname = name
        # TODO Counter column of births, deaths
        location_cf.insert(loc, {
            "%s_person" % shortname: str(name)
            })
        person_cf.insert(name, {
            "name": name,
            event: loc
            })

def parse_date(line):
    if event_date_pattern.match(line):
        vals = event_date_pattern.search(line).groupdict()
        name = vals['name'].replace("_", " ")
        event = vals['event']
        try:
            date = datetime.strptime(vals['date'], "%Y-%m-%d")
        except ValueError:
            date = None
        try:
            shortname = name.split()[0]
        except IndexError:
            shortname = name
        person_cf.insert(name, {
            "name": name,
            # Manually force into UTC time.
            event: "%sZ" % date.isoformat()
            })

# Only for geo-data
def parse_location(line):
    if kv_pattern.match(line):
        vals = kv_pattern.search(line).groupdict()
        name = vals['name'].replace("_", " ")
        print(vals)
        location_cf.insert(
            name, {
            "name": name,
            "location": vals['value']
            })
    pass

def parse_tags(line):
    if kv_pattern.match(line):
        vals = kv_pattern.search(line).groupdict()
        name = vals['name'].replace("_", " ")
        value = str(vals['value'])
        value_tag = "%s_tag" % random.randint(0,10)
        person_cf.insert(
            name, {
            "name": name,
            value_tag: value
            })
    pass

def parse(line):
    pool = ConnectionPool("solr", ["localhost:9160"])
    location_cf = ColumnFamily(pool, "location")
    person_cf = ColumnFamily(pool, "person")

    for f in [parse_tags, parse_location, parse_life, parse_date]:
        f(line)



if __name__ == "__main__":
    # NOTE: Pycassa not threadsafe; need to create a new
    # connection per thread.
    try:
        filename = sys.argv[1]
    except IndexError:
        filename = "test"
    threadpool = Pool(20)
    for line in open("data/geo_coordinates_en", "rb"):
        parse_location(line)
    #with open("%s/%s" % (DATA_DIR, filename), "rb") as fin:
    #    threadpool.map(parse, fin, 5)
    #with bz2.BZ2File("data/geo_coordinates_en.nt.bz2", "rb") as fin:
    #   threadpool.map(parse_location, fin, 4)
