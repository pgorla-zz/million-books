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

solr = pysolr.Solr("http://localhost:8983/solr/resource/solr.person/")

# Connect to 'solr' keyspace
pool = ConnectionPool("solr", ["localhost:9160"])

location_cf = ColumnFamily(pool, "location")
person_cf = ColumnFamily(pool, "person")


def parse_life(line):
    if event_loc_pattern.match(line):
        name = event_loc_pattern.search(line).group('name').replace("_", " ")
        event = event_loc_pattern.search(line).group('event')
        loc = event_loc_pattern.search(line).group('loc').replace("_", " ")
        print(name, event, loc)

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
        name = event_date_pattern.search(line).group('name').replace("_", " ")
        event = event_date_pattern.search(line).group('event')
        _date = event_date_pattern.search(line).group('date')
        try:
            date = datetime.strptime(_date, "%Y-%m-%d")
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

def parse_location(line):
    if kv_pattern.match(line):
        name = kv_pattern.search(line).group('name').replace("_", " ")
        location_cf.insert(name, {
            "name": name,
            "location": kv_pattern.search(line).group('value')
            })
    pass

def parse_tags(line):
    if kv_pattern.match(line):
        name = kv_pattern.search(line).group('name').replace("_", " ")
        value = str(kv_pattern.search(line).group('value'))
        value_tag = "%s_tag" % random.randint(0,10)
        print(name, value, value_tag)
        person_cf.insert(name, {
            "name": name,
            value_tag: value
            })
    pass


if __name__ == "__main__":
    # NOTE: Pycassa not threadsafe; need to create a new
    # connection per thread.
    try:
        filename = sys.argv[1]
    except IndexError:
        filename = "test"
    threadpool = Pool(20)
    #for line in bz2.BZ2File("data/persondata_en.nt.bz2", "rb"):
    with open("%s/%s" % (DATA_DIR, filename), "rb") as fin:
        threadpool.map(parse_tags, fin, 5)
        threadpool.map(parse_life, fin, 5)
        threadpool.map(parse_date, fin, 5)
    #with bz2.BZ2File("data/geo_coordinates_en.nt.bz2", "rb") as fin:
    #   threadpool.map(parse_location, fin, 4)
