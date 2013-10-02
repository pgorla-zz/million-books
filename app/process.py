#!/usr/bin/env python

from __future__ import print_function

import bz2
import os, sys
import random
import re

from multiprocessing import Pool

from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
import pycassa

import pysolr

# TODO Split out tasks into threads.
DATA_DIR = "data/"

solr = pysolr.Solr("http://localhost:8983/solr/resource/solr.person/")

# Connect to 'solr' keyspace
pool = ConnectionPool("solr", ["localhost:9160"])

location_cf = ColumnFamily(pool, "location")
person_cf = ColumnFamily(pool, "person")


kv_pattern = re.compile(r'''^\<http://dbpedia.org/resource/
        (?P<name>\w+)       # Resource name
        .*\"(?P<value>.*)   # Value
        \"\@en \.''', re.VERBOSE)

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
        .*\"(?P<date>.*)\" # Date
        ''', re.VERBOSE)


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
            "%s_person" % shortname: name
            })

        person_cf.insert(name, {
            "name": name,
            # Solr dynamic field names cannot have spaces.
            event: loc
            })

    if event_date_pattern.match(line):
        name = event_date_pattern.search(line).group('name').replace("_", " ")
        event = event_date_pattern.search(line).group('event')
        date = event_date_pattern.search(line).group('date')
        try:
            shortname = name.split()[0]
        except IndexError:
            shortname = name

        print(name, event, date)

        person_cf.insert(name, {
            "name": name,
            # Solr dynamic field names cannot have spaces.
            event: date
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
        person_cf.insert(name, {
            "name": name,
            # Solr dynamic field names cannot have spaces.
            value_tag: value
            })
    pass



if __name__ == "__main__":
    # NOTE: Pycassa not threadsafe; need to create a new
    # connection per thread.
    filename = sys.argv[1] if sys.argv[1] else "data/test"
    #threadpool = Pool(20)
    #for line in bz2.BZ2File("data/persondata_en.nt.bz2", "rb"):
    for line in open("%s/%s" % (DATA_DIR, filename), "rb"):
        parse_life(line)
    #with open("%s/%s" % (DATA_DIR, filename), "rb") as fin:
        #threadpool.map(parse_tags, fin, 5)
    #with bz2.BZ2File("data/geo_coordinates_en.nt.bz2", "rb") as fin:
    #   threadpool.map(parse_location, fin, 4)
