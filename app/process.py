#!/usr/bin/env python

import bz2
import os, sys
import re
import uuid

from multiprocessing import Pool

from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

# TODO Split out tasks into threads.
DATA_DIR = "data/"

# Connect to 'solr' keyspace
pool = ConnectionPool("solr", ["localhost:9160"])
# Connect to 'geosearch' column family
cf = ColumnFamily(pool, "geosearch")


# Produces "57.151 -2.123"@en . 
point_pattern = re.compile("\"(.*)\"\@en \.")
name_pattern = re.compile("^\<http://dbpedia.org/resource/(\w+)")

pattern = re.compile(r'^\<http://dbpedia.org/resource/(?P<name>\w+).*\"(?P<point>.*)\"\@en \.')

def parse_line(line):
    if pattern.match(line):
        name = pattern.search(line).group('name')
        cf.insert(str(uuid.uuid4()), {
            "name": name.replace("_", " "),
            "location": pattern.search(line).group('point')
            })
    pass

if __name__ == "__main__":
    threadpool = Pool(10)
    with bz2.BZ2File("data/geo_coordinates_en.nt.bz2", "rb") as fin:
        threadpool.map(parse_line, fin, 4)
