#!/usr/bin/env python

from multiprocessing import Pool
from urllib2 import urlopen
from urlparse import urlparse
import os, sys

from bs4 import BeautifulSoup

# TODO Move into separate config.
SCRAPE_URL = "http://storage.googleapis.com/books/ngrams/books/datasetsv2.html"
DATA_DIR = "/var/million-books/data"
CHUNK = 16 * 1024

soup = BeautifulSoup(urlopen(SCRAPE_URL))

# Get all of the gunzipped files from the 'a' tags on the page.
# At 30222 links as of 26 Juillet 2013.
links = [link for link in [a.get("href") for a in soup.findAll("a")] \
            if link.endswith("gz")]

def download(link):
    l = urlopen(link)
    name = urlparse(link).path.split('/')[-1]
    # TODO Ensure directory exists.
    # TODO Check that the file exists before overwriting it.
    with open(os.path.join(DATA_DIR,name), "wb") as fin:
        while True:
            chunk = l.read(CHUNK)
            if not chunk: break
            fin.write(chunk)
    print("%s written." % os.path.join(DATA_DIR, name))


def run():
    # What's a reasonable number here? The m1.large has 2 cores.
    pool = Pool(processes=50)
    # TODO Find a better way to end these processes.
    # NOTE End processes with `pkill -f python`
    pool.map(download, links)
