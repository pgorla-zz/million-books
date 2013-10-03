Million Books
============

Search and analysis into books from Wikipedia using Cassandra,
Hadoop, and Solr.


Setup
------
Install the Python requirements with pip.

    # cd app
    # pip install -r requirements.txt


Download Data
-------------
Download the persondata and geo\_coordinates from dbpedia.org.

Index into Cassandra
--------------------
Index the data into Cassandra by running the processor:

    $ python process.py

Notes
-----
This cassandra.yaml is symlinked to /etc/dse/cassandra/cassandra.yaml to
keep production up to date.

You may see `Cannot determine CASSANDRA_CONF` after trying to start the
dse service.

Append `CASSANDRA_CONF="/etc/dse/cassandra"` to `/etc/init.d/dse`.
