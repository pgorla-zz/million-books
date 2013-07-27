Million Books
============

Search and analysis into the Million Books n-gram corpus using Cassandra,
Hadoop, and Solr.


Setup
------
Install the Python requirements with pip.

    # cd app
    # pip install -r requirements.txt


Download Data
-------------
Run the scraping program and [download](http://storage.googleapis.com/books/ngrams/books/datasetsv2.html)
the n-gram corpus from Google.

Index into Cassandra
--------------------
Index the data into Cassandra.
