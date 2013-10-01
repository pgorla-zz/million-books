
create keyspace solr
    with replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
    };

create table location (
    id text primary key
);

create table person (
    id text primary key
);
