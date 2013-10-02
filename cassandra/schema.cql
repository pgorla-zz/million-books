
create keyspace solr
    with replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
    };

create table location (
    id text primary key
) with compact storage;

create table person (
    id text primary key,
    name text,
    birthDate DateType,
    deathDate DateType,
    birthPlace text,
    deathPlace text
) with compact storage;
