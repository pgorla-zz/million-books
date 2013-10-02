#!/bin/bash

echo "Uploading location schema"
curl http://localhost:8983/solr/resource/solr.location/schema.xml \
    --data-binary @solr/location_schema.xml \
    -H 'Content-type:text/xml; charset=utf-8'

echo "Uploading location solrconfig"
curl http://localhost:8983/solr/resource/solr.location/solrconfig.xml \
    --data-binary @solr/location_solrconfig.xml \
    -H 'Content-type:text/xml; charset=utf-8'

echo "Uploading person schema"
curl http://localhost:8983/solr/resource/solr.person/schema.xml \
    --data-binary @solr/person_schema.xml \
    -H 'Content-type:text/xml; charset=utf-8'

echo "Upload person solrconfig"
curl http://localhost:8983/solr/resource/solr.person/solrconfig.xml \
    --data-binary @solr/person_solrconfig.xml \
    -H 'Content-type:text/xml; charset=utf-8'

#echo "Creating location core"
#curl "http://localhost:8983/solr/admin/cores?action=CREATE&name=solr.location"

#echo "Creating person core"
curl "http://localhost:8983/solr/admin/cores?action=CREATE&name=solr.person"

echo "Reloading location core"
curl "http://localhost:8983/solr/admin/cores?action=RELOAD&name=solr.location"

echo "Reloading person core"
curl "http://localhost:8983/solr/admin/cores?action=RELOAD&name=solr.person"

# echo "Deleting data"
#curl http://localhost:8983/solr/solr.person/update --data \
    #'<delete><query>*:*</query></delete>' -H 'Content-type:text/xml; charset=utf-8'
