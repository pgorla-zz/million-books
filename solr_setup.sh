#!/bin/bash

USAGE="sh solr_setup.sh {create|reload|delete} core-name"

cmd=$1
core=$2

echo $USAGE

function upload {
    upload_core=$1
    curl http://localhost:8983/solr/resource/solr.$upload_core/schema.xml \
        --data-binary @solr/$upload_core\_schema.xml \
        -H 'Content-type:text/xml; charset=utf-8'

    curl http://localhost:8983/solr/resource/solr.$upload_core/solrconfig.xml \
        --data-binary @solr/$upload_core\_solrconfig.xml \
        -H 'Content-type:text/xml; charset=utf-8'
}


case $cmd in
    create)
        echo "Uploading schema, solrconfig and creating core for $core"
        upload $core
        curl "http://localhost:8983/solr/admin/cores?action=CREATE&name=solr.$core"
        ;;
    reload)
        echo "Uploading schema, solrconfig and reloading core for $core"
        upload $core
        curl "http://localhost:8983/solr/admin/cores?action=RELOAD&name=solr.$core"
        ;;
    delete)
        echo "Deleting all data from $core"
        curl http://localhost:8983/solr/solr.$core/update --data \
            '<delete><query>*:*</query></delete>' -H 'Content-type:text/xml; charset=utf-8'
        ;;
esac
