# Build with the DataWave SOURCE repository as context (see README).
# Match this base image to the ingest image running in your stack.
ARG INGEST_IMAGE=local/datawave/ingest-kubernetes:sandbox-20261011-hadoop341
FROM ${INGEST_IMAGE}
USER root
RUN mkdir -p /opt/datawave-examples/default-config && \
    cp /opt/datawave-ingest/current/config/wikipedia-ingest-config.xml \
       /opt/datawave-ingest/current/config/annotation-ingest-config.xml \
       /opt/datawave-ingest/current/config/myjson-ingest-config.xml \
       /opt/datawave-ingest/current/config/mycsv-ingest-config.xml \
       /opt/datawave-examples/default-config/
COPY contrib/datawave-quickstart/bin/services/datawave/bootstrap-ingest.sh /opt/datawave-examples/bootstrap-ingest.sh
COPY warehouse/ingest-wikipedia/src/test/resources/input/enwiki-20130305-pages-articles-brief.xml /opt/datawave-examples/source/warehouse/ingest-wikipedia/src/test/resources/input/
COPY warehouse/ingest-wikipedia/src/test/resources/input/enwiki-20250519-pages-articles-medium.xml.gz /opt/datawave-examples/source/warehouse/ingest-wikipedia/src/test/resources/input/
COPY warehouse/ingest-wikipedia/src/test/resources/input/dewiki-20250520-pages-articles-brief.xml /opt/datawave-examples/source/warehouse/ingest-wikipedia/src/test/resources/input/
COPY warehouse/ingest-wikipedia/src/test/resources/input/eswiki-20250520-pages-articles-brief.xml /opt/datawave-examples/source/warehouse/ingest-wikipedia/src/test/resources/input/
COPY warehouse/ingest-wikipedia/src/test/resources/input/frwiki-20250520-pages-articles-brief.xml /opt/datawave-examples/source/warehouse/ingest-wikipedia/src/test/resources/input/
COPY warehouse/ingest-csv/src/test/resources/input/my.csv /opt/datawave-examples/source/warehouse/ingest-csv/src/test/resources/input/
COPY warehouse/ingest-json/src/test/resources/input/tvmaze-api.json /opt/datawave-examples/source/warehouse/ingest-json/src/test/resources/input/
COPY warehouse/ingest-annotation/src/test/resources/input/doubleAnnotation.json /opt/datawave-examples/source/warehouse/ingest-annotation/src/test/resources/input/
RUN chmod -R a+rX /opt/datawave-examples
USER datawave
