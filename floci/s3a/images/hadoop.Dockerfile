ARG BASE_IMAGE
FROM ${BASE_IMAGE}
USER root
COPY libs/ /usr/lib/hadoop/lib/
# The image's Hadoop and Accumulo launchers already include this directory.
RUN chmod a+r /usr/lib/hadoop/lib/*.jar
