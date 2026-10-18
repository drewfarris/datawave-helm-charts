ARG S3A_IMAGE=local/datawave/ingest-s3a:8.4.0-SNAPSHOT-hadoop3.4.2
ARG EXAMPLES_IMAGE=local/datawave/ingest-examples:8.4.0-SNAPSHOT-annotationfix
FROM ${S3A_IMAGE} AS s3a
FROM ${EXAMPLES_IMAGE}
USER root
COPY --from=s3a /usr/lib/hadoop/lib/hadoop-aws-3.4.2.jar /usr/lib/hadoop/lib/
COPY --from=s3a /usr/lib/hadoop/lib/bundle-2.29.52.jar /usr/lib/hadoop/lib/
COPY --from=s3a /usr/lib/hadoop/lib/analyticsaccelerator-s3-1.2.1.jar /usr/lib/hadoop/lib/
COPY --from=s3a /usr/lib/hadoop/lib/wildfly-openssl-2.1.4.Final.jar /usr/lib/hadoop/lib/
USER datawave
