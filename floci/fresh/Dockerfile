ARG BASE_IMAGE=local/datawave/accumulo-s3a:2.1.4-hadoop3.4.1
FROM ${BASE_IMAGE}
USER root
COPY HybridBootstrap.java /tmp/HybridBootstrap.java
RUN mkdir -p /opt/datawave-bootstrap/classes && \
    javac --release 11 -cp '/opt/accumulo/lib/*:/usr/lib/hadoop/*:/usr/lib/hadoop/lib/*:/usr/lib/zookeeper/*' \
      -d /opt/datawave-bootstrap/classes /tmp/HybridBootstrap.java && \
    jar --create --file /opt/datawave-bootstrap/bootstrap.jar -C /opt/datawave-bootstrap/classes . && \
    rm -rf /opt/datawave-bootstrap/classes /tmp/HybridBootstrap.java && \
    chmod -R a+rX /opt/datawave-bootstrap
USER accumulo
