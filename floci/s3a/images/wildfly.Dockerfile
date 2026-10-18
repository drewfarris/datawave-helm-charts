ARG BASE_IMAGE
FROM ${BASE_IMAGE}
USER root
COPY libs/ /opt/jboss/wildfly/modules/org/apache/hadoop/common/main/
RUN sed -i '/<\/resources>/i\        <resource-root path="hadoop-aws-3.4.1.jar"/>\n        <resource-root path="bundle-2.24.6.jar"/>\n        <resource-root path="wildfly-openssl-1.1.3.Final.jar"/>' /opt/jboss/wildfly/modules/org/apache/hadoop/common/main/module.xml && \
    chmod a+r /opt/jboss/wildfly/modules/org/apache/hadoop/common/main/*.jar && \
    mv /opt/jboss/wildfly/modules/org/apache/hadoop/common/main/hadoop-conf /opt/jboss/wildfly/modules/org/apache/hadoop/common/main/hadoop-conf.image && \
    ln -s /etc/hadoop/conf /opt/jboss/wildfly/modules/org/apache/hadoop/common/main/hadoop-conf
USER datawave
