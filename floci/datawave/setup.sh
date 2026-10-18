#!/usr/bin/env bash
set -eo pipefail
export HADOOP_CONF_DIR=/usr/local/hadoop/conf
export BASE_WORK_DIR=/datawave/floci-lab/ingest/work
export ADDITIONAL_INGEST_LIBS=${ADDITIONAL_INGEST_LIBS:?Runtime-matched libraries are required}
for library in /lib/hadoop/hadoop-common.jar /lib/hadoop/hadoop-annotations.jar /lib/hadoop/lib/commons-collections-*.jar /lib/hadoop-hdfs/hadoop-hdfs-client.jar /lib/hadoop-mapreduce/hadoop-mapreduce-client-{core,common,jobclient}.jar /lib/hadoop-yarn/hadoop-yarn-{client,common,api}.jar /lib/hadoop-yarn/lib/websocket-{api,client,common}-*.jar; do
 [[ ! -f "$library" ]] || ADDITIONAL_INGEST_LIBS+=":$library"
done
export ADDITIONAL_INGEST_LIBS
export HADOOP_CLASSPATH='/usr/local/hadoop/etc/hadoop:/usr/local/hadoop/lib/*:/usr/local/hadoop/.//*:/usr/local/hadoop-hdfs/./:/usr/local/hadoop-hdfs/lib/*:/usr/local/hadoop-hdfs/.//*:/usr/local/hadoop-mapreduce/.//*:/usr/local/hadoop-yarn/./:/usr/local/hadoop-yarn/lib/*:/usr/local/hadoop-yarn/.//*'
mkdir -p /var/run/datawave /srv/logs/ingest
chmod -R u+w /opt/datawave-ingest/current/config
cp -Lr /stack-config/. /opt/datawave-ingest/current/config/
source /opt/datawave-ingest/ingest-passwd.sh
hdfs dfs -mkdir -p /datawave/floci-lab/accumulo-vfs-classpath "$BASE_WORK_DIR"
# Copy the already validated immutable baseline iterator JAR set into the lab path.
hdfs dfs -cp -f 'hdfs://hdfs-nn.default.svc.cluster.local:9000/datawave/accumulo-vfs-classpath/*.jar' /datawave/floci-lab/accumulo-vfs-classpath/
# Guard the instance before any table or instance property mutations.
grep -q '^instance.name=floci-lab$' /opt/accumulo/conf/accumulo-client.properties
if ! accumulo shell -u root -p "$PASSWORD" -e namespaces | grep -Eq '^[[:space:]]*datawave([[:space:]]|$)'; then
 accumulo shell -u root -p "$PASSWORD" -e 'createnamespace datawave'
fi
accumulo shell -u root -p "$PASSWORD" -f /dev/stdin <<'SHELL'
setauths -u root -s JBOSS_ADMIN,DW_ADMIN,AUTH_USER,DW_SERV,BAR,FOO,PRIVATE,PUBLIC
config -s general.vfs.context.classpath.datawave=hdfs://hdfs-nn.default.svc.cluster.local:9000/datawave/floci-lab/accumulo-vfs-classpath/.*.jar
config -s table.class.loader.context=datawave
SHELL
cd /opt/datawave-ingest/current/bin/ingest
./create-all-tables.sh -force
tables=$(accumulo shell -u root -p "$PASSWORD" -e tables)
grep -q 'datawave.queryMetrics_s' <<< "$tables"
grep -q 'datawave.queryMetrics_m' <<< "$tables"
./load-job-cache.sh
hdfs dfs -test -d /datawave/floci-lab/ingest/work/jobCacheB
echo 'LAB_SETUP_COMPLETE instance=floci-lab staging=HDFS userTableDefault=S3'
