#!/usr/bin/env python3
"""Read a real DataWave S3 RFile with the ingest libraries and web EAR+Hadoop module."""
import argparse,copy,importlib.util,json,sys
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);p.add_argument('--role',choices=['all','ingest','web'],default='all');a=p.parse_args();role_choice=a.role;sys.argv=[sys.argv[0],'--evidence',str(a.evidence)];sys.dont_write_bytecode=True
ROOT=Path(__file__).resolve().parent;s=importlib.util.spec_from_file_location('lab',ROOT.parent/'lab/run-phase3.py');lab=importlib.util.module_from_spec(s);s.loader.exec_module(lab)
file=max((x for x in json.loads((lab.E/'datawave-files.json').read_text())['files'] if x['table']=='datawave.metadata'),key=lambda x:x['bytes'])
(lab.E/'direct-rfile-selection.json').write_text(json.dumps(file,indent=2)+'\n')
for role in (['ingest','web'] if role_choice=='all' else [role_choice]):
 name='floci-phase5-direct-'+role
 if lab.get('pod',name) is None:
  if role=='ingest':template=lab.get('job','floci-datawave-setup')['spec']['template']['spec']
  else:template=lab.get('deployment','datawave-monolith')['spec']['template']['spec']
  pod=copy.deepcopy(template);pod.pop('initContainers',None);pod.pop('imagePullSecrets',None);container=pod['containers'][0];container['name']='probe';container['command']=['sleep','infinity'];container.pop('args',None)
  for field in ['livenessProbe','readinessProbe','startupProbe','ports','lifecycle']:container.pop(field,None)
  container['resources']={'requests':{'cpu':'50m','memory':'64Mi'},'limits':{'memory':'768Mi'}}
  container['env']=[x for x in container.get('env',[]) if x['name']!='HADOOP_CONF_DIR']+[{'name':'HADOOP_CONF_DIR','value':'/usr/local/hadoop/conf' if role=='ingest' else '/etc/hadoop/conf'}]
  pod['restartPolicy']='Never';pod['automountServiceAccountToken']=False;pod['enableServiceLinks']=False
  pod['volumes'].append({'name':'direct-probe','configMap':{'name':'floci-datawave-probes'}});container['volumeMounts'].append({'name':'direct-probe','mountPath':'/direct-probe','readOnly':True})
  lab.apply({'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'namespace':lab.NS},'spec':pod})
 lab.kube('wait','--for=condition=Ready','pod/'+name,'--timeout=90s')
 if role=='ingest':
  code=r'''set -eo pipefail
mkdir -p /var/run/datawave /srv/logs/ingest
cd /opt/datawave-ingest/current/bin/ingest
for library in /lib/hadoop/hadoop-common.jar /lib/hadoop/hadoop-annotations.jar /lib/hadoop/lib/commons-collections-*.jar /lib/hadoop-hdfs/hadoop-hdfs-client.jar /lib/hadoop-mapreduce/hadoop-mapreduce-client-{core,common,jobclient}.jar /lib/hadoop-yarn/hadoop-yarn-{client,common,api}.jar /lib/hadoop-yarn/lib/websocket-{api,client,common}-*.jar; do
 [[ ! -f "$library" ]] || ADDITIONAL_INGEST_LIBS+=":$library"
done
export ADDITIONAL_INGEST_LIBS
if ! source ./ingest-libs.sh; then echo "Could not resolve ingest client classpath" >&2; exit 1; fi
export HADOOP_CLASSPATH="$CLASSPATH"
export HADOOP_HEAPSIZE_MAX=256
exec hadoop jar /direct-probe/datawave-probes.jar local.floci.RFileReadProbe "$1" "$2"
'''
 else:
  code=r'''set -eo pipefail
module=/tmp/reader-modules/local/floci/reader/main
mkdir -p "$module"
unzip -oq /opt/jboss/wildfly/standalone/deployments/*.ear 'lib/*.jar' -d "$module"
cp /direct-probe/datawave-probes.jar "$module/"
{
 echo '<module xmlns="urn:jboss:module:1.3" name="local.floci.reader"><main-class name="local.floci.RFileReadProbe"/><resources><resource-root path="datawave-probes.jar"/>'
 for jar in "$module"/lib/*.jar; do
  case "${jar##*/}" in *hadoop-*.jar) continue;; esac
  echo "<resource-root path=\"lib/${jar##*/}\"/>"
 done
 echo '</resources><dependencies><module name="org.apache.hadoop.common"/><module name="org.apache.commons.lang3"/><module name="com.google.guava"/><module name="org.slf4j"/><module name="javax.api"/><module name="com.fasterxml.jackson.core.jackson-databind"/></dependencies></module>'
} > "$module/module.xml"
exec java -Xmx256m -jar /opt/jboss/wildfly/jboss-modules.jar -mp /opt/jboss/wildfly/modules:/tmp/reader-modules local.floci.reader "$1" "$2"
'''
 result=lab.kube('exec',name,'--','bash','-c',code,'probe',file['uri'],file['sha256'],name='direct-rfile-'+role+'.log',timeout=240)
 assert 'DIRECT_RFILE_READ=PASS' in result.stdout and file['sha256'] in result.stdout
 lab.kube('exec',name,'--','cat','/sys/fs/cgroup/memory.peak',name='direct-rfile-'+role+'-peak.txt')
 lab.kube('delete','pod',name,'--grace-period=5','--wait=true','--timeout=30s')
print('Direct S3 RFile reads and 25 random seeks passed: '+role_choice)
