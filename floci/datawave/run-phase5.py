#!/usr/bin/env python3
"""Prepare and verify DataWave clients scoped to the existing Floci lab."""
import argparse,base64,copy,io,json,re,subprocess,sys,tarfile,time
from pathlib import Path
import importlib.util,yaml
p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);p.add_argument('--baseline-job',default='floci-phase5-baseline-query-datawave-query-examples-p5base');p.add_argument('--action',choices=['prepare','baseline','setup','web','ingest-result','query-result','auth','park'],required=True);a=p.parse_args();action=a.action;baseline_job=a.baseline_job
sys.argv=[sys.argv[0],'--evidence',str(a.evidence)];sys.dont_write_bytecode=True
root=Path(__file__).resolve().parent
s=importlib.util.spec_from_file_location('lab',root.parent/'lab/run-phase3.py');lab=importlib.util.module_from_spec(s);s.loader.exec_module(lab)
E=lab.E;NS=lab.NS;HDFS=lab.HDFS;S3=lab.S3;BASE='/datawave/floci-lab';IMAGE='local/datawave/ingest-examples-s3a:8.4.0-SNAPSHOT-hadoop3.4.2'
def cm(name,data):lab.apply({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':name,'namespace':NS},'data':data})
def secret(name,data):lab.apply({'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'namespace':NS},'type':'Opaque','data':data})
def clone_secret(name):
 obj=lab.get('secret',name,'default');assert obj;secret(name,obj['data'])
def alias(name,host):lab.apply({'apiVersion':'v1','kind':'Service','metadata':{'name':name,'namespace':NS},'spec':{'type':'ExternalName','externalName':host}})
def wait_job(name,namespace=NS,timeout=1200):
 end=time.monotonic()+timeout
 while time.monotonic()<end:
  job=lab.get('job',name,namespace)
  if job and (job['status'].get('succeeded') or job['status'].get('failed')):
   lab.run(['kubectl','-n',namespace,'logs','job/'+name],name=name+'.log',check=False)
   lab.run(['kubectl','-n',namespace,'get','job',name,'-o','json'],name=name+'.json')
   return bool(job['status'].get('succeeded'))
  time.sleep(3)
 raise TimeoutError(name)
def retarget(s):
 s=s.replace('hdfs://hdfs-nn:9000','hdfs://hdfs-nn.default.svc.cluster.local:9000').replace('/datawave/ingest',BASE+'/ingest').replace('/datawave/accumulo-vfs-classpath',BASE+'/accumulo-vfs-classpath')
 s=s.replace('WAREHOUSE_INSTANCE_NAME="dev"','WAREHOUSE_INSTANCE_NAME="floci-lab"').replace('INGEST_INSTANCE_NAME="dev"','INGEST_INSTANCE_NAME="floci-lab"')
 s=s.replace('"zookeeper:2181"','"zookeeper.default.svc.cluster.local:2181"')
 return s
if action=='baseline':
 name=baseline_job;wait_job(name,'default')
 text=(E/(name+'.log')).read_text();assert 'Tests Passed: 55' in text and 'Tests Failed: 1' in text
 assert '[FAILED][FAILED][DISABLED] -> KeywordExtraction/ExtractEmptyKeywordsByPageId' in text
 (E/'fresh-baseline.json').write_text(json.dumps({'passed':55,'failed':1,'knownFailure':'KeywordExtraction/ExtractEmptyKeywordsByPageId','job':name},indent=2)+'\n')
elif action=='prepare':
 hadoop=lab.get('configmap','hadoop-config');hadoop['data']['mapred-site.xml']=(root/'mapred-site.xml').read_text();cm('hadoop-config',hadoop['data'])
 for name,host in [('hdfs-nn','hdfs-nn'),('yarn-rm','yarn-rm'),('zookeeper','zookeeper'),('mysql-cluster','mysql-cluster')]:alias(name,host+'.default.svc.cluster.local')
 for name in ['query-examples-client','certificates-secret']:clone_secret(name)
 # Transfer the original immutable config snapshot in memory; never mount its PVC for writes.
 r=subprocess.run(['kubectl','-n','default','exec','deployment/dwv-dwv-ingest-ingest','--','tar','-C','/opt/datawave-ingest/current/shared-config','-czf','-','.'],capture_output=True,check=True)
 data={}
 with tarfile.open(fileobj=io.BytesIO(r.stdout),mode='r:gz') as archive:
  for member in archive:
   if member.isfile():data[Path(member.name).name]=retarget(archive.extractfile(member).read().decode())
 assert data and 'ingest-config.xml' in data
 cm('floci-ingest-shared-config',data)
 original=lab.get('configmap','dwv-ingest-ingest-env-configmap','default')['data']
 env={k:retarget(v) for k,v in original.items()}
 env['ingest-env.sh']='export BASE_WORK_DIR='+BASE+'/ingest/work\nexport LOAD_JOBCACHE_CPU_MULTIPLIER=1\nexport JOB_CACHE_REPLICATION=2\n'+env['ingest-env.sh']
 cm('floci-ingest-env',env)
 cm('floci-ingest-genders',lab.get('configmap','dwv-ingest-genders','default')['data'])
 pw=base64.b64decode(lab.get('secret','floci-lab-root')['data']['password']).decode()
 # Shell quote credentials before writing the namespace-local Secret.
 import shlex
 script='export PASSWORD='+shlex.quote(pw)+'\nexport KEYSTORE_PASSWORD=root\nexport TRUSTSTORE_PASSWORD=root\n'
 secret('floci-ingest-credentials',{'ingest-passwd.sh':base64.b64encode(script.encode()).decode()})
 cm('floci-datawave-setup',{'setup.sh':(root/'setup.sh').read_text()})
 (E/'config-snapshot.json').write_text(json.dumps({'source':'default shared ingest PVC read-only snapshot','files':sorted(data),'bytes':sum(len(x.encode()) for x in data.values()),'targetInstance':'floci-lab','targetNamespace':NS,'stagingRoot':BASE,'credentials':'namespace-local Secrets; values omitted'},indent=2)+'\n')
elif action=='setup':
 cm('floci-datawave-setup',{'setup.sh':(root/'setup.sh').read_text()})
 spec={'restartPolicy':'Never','automountServiceAccountToken':False,'enableServiceLinks':False,'nodeSelector':{'kubernetes.io/hostname':'minikube-m03'},'securityContext':{'runAsUser':1002,'runAsGroup':1002,'fsGroup':1002},'containers':[{'name':'setup','image':IMAGE,'imagePullPolicy':'Never','command':['bash','/setup/setup.sh'],'env':[{'name':'ADDITIONAL_INGEST_LIBS','value':yaml.safe_load((root/'ingest-values.yaml').read_text())['additionalLibraries']},{'name':'HADOOP_CONF_DIR','value':'/usr/local/hadoop/conf'},{'name':'HADOOP_CREDSTORE_PASSWORD','valueFrom':{'secretKeyRef':{'name':'floci-s3a-credentials','key':'store-password'}}}],'resources':{'requests':{'cpu':'100m','memory':'256Mi'},'limits':{'memory':'1536Mi'}},'volumeMounts':[{'name':'genders','mountPath':'/etc/genders','subPath':'genders','readOnly':True},{'name':'setup','mountPath':'/setup','readOnly':True},{'name':'config','mountPath':'/stack-config','readOnly':True},{'name':'env','mountPath':'/opt/datawave-ingest/current/bin/ingest/ingest-env.sh','subPath':'ingest-env.sh','readOnly':True},{'name':'password','mountPath':'/opt/datawave-ingest/ingest-passwd.sh','subPath':'ingest-passwd.sh','readOnly':True},{'name':'hadoop','mountPath':'/usr/local/hadoop/conf','readOnly':True},{'name':'accumulo','mountPath':'/opt/accumulo/conf','readOnly':True},{'name':'credentials','mountPath':'/etc/hadoop/credentials','readOnly':True}]}],'volumes':[{'name':'genders','configMap':{'name':'floci-ingest-genders'}},{'name':'setup','configMap':{'name':'floci-datawave-setup'}},{'name':'config','configMap':{'name':'floci-ingest-shared-config'}},{'name':'env','configMap':{'name':'floci-ingest-env'}},{'name':'password','secret':{'secretName':'floci-ingest-credentials'}},{'name':'hadoop','configMap':{'name':'hadoop-config'}},{'name':'accumulo','configMap':{'name':'accumulo-config'}},{'name':'credentials','secret':{'secretName':'floci-s3a-credentials','items':[{'key':'floci.jceks','path':'floci.jceks'}]}}]}
 assert lab.get('job','floci-datawave-setup') is None,'Inspect an existing setup Job before retrying'
 lab.apply({'apiVersion':'batch/v1','kind':'Job','metadata':{'name':'floci-datawave-setup','namespace':NS},'spec':{'backoffLimit':0,'activeDeadlineSeconds':1200,'template':{'spec':spec}}})
 assert wait_job('floci-datawave-setup'),'Lab setup failed; inspect captured logs'
elif action in ['ingest-result','query-result']:
 release='floci-phase5-ingest' if action=='ingest-result' else 'floci-phase5-query'
 jobs=json.loads(lab.kube('get','jobs','-l','app.kubernetes.io/instance='+release,'-o','json').stdout)['items']
 assert jobs,'Missing test Job'
 name=max(jobs,key=lambda j:j['metadata']['creationTimestamp'])['metadata']['name']
 success=wait_job(name)
 text=(E/(name+'.log')).read_text()
 if action=='ingest-result':
  datasets=re.findall(r'Completed example: (\S+)',text)
  assert success and set(datasets)==set(yaml.safe_load((root/'ingest-values.yaml').read_text())['examples']['datasets'])
  (E/'ingest-result.json').write_text(json.dumps({'status':'pass','job':name,'completedDatasets':datasets,'matchedCacheJars':re.search(r'Existing cache matches all (\d+) required JARs',text).group(1)},indent=2)+'\n')
 else:
  passed=int(re.search(r'Tests Passed: (\d+)',text).group(1));failed=int(re.search(r'Tests Failed: (\d+)',text).group(1))
  assert passed==55 and failed==1 and '[FAILED][FAILED][DISABLED] -> KeywordExtraction/ExtractEmptyKeywordsByPageId' in text
  assert 'Cache refresh evidence:' in text
  from datetime import datetime
  timestamp=lambda value:datetime.fromisoformat(value.replace('Z','+00:00'))
  server=lab.get('pod','accumulo-tservers-0');start=timestamp(lab.get('job',name)['status']['startTime'])
  assert timestamp(server['metadata']['creationTimestamp'])<=start,'Tablet server Pod was replaced during query validation'
  terminated=server['status']['containerStatuses'][0].get('lastState',{}).get('terminated')
  assert not terminated or timestamp(terminated['finishedAt'])<=start,'Tablet server restarted during query validation'
  (E/'query-result.json').write_text(json.dumps({'matchesFreshBaseline':True,'passed':passed,'failed':failed,'knownFailure':'KeywordExtraction/ExtractEmptyKeywordsByPageId','job':name},indent=2)+'\n')
elif action=='auth':
 for name in ['cache','configuration','rabbitmq']:alias(name,name+'.default.svc.cluster.local')
 clone_secret('certificates-secret')
 original=lab.get('deployment','authorization','default')
 pod=copy.deepcopy(original['spec']['template']['spec'])
 pod.pop('imagePullSecrets',None);pod['nodeSelector']={'kubernetes.io/hostname':'minikube-m02'}
 container=pod['containers'][0];container['imagePullPolicy']='Never'
 container['resources']={'requests':{'cpu':'100m','memory':'256Mi'},'limits':{'memory':'1Gi'}}
 for env in container['env']:
  if env['name']=='CONFIG_SERVER_URL':env['value']='http://configuration.default.svc.cluster.local:8888/configserver'
 container['env'].append({'name':'JAVA_TOOL_OPTIONS','value':'-Xms64m -Xmx512m'})
 lab.apply({'apiVersion':'apps/v1','kind':'Deployment','metadata':{'name':'authorization','namespace':NS},'spec':{'replicas':1,'selector':{'matchLabels':{'application':'authorization'}},'template':{'metadata':{'labels':{'application':'authorization'}},'spec':pod}}})
 current=lab.get('service','authorization')
 if current and current['spec']['type']=='ExternalName':lab.kube('delete','service','authorization')
 lab.apply({'apiVersion':'v1','kind':'Service','metadata':{'name':'authorization','namespace':NS},'spec':{'selector':{'application':'authorization'},'ports':[{'name':'http','port':8080},{'name':'https','port':8443}]}})
 lab.kube('rollout','status','deployment/authorization','--timeout=600s',name='lab-authorization-ready.txt',timeout=620)
elif action=='park':
 for name in ['floci-phase5-files','floci-phase5-direct-ingest','floci-phase5-direct-web']:
  if lab.get('pod',name):lab.kube('delete','pod',name,'--grace-period=5','--wait=true','--timeout=30s')
 for name in ['datawave-monolith','authorization']:
  if lab.get('deployment',name):lab.kube('scale','deployment/'+name,'--replicas=0')
elif action=='web':
 # Give cached results their own database/user; root credentials travel only through stdin.
 cached=lab.get('secret','floci-cached-results')
 if cached is None:
  import secrets
  password=secrets.token_urlsafe(24)
  cluster=lab.get('innodbcluster','mysql-cluster','default')
  administrative=lab.get('secret','mysql-cluster-privsecrets','default')['data']
  adminuser=base64.b64decode(administrative['clusterAdminUsername']).decode()
  rootpassword=base64.b64decode(administrative['clusterAdminPassword']).decode()
  sql="CREATE DATABASE IF NOT EXISTS floci_cached_results; CREATE USER IF NOT EXISTS 'floci_lab'@'%' IDENTIFIED BY '"+password+"'; GRANT ALL PRIVILEGES ON floci_cached_results.* TO 'floci_lab'@'%';\n"
  lab.run(['kubectl','-n','default','exec','-i','mysql-cluster-0','-c','mysql','--','bash','-c','IFS= read -r MYSQL_USER; IFS= read -r MYSQL_PWD; export MYSQL_PWD; exec mysql -h mysql-cluster -u"$MYSQL_USER"'],stdin=adminuser+'\n'+rootpassword+'\n'+sql,name='lab-cached-results-database.txt')
  secret('floci-cached-results',{k:base64.b64encode(v.encode()).decode() for k,v in {'username':'floci_lab','password':password}.items()})
 original=lab.get('deployment','datawave-monolith','default');pod=copy.deepcopy(original['spec']['template']['spec'])
 container=pod['containers'][0];container['image']='local/datawave/webservice-s3a:8.4.0-SNAPSHOT-hadoop3.4.1';container['imagePullPolicy']='Never'
 container['resources']={'requests':{'cpu':'100m','memory':'512Mi'},'limits':{'memory':'2Gi'}}
 for env in container['env']:
  if env['name']=='ACCUMULO_PASSWORD':env.pop('value',None);env['valueFrom']={'secretKeyRef':{'name':'floci-lab-root','key':'password'}}
  if env['name'].startswith('CACHED_RESULTS_'):env.pop('value',None);env['valueFrom']={'secretKeyRef':{'name':'floci-cached-results','key':'username' if env['name'].endswith('USERNAME') else 'password'}}
 container['env'] += [{'name':'HADOOP_CREDSTORE_PASSWORD','valueFrom':{'secretKeyRef':{'name':'floci-s3a-credentials','key':'store-password'}}},{'name':'JAVA_OPTS','value':'-Xms128m -Xmx1024m -XX:MaxMetaspaceSize=384m -Djava.net.preferIPv4Stack=true -Dcom.google.protobuf.use_unsafe_pre22_gencode=true'}]
 pod['nodeSelector']={'kubernetes.io/hostname':'minikube'};pod.pop('imagePullSecrets',None)
 pod['initContainers']=[i for i in pod.get('initContainers',[]) if i['name']!='init-ingest']
 for volume in pod['volumes']:
  if volume['name'].endswith('web-runtime-config'):volume['configMap']['name']='floci-web-runtime-config'
 pod['volumes'].append({'name':'floci-credentials','secret':{'secretName':'floci-s3a-credentials','items':[{'key':'floci.jceks','path':'floci.jceks'}]}})
 container['volumeMounts'].append({'name':'floci-credentials','mountPath':'/etc/hadoop/credentials','readOnly':True})
 runtime_name=next(v['configMap']['name'] for v in original['spec']['template']['spec']['volumes'] if v['name'].endswith('web-runtime-config'))
 runtime=lab.get('configmap',runtime_name,'default')['data']['runtime-config.cli']
 additions=[]
 for group in ['warehouse','metrics','uuid']:
  additions += ['/system-property=dw.'+group+'.instanceName:write-attribute(name=value,value=floci-lab)','/system-property=dw.'+group+'.zookeepers:write-attribute(name=value,value="zookeeper.default.svc.cluster.local:2181")']
 runtime=runtime.replace('run-batch','\n'.join(additions)+'\nrun-batch')
 runtime=runtime.replace('jdbc:mysql://mysql-cluster:3306/cachedResults','jdbc:mysql://mysql-cluster.default.svc.cluster.local:3306/floci_cached_results')
 # Match the actual configured router and database without assuming their original names.
 runtime=re.sub(r'jdbc:mysql://[^/\s"]+/[^?"\s]+', 'jdbc:mysql://mysql-cluster.default.svc.cluster.local:3306/floci_cached_results',runtime)
 cm('floci-web-runtime-config',{'runtime-config.cli':runtime})
 deployment={'apiVersion':'apps/v1','kind':'Deployment','metadata':{'name':'datawave-monolith','namespace':NS},'spec':{'replicas':1,'selector':{'matchLabels':{'application':'datawave-monolith'}},'strategy':{'type':'Recreate'},'template':{'metadata':{'labels':{'application':'datawave-monolith'}},'spec':pod}}}
 lab.apply(deployment)
 for name in ['web-datawave','datawave-http']:
  service=lab.get('service',name,'default')
  if service:
   service={'apiVersion':'v1','kind':'Service','metadata':{'name':name,'namespace':NS},'spec':{k:v for k,v in service['spec'].items() if k in ['ports','selector','type']}}
   for port in service['spec']['ports']:port.pop('nodePort',None)
   service['spec']['type']='ClusterIP';lab.apply(service)
 lab.kube('rollout','status','deployment/datawave-monolith','--timeout=600s',name='web-rollout-ready.txt',timeout=620)
 lab.kube('get','deployment','datawave-monolith','-o','json',name='lab-web-deployment-redacted.json')
else:raise NotImplementedError(action)
print('Phase 5 '+action+' complete.',flush=True)
