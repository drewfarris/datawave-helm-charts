#!/usr/bin/env python3
"""Initialize and verify a disposable hybrid lab, then restore the original compute."""
import argparse,base64,json,os,re,subprocess,tempfile,time
from pathlib import Path
from datetime import datetime,timezone
import yaml
p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);p.add_argument('--overlay',type=Path,action='append',default=[]);p.add_argument('--action',choices=['run','activate','park'],default='run');args=p.parse_args()
ROOT=Path(__file__).resolve().parent;CHARTS=ROOT.parents[1];E=args.evidence.resolve();E.mkdir(parents=True,exist_ok=True)
NS='datawave-s3-lab';RELEASE='floci-lab';HDFS='hdfs://hdfs-nn.default.svc.cluster.local:9000/accumulo-s3-lab';S3='s3a://dwv-accumulo-lab/accumulo';ZK='zookeeper.default.svc.cluster.local:2181';IMAGE='local/datawave/accumulo-s3a:2.1.4-hadoop3.4.1'
COMMANDS=json.loads((E/'commands.json').read_text()) if (E/'commands.json').exists() else []

def redact(value):
 if isinstance(value,dict):
  if value.get('kind')=='Secret':return {'kind':'Secret','metadata':{'name':value['metadata']['name']},'data':'[REDACTED]'}
  sensitive=bool(re.search('password|token|secret',str(value.get('name','')),re.I))
  return {k:('[REDACTED]' if re.search('password|token|(^|[.])secret$',k,re.I) or (k=='value' and sensitive) else redact(v)) for k,v in value.items()}
 if isinstance(value,list):return [redact(x) for x in value]
 if isinstance(value,str):return re.sub(r'(?im)^([^\n]*(?:auth[.]token|instance[.]secret|trace[.]token[.]property[.]password)\s*=).*$',r'\1[REDACTED]',value)
 return value

def run(cmd,name=None,stdin=None,check=True,timeout=300):
 r=subprocess.run(cmd,input=stdin,text=True,capture_output=True,timeout=timeout)
 # Preserve commands added by a nested phase runner while this process waited.
 if (E/'commands.json').exists():COMMANDS[:]=json.loads((E/'commands.json').read_text())
 COMMANDS.append({'time':datetime.now(timezone.utc).isoformat(),'command':cmd,'exitCode':r.returncode,'file':name})
 (E/'commands.json').write_text(json.dumps(COMMANDS,indent=2)+'\n')
 if name:
  output=redact(r.stdout)
  if name.endswith('.json'):
   try:output=json.dumps(redact(json.loads(r.stdout)),indent=2)+'\n'
   except ValueError:pass
  (E/name).write_text(output+('\nSTDERR:\n'+redact(r.stderr) if r.stderr else ''))
 if check and r.returncode:raise RuntimeError('Failed '+' '.join(cmd)+'\n'+r.stdout+r.stderr)
 return r

def kube(*cmd,**kw):return run(['kubectl','-n',NS,*cmd],**kw)
def apply(obj):return run(['kubectl','apply','-f','-'],stdin=json.dumps(obj))
def get(kind,name,namespace=NS):
 r=run(['kubectl','-n',namespace,'get',kind,name,'-o','json'],check=False)
 return json.loads(r.stdout) if r.returncode==0 else None

def pods(namespace):return json.loads(run(['kubectl','-n',namespace,'get','pods','-o','json']).stdout)['items']
def wait_original(gone):
 targets=json.loads((E/'original-replicas.json').read_text())
 deadline=time.monotonic()+300
 while time.monotonic()<deadline:
  selected=[x for x in pods('default') if any(x['metadata']['name'].startswith(t['name']+'-') for t in targets)]
  if gone and not selected:return
  if not gone:
   desired=sum(t['replicas'] for t in targets)
   ready=[x for x in selected if not x['metadata'].get('deletionTimestamp') and any(c['type']=='Ready' and c['status']=='True' for c in x['status'].get('conditions',[]))]
   if len(ready)==desired:return
  time.sleep(3)
 raise TimeoutError('Original compute '+('shutdown' if gone else 'recovery'))

def memory(name):
 text=run(['docker','stats','--no-stream','--format','{{.Name}} {{.MemUsage}}','minikube','minikube-m02','minikube-m03'],name=name).stdout
 row=next(x for x in text.splitlines() if x.startswith('minikube-m03 '));used,limit=re.findall(r'([0-9.]+)([GM]iB)',row)
 def gib(x):return float(x[0])*(1 if x[1]=='GiB' else 1/1024)
 return gib(limit)-gib(used)

def pause_original():
 if not (E/'original-replicas.json').exists():
  d=json.loads(run(['kubectl','-n','default','get','deployment,statefulset','-o','json']).stdout)
  selected=[{'kind':x['kind'].lower(),'name':x['metadata']['name'],'replicas':x['spec'].get('replicas',1)} for x in d['items'] if x['metadata']['name'].startswith('accumulo-') or x['metadata']['name'] in ['datawave-monolith','dwv-dwv-ingest-ingest']]
  (E/'original-replicas.json').write_text(json.dumps(selected,indent=2)+'\n')
 targets=json.loads((E/'original-replicas.json').read_text())
 run(['kubectl','-n','default','scale','deployment/datawave-monolith','deployment/dwv-dwv-ingest-ingest','--replicas=0'])
 rest=[x for x in targets if x['name'] not in ['datawave-monolith','dwv-dwv-ingest-ingest']]
 run(['kubectl','-n','default','scale',*[x['kind']+'/'+x['name'] for x in rest],'--replicas=0'])
 wait_original(True)
 # A running core lab already consumes its startup reservation. Reconfiguration
 # needs headroom for a bounded rolling replacement, rather than reserving it twice.
 active=any(get(kind,name)['spec'].get('replicas',0)>0 for kind,name in [('deployment','accumulo-manager'),('statefulset','accumulo-tservers'),('deployment','accumulo-gc')])
 required=1 if active else 3
 assert memory('memory-after-pause.txt')>=required,'Less than '+str(required)+'Gi real free node memory; do not start/reconfigure the lab'
 print('Original compute paused; HDFS/ZooKeeper preserved; lab capacity gate passed.',flush=True)

def prepare():
 if get('namespace',NS) is None:run(['kubectl','create','namespace',NS])
 for name in ['floci-s3a-credentials']:
  secret=get('secret',name,'aws-emulation');assert secret
  apply({'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'namespace':NS},'type':'Opaque','data':secret['data']})
 config=get('configmap','floci-s3a-hadoop-config','aws-emulation')
 apply({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'hadoop-config','namespace':NS},'data':config['data']})
 status=run(['helm','status',RELEASE,'-n',NS],check=False)
 verb='upgrade' if status.returncode==0 else 'install'
 run(['helm',verb,RELEASE,str(CHARTS/'accumulo'),'-n',NS,'-f',str(ROOT/'values.yaml'),'--wait','--timeout','3m'],name='helm-initial.txt')
 cm=get('configmap','accumulo-config');assert cm
 client=cm['data']['accumulo-client.properties'];password=re.search(r'^auth.token=(.*)$',client,re.M).group(1)
 apply({'apiVersion':'v1','kind':'Secret','metadata':{'name':'floci-lab-root','namespace':NS},'stringData':{'password':password}})
 with tempfile.TemporaryDirectory(prefix='floci-lab-java-') as td:
  td=Path(td);classes=td/'classes';classes.mkdir();m2=Path.home()/'.m2/repository'
  jars=[m2/'org/apache/hadoop/hadoop-common/3.4.1/hadoop-common-3.4.1.jar',m2/'org/apache/zookeeper/zookeeper/3.8.1/zookeeper-3.8.1.jar',m2/'org/apache/zookeeper/zookeeper-jute/3.8.1/zookeeper-jute-3.8.1.jar']
  run(['javac','--release','11','-cp',':'.join(map(str,jars)),'-d',str(classes),str(ROOT.parent/'s3a/java/LabState.java')],name='javac-lab.log')
  jar=td/'lab-probe.jar';run(['jar','--create','--file',str(jar),'-C',str(classes),'.'])
  apply({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'floci-lab-bootstrap','namespace':NS},'data':{'initialize.sh':(ROOT/'initialize.sh').read_text()},'binaryData':{'lab-probe.jar':base64.b64encode(jar.read_bytes()).decode()}})
 print('Isolated namespace/configuration prepared; all lab server replicas remain zero.',flush=True)

def bootstrap():
 env=[{'name':'HADOOP_CLASSPATH','value':yaml.safe_load((CHARTS/'accumulo/values.yaml').read_text())['hadoop']['classpath']},{'name':'ACCUMULO_INSTANCE_NAME','value':'floci-lab'},{'name':'ACCUMULO_CONF_DIR','value':'/work/accumulo'},{'name':'HADOOP_CONF_DIR','value':'/usr/local/hadoop/conf'},{'name':'LAB_HDFS_VOLUME','value':HDFS},{'name':'LAB_S3_VOLUME','value':S3},{'name':'LAB_ZOOKEEPERS','value':ZK},{'name':'CLASSPATH','value':'/probe/lab-probe.jar'},{'name':'ACCUMULO_JAVA_OPTS','value':'-Xmx256m -Xms64m'},{'name':'ACCUMULO_ROOT_PASSWORD','valueFrom':{'secretKeyRef':{'name':'floci-lab-root','key':'password'}}},{'name':'HADOOP_CREDSTORE_PASSWORD','valueFrom':{'secretKeyRef':{'name':'floci-s3a-credentials','key':'store-password'}}}]
 spec={'restartPolicy':'Never','automountServiceAccountToken':False,'enableServiceLinks':False,'nodeSelector':{'kubernetes.io/hostname':'minikube-m03'},'securityContext':{'runAsUser':1001,'fsGroup':1001},'containers':[{'name':'bootstrap','image':IMAGE,'imagePullPolicy':'Never','command':['/bin/bash','/probe/initialize.sh'],'env':env,'resources':{'requests':{'cpu':'100m','memory':'128Mi'},'limits':{'cpu':'2','memory':'768Mi'}},'volumeMounts':[{'name':'probe','mountPath':'/probe','readOnly':True},{'name':'accumulo','mountPath':'/source-accumulo','readOnly':True},{'name':'hadoop','mountPath':'/usr/local/hadoop/conf','readOnly':True},{'name':'credentials','mountPath':'/etc/hadoop/credentials','readOnly':True},{'name':'work','mountPath':'/work'}]}],'volumes':[{'name':'probe','configMap':{'name':'floci-lab-bootstrap'}},{'name':'accumulo','configMap':{'name':'accumulo-config'}},{'name':'hadoop','configMap':{'name':'hadoop-config'}},{'name':'credentials','secret':{'secretName':'floci-s3a-credentials','items':[{'key':'floci.jceks','path':'floci.jceks'}]}},{'name':'work','emptyDir':{}}]}
 job={'apiVersion':'batch/v1','kind':'Job','metadata':{'name':'floci-lab-storage-bootstrap','namespace':NS},'spec':{'parallelism':1,'completions':1,'backoffLimit':0,'activeDeadlineSeconds':600,'template':{'spec':spec}}}
 if get('job','floci-lab-storage-bootstrap') is None:apply(job)
 deadline=time.monotonic()+600
 while time.monotonic()<deadline:
  j=get('job','floci-lab-storage-bootstrap')
  if j['status'].get('failed'):
   kube('logs','job/floci-lab-storage-bootstrap',name='bootstrap-failed.log',check=False)
   raise RuntimeError('Storage bootstrap failed; inspect state before retrying the fixed-name Job')
  if j['status'].get('succeeded'):
   kube('logs','job/floci-lab-storage-bootstrap',name='bootstrap.log');kube('get','job','floci-lab-storage-bootstrap','-o','json',name='bootstrap-job.json');return
  time.sleep(3)
 raise TimeoutError('Storage bootstrap')

def switch_lab(active):
 count='1' if active else '0'
 cmd=['helm','upgrade',RELEASE,str(CHARTS/'accumulo'),'-n',NS,'-f',str(ROOT/'values.yaml'),*[item for path in args.overlay for item in ['-f',str(path.resolve())]],'--set','storage.volumes[0]='+HDFS,'--set','storage.volumes[1]='+S3,'--set','manager.replicaCount='+count,'--set','tabletServers.replicaCount='+count,'--set','gc.replicaCount='+count,'--wait','--timeout','4m']
 run(cmd,name='helm-'+('active' if active else 'parked')+'.txt')
 if not active:
  deadline=time.monotonic()+120
  while time.monotonic()<deadline:
   if not any(x['status'].get('phase')=='Running' for x in pods(NS)):return
   time.sleep(2)
  raise TimeoutError('Lab compute shutdown')

def client(commands,name):
 code='PASSWORD=$(sed -n "s/^auth.token=//p" /opt/accumulo/conf/accumulo-client.properties); exec accumulo shell -u root -p "$PASSWORD" -f /dev/stdin'
 batch='\n'.join(commands.split('; '))+'\n'
 (E/(name+'.commands')).write_text(batch)
 return kube('exec','-i','deployment/accumulo-manager','--','/bin/bash','-c',code,name=name,stdin=batch)

def verify_lab():
 kube('exec','deployment/accumulo-manager','--','/bin/bash','-c','timeout 120 bash -c "until echo >/dev/tcp/accumulo-manager/9999; do sleep 2; done"',name='manager-rpc-ready.txt')
 props=kube('exec','deployment/accumulo-manager','--','cat','/opt/accumulo/conf/accumulo.properties',name='accumulo-effective-redacted.properties').stdout
 pairs=[line.split('=',1) for line in props.splitlines() if '=' in line and not line.strip().startswith('#')]
 keys=[k.strip() for k,v in pairs];assert len(keys)==len(set(keys)),'Duplicate property keys'
 values={k.strip():v.strip() for k,v in pairs};assert values['instance.volumes']==HDFS+','+S3
 assert values['general.volume.chooser']=='org.apache.accumulo.core.spi.fs.PreferredVolumeChooser'
 for scope in ['default','init','logger']:assert values['general.custom.volume.preferred.'+scope]==HDFS
 assert 'STATE=REGISTERED' in kube('exec','deployment/accumulo-manager','--','accumulo','local.floci.LabState','floci-lab',ZK,HDFS,S3,name='registered-state.txt').stdout
 client('config -t accumulo.root -s table.custom.volume.preferred='+HDFS+'; config -t accumulo.metadata -s table.custom.volume.preferred='+HDFS,'pin-system-tables.txt')
 tables=client('tables -l','tables-before-canary.txt').stdout
 if 'floci_canary' not in tables:client('createtable floci_canary','create-canary.txt')
 client('config -t floci_canary -s table.custom.volume.preferred='+S3,'pin-canary.txt')
 time.sleep(3)
 client('insert -t floci_canary phase3 cf cq floci-ready; flush -t floci_canary -w; flush -t accumulo.metadata -w; flush -t accumulo.root -w','canary-flush.txt')
 scan=client('scan -t floci_canary -np','canary-scan.txt').stdout;assert 'floci-ready' in scan
 policy=client('config -t accumulo.root -f volume -np; config -t accumulo.metadata -f volume -np; config -t floci_canary -f volume -np','effective-placement.txt').stdout
 assert 'table.custom.volume.preferred' in policy and S3 in policy and HDFS in policy
 metadata=client('scan -t accumulo.metadata -c file -np','metadata-file-references.txt').stdout
 assert S3+'/tables/' in metadata,'Canary has no S3A metadata file URI'
 hfiles=kube('exec','deployment/accumulo-manager','--','hdfs','dfs','-ls','-R',HDFS+'/tables',name='hdfs-system-files.txt').stdout
 for system in ['+r','!0']:assert any('/tables/'+system+'/' in line and '.rf' in line for line in hfiles.splitlines()),'System files not visible in HDFS: '+system
 wals=kube('exec','deployment/accumulo-manager','--','hdfs','dfs','-ls','-R',HDFS+'/wal',name='hdfs-wal-files.txt').stdout
 assert any('/wal/' in line and line.startswith('-') for line in wals.splitlines()),'No HDFS WAL file'
 original=kube('exec','deployment/accumulo-manager','--','hdfs','dfs','-ls','hdfs://hdfs-nn.default.svc.cluster.local:9000/accumulo/instance_id',name='original-instance-id.txt').stdout
 assert 'f255c67d-93c2-4cfc-8fe0-c2b686024598' in original,'Original dev instance identity changed'
 # Manager restart must preserve registration and data; it must not execute automatic init.
 kube('rollout','restart','deployment/accumulo-manager')
 kube('rollout','status','deployment/accumulo-manager','--timeout=180s')
 assert 'STATE=REGISTERED' in kube('exec','deployment/accumulo-manager','--','accumulo','local.floci.LabState','floci-lab',ZK,HDFS,S3,name='registered-state-after-restart.txt').stdout
 assert 'floci-ready' in client('scan -t floci_canary -np','canary-after-restart.txt').stdout
 inventory_s3()
 kube('get','pods','-o','json',name='active-lab-pods.json')
 print('Hybrid placement, HDFS system storage and manager restart verified.',flush=True)

def inventory_s3():
 name='floci-lab-object-inventory'
 code="""import boto3,os,json
from botocore.config import Config
s=boto3.client('s3',endpoint_url=os.environ['AWS_ENDPOINT_URL'],region_name='us-east-1',config=Config(s3={'addressing_style':'path'}))
keys=[{'key':o['Key'],'bytes':o['Size'],'etag':o['ETag']} for page in s.get_paginator('list_objects_v2').paginate(Bucket='dwv-accumulo-lab',Prefix='accumulo/') for o in page.get('Contents',[])]
assert any('/tables/' in o['key'] and o['key'].endswith('.rf') for o in keys)
assert not any(o['key'].startswith(p) for o in keys for p in ['accumulo/wal/','accumulo/tables/+r/','accumulo/tables/!0/'])
print(json.dumps({'bucket':'dwv-accumulo-lab','objects':keys,'systemFilesAndWalsAbsent':True},indent=2))
"""
 job={'apiVersion':'batch/v1','kind':'Job','metadata':{'name':name,'namespace':'aws-emulation'},'spec':{'backoffLimit':0,'activeDeadlineSeconds':90,'template':{'spec':{'restartPolicy':'Never','terminationGracePeriodSeconds':5,'automountServiceAccountToken':False,'enableServiceLinks':False,'nodeSelector':{'kubernetes.io/hostname':'minikube-m03'},'securityContext':{'runAsUser':1001,'runAsNonRoot':True},'containers':[{'name':'inspect','image':'local/datawave/floci-probe:phase1-20261018','imagePullPolicy':'Never','args':['-c',code],'envFrom':[{'secretRef':{'name':'floci-aws-credentials'}}],'env':[{'name':'AWS_ENDPOINT_URL','value':'http://floci.aws-emulation.svc.cluster.local:4566'},{'name':'AWS_EC2_METADATA_DISABLED','value':'true'}],'resources':{'requests':{'cpu':'50m','memory':'64Mi'},'limits':{'memory':'192Mi'}}}]}}}}
 existing=get('job',name,'aws-emulation')
 if existing and (existing['status'].get('succeeded') or existing['status'].get('failed')):
  run(['kubectl','-n','aws-emulation','delete','job',name,'--wait=true','--timeout=30s'])
  existing=None
 if existing is None:apply(job)
 run(['kubectl','-n','aws-emulation','wait','--for=condition=complete','job/'+name,'--timeout=90s'])
 run(['kubectl','-n','aws-emulation','logs','job/'+name],name='s3-object-inventory.json')

def restore_original():
 targets=json.loads((E/'original-replicas.json').read_text())
 for x in targets:
  if x['name'] not in ['datawave-monolith','dwv-dwv-ingest-ingest']:run(['kubectl','-n','default','scale',x['kind']+'/'+x['name'],'--replicas='+str(x['replicas'])])
 time.sleep(10)
 for name in ['dwv-dwv-ingest-ingest','datawave-monolith']:
  x=next(x for x in targets if x['name']==name);run(['kubectl','-n','default','scale',x['kind']+'/'+name,'--replicas='+str(x['replicas'])])
 wait_original(False)
 memory('memory-after-restore.txt');run(['kubectl','get','pods','-A','-o','wide'],name='final-pods.txt');run(['helm','list','-A'],name='final-helm-releases.txt')
 print('Original compute restored; verified lab is parked to preserve memory headroom.',flush=True)

def save_redacted():
 for suffix,cmd in [('values',['helm','get','values',RELEASE,'-n',NS,'-a']),('manifest',['helm','get','manifest',RELEASE,'-n',NS])]:
  raw=run(cmd).stdout;docs=[redact(x) for x in yaml.safe_load_all(raw)];(E/('lab-'+suffix+'-redacted.yaml')).write_text(yaml.safe_dump_all(docs,sort_keys=False))

def finish_report():
 for kind,name in [('deployment','accumulo-manager'),('statefulset','accumulo-tservers'),('deployment','accumulo-gc')]:
  assert get(kind,name)['spec']['replicas']==0,'Lab must be parked after verification'
 wait_original(False)
 inventory=json.loads((E/'s3-object-inventory.json').read_text())
 metadata=(E/'metadata-file-references.txt').read_text()
 rfiles=[x for x in inventory['objects'] if x['key'].endswith('.rf')]
 assert rfiles and all('s3a://dwv-accumulo-lab/'+x['key'] in metadata for x in rfiles)
 state=(E/'registered-state-after-restart.txt').read_text();instance=re.search(r'^INSTANCE_ID=(.*)$',state,re.M).group(1)
 (E/'phase3-result.json').write_text(json.dumps({'status':'pass','instanceName':'floci-lab','instanceID':instance,'namespace':NS,'volumes':[HDFS,S3],'systemStorage':'HDFS WAL/root/metadata','canaryStorage':'S3A RFile with matching metadata and successful scan','managerRestart':'pass','labCompute':'parked after verification','originalCompute':'restored to recorded replica counts','originalInstanceID':'f255c67d-93c2-4cfc-8fe0-c2b686024598','completedAt':datetime.now(timezone.utc).isoformat()},indent=2)+'\n')
 print('Phase 3 complete.',flush=True)

def main():
 if args.action=='activate':
  assert get('job','floci-lab-storage-bootstrap')['status'].get('succeeded'),'Bootstrap must complete first'
  pause_original();switch_lab(True)
  # Pod Ready does not imply that the Accumulo RPC services have acquired locks.
  for target,host,port in [('deployment/accumulo-manager','accumulo-manager',9999),('statefulset/accumulo-tservers','accumulo-tservers',9997)]:
   kube('exec',target,'--','bash','-c','timeout 180 bash -c "until echo >/dev/tcp/'+host+'/'+str(port)+'; do sleep 2; done"',name=host+'-activation-rpc-ready.txt')
  assert 'STATE=REGISTERED' in kube('exec','deployment/accumulo-manager','--','accumulo','local.floci.LabState','floci-lab',ZK,HDFS,S3,name='activation-state.txt').stdout
  print('Lab active; original compute paused. Use --action park to restore it.')
 elif args.action=='park':switch_lab(False);restore_original()
 else:
  memory('memory-before-lab.txt');prepare();pause_original();bootstrap();switch_lab(True);verify_lab();memory('memory-active-lab.txt');switch_lab(False);restore_original();save_redacted();finish_report()

if __name__=='__main__':main()
