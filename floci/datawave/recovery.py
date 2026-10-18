#!/usr/bin/env python3
"""Checkpoint and verify Minikube/host reboot recovery without reinitializing storage."""
import argparse,importlib.util,json,re,sys,time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);p.add_argument('--action',choices=['checkpoint','restart-minikube','verify','verify-host'],required=True);a=p.parse_args();action=a.action
sys.argv=[sys.argv[0],'--evidence',str(a.evidence)];sys.dont_write_bytecode=True
root=Path(__file__).resolve().parent;s=importlib.util.spec_from_file_location('lab',root.parent/'lab/run-phase3.py');lab=importlib.util.module_from_spec(s);s.loader.exec_module(lab)
E=lab.E

def bootid():return Path('/proc/sys/kernel/random/boot_id').read_text().strip()
def snapshot(label):
 lab.run(['kubectl','get','nodes','-o','json'],name=label+'-nodes.json')
 lab.run(['kubectl','get','pods','-A','-o','json'],name=label+'-pods-redacted.json')
 lab.run(['kubectl','get','pvc,pv,storageclass','-A','-o','json'],name=label+'-storage.json')
 lab.run(['docker','inspect','minikube','minikube-m02','minikube-m03','--format','{{.Name}} memory={{.HostConfig.Memory}} started={{.State.StartedAt}}'],name=label+'-node-limits.txt')
 lab.run(['docker','stats','--no-stream','--format','{{.Name}} {{.MemUsage}} {{.CPUPerc}}','minikube','minikube-m02','minikube-m03'],name=label+'-node-usage.txt')
 lab.run(['sysctl','fs.inotify.max_user_instances'],name=label+'-inotify.txt')
 lab.run(['df','-h','/','/var/lib/docker'],name=label+'-disk.txt')

def probe_nodes(label,checkpoint):
 obj=checkpoint['immutableObjects'][0]
 for node in ['minikube','minikube-m02','minikube-m03']:
  name='floci-recovery-'+label+'-'+node
  existing=lab.get('job',name,'aws-emulation')
  if existing:
   assert existing['status'].get('succeeded') or existing['status'].get('failed'),'A prior recovery probe is still running'
   lab.run(['kubectl','-n','aws-emulation','delete','job',name,'--wait=true','--timeout=30s'])
  code="import boto3,socket,hashlib,json;from botocore.config import Config;host='floci.aws-emulation.svc.cluster.local';addresses=sorted({x[4][0] for x in socket.getaddrinfo(host,4566)});s=boto3.client('s3',endpoint_url='http://'+host+':4566',region_name='us-east-1',config=Config(connect_timeout=3,read_timeout=10,retries={'max_attempts':2},s3={'addressing_style':'path'}));s.head_bucket(Bucket='dwv-accumulo-lab');body=s.get_object(Bucket='dwv-accumulo-lab',Key="+repr(obj['uri'].split('dwv-accumulo-lab/',1)[1])+")['Body'].read();digest=hashlib.sha256(body).hexdigest();assert digest=="+repr(obj['sha256'])+";print(json.dumps({'node':"+repr(node)+",'dns':addresses,'sha256':digest,'signedS3Read':'pass'}))"
  job={'apiVersion':'batch/v1','kind':'Job','metadata':{'name':name,'namespace':'aws-emulation'},'spec':{'backoffLimit':0,'activeDeadlineSeconds':60,'template':{'spec':{'restartPolicy':'Never','automountServiceAccountToken':False,'enableServiceLinks':False,'nodeSelector':{'kubernetes.io/hostname':node},'containers':[{'name':'probe','image':'local/datawave/floci-probe:phase1-20261018','imagePullPolicy':'Never','args':['-c',code],'envFrom':[{'secretRef':{'name':'floci-aws-credentials'}}],'env':[{'name':'AWS_EC2_METADATA_DISABLED','value':'true'}],'resources':{'requests':{'cpu':'50m','memory':'64Mi'},'limits':{'memory':'192Mi'}}}]}}}}
  lab.apply(job)
  completed=lab.run(['kubectl','-n','aws-emulation','wait','--for=condition=complete','job/'+name,'--timeout=90s'],check=False)
  lab.run(['kubectl','-n','aws-emulation','logs','job/'+name],name=label+'-'+node+'-signed-s3.json')
  assert completed.returncode==0,'All-node signed S3 probe failed: '+node

def verify(label):
 lab.run(['kubectl','wait','--for=condition=Ready','node','--all','--timeout=240s'],name=label+'-node-readiness.txt',timeout=260)
 for namespace,kind,name in [('aws-emulation','statefulset','floci'),(lab.NS,'deployment','accumulo-manager'),(lab.NS,'statefulset','accumulo-tservers'),(lab.NS,'deployment','accumulo-gc'),(lab.NS,'deployment','datawave-monolith'),(lab.NS,'deployment','authorization'),('default','deployment','mysql-cluster-router'),('kube-system','daemonset','kube-proxy'),('kube-system','daemonset','csi-hostpathplugin')]:
  lab.run(['kubectl','-n',namespace,'rollout','status',kind+'/'+name,'--timeout=600s'],name=label+'-'+name+'-ready.txt',timeout=620)
 assert lab.get('innodbcluster','mysql-cluster','default')['status']['cluster']['status']=='ONLINE','Shared MySQL cluster must be ONLINE'
 assert lab.get('statefulset','floci','aws-emulation')['spec']['replicas']==1
 for pod in lab.pods('kube-system'):
  if pod['metadata']['name'].startswith('csi-hostpathplugin-'):assert all(c['ready'] for c in pod['status']['containerStatuses'])
 lab.kube('exec','deployment/accumulo-manager','--','accumulo','local.floci.LabState','floci-lab',lab.ZK,lab.HDFS,lab.S3,name=label+'-registered-state.txt')
 assert 'STATE=REGISTERED' in (E/(label+'-registered-state.txt')).read_text()
 checkpoint=json.loads((E/'recovery-checkpoint.json').read_text())
 current=lab.get('pvc','floci-data','aws-emulation');assert current['metadata']['uid']==checkpoint['flociPVCUID'] and current['spec']['volumeName']==checkpoint['flociPV']
 # Verify an immutable source table object used by the successful backup/restore test.
 for obj in checkpoint['immutableObjects']:
  result=lab.kube('exec','deployment/accumulo-manager','--','bash','-c','set -o pipefail; hdfs dfs -cat "$1" | sha256sum','verify',obj['uri'],name=label+'-immutable-'+str(checkpoint['immutableObjects'].index(obj))+'.txt')
  assert obj['sha256'] in result.stdout
 lab.kube('exec','deployment/datawave-monolith','--','getent','hosts','floci.aws-emulation.svc.cluster.local',name=label+'-floci-dns.txt')
 for node in ['minikube','minikube-m02','minikube-m03']:
  lab.run(['docker','exec',node,'test','-r','/etc/resolv.conf'])
 probe_nodes(label,checkpoint)
 snapshot(label)
 before_inotify=int((E/'before-reboot-inotify.txt').read_text().split('=')[-1]);current_inotify=int((E/(label+'-inotify.txt')).read_text().split('=')[-1]);assert current_inotify>=before_inotify,'inotify limit regressed'
 limits=(E/(label+'-node-limits.txt')).read_text();assert limits.count('memory=8589934592')==3,'Expected 8GiB per Docker node'
 before=json.loads((E/'before-reboot-pods-redacted.json').read_text())
 expected={(p['metadata']['namespace'],c['name']):c['imageID'] for p in before['items'] if p['metadata']['namespace'] in [lab.NS,'aws-emulation'] for c in p['status'].get('containerStatuses',[]) if c.get('ready')}
 actual=json.loads((E/(label+'-pods-redacted.json')).read_text())
 after={(p['metadata']['namespace'],c['name']):c['imageID'] for p in actual['items'] if p['metadata']['namespace'] in [lab.NS,'aws-emulation'] for c in p['status'].get('containerStatuses',[]) if c.get('ready')}
 for key,image in expected.items():
  if key[1] in ['accumulo-manager','accumulo-tserver','accumulo-gc','datawave-monolith','floci']:assert after.get(key)==image,(key,after.get(key),image)
 (E/(label+'-result.json')).write_text(json.dumps({'status':'pass','hostBootID':bootid(),'instance':'floci-lab','retainedPVC':checkpoint['flociPVCUID'],'immutableObjects':checkpoint['immutableObjects'],'runningRoleImageIDsRetained':True,'notes':'Query recovery is verified by a separate new test Job.'},indent=2)+'\n')

if action=='checkpoint':
 assert (E/'lab-rollback.log').exists() and 'LAB_ROLLBACK_RESTORE=PASS' in (E/'lab-rollback.log').read_text()
 objects=[{'uri':u,'sha256':h} for u,h in re.findall(r'BACKUP_FILE source=(\S+) backup=\S+ sha256=(\w+)',(E/'lab-rollback.log').read_text())];assert objects
 pvc=lab.get('pvc','floci-data','aws-emulation')
 snapshot('before-reboot')
 (E/'recovery-checkpoint.json').write_text(json.dumps({'hostBootID':bootid(),'flociPVCUID':pvc['metadata']['uid'],'flociPV':pvc['spec']['volumeName'],'immutableObjects':objects,'labInstanceID':'2f824244-c68b-4e29-8f9b-38bf74c5f054','originalReplicaSnapshot':'original-replicas.json','resumeCommand':'python3 floci/datawave/recovery.py --action verify-host --evidence '+str(E)},indent=2)+'\n')
elif action=='restart-minikube':
 assert (E/'recovery-checkpoint.json').exists(),'Create checkpoint first'
 lab.run(['minikube','stop','-p','minikube'],name='minikube-stop.txt',timeout=300)
 lab.run(['minikube','start','-p','minikube','--driver=docker','--nodes=3','--memory=8192'],name='minikube-start.txt',timeout=900)
 verify('after-minikube')
elif action=='verify-host':
 assert bootid()!=json.loads((E/'recovery-checkpoint.json').read_text())['hostBootID'],'Host boot ID has not changed; do not claim a host reboot'
 verify('after-host')
else:verify('manual-verification')
print('Recovery '+action+' complete.',flush=True)
