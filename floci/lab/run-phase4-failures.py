#!/usr/bin/env python3
"""Restart/outage tests scoped to the disposable lab; always restore Floci routing."""
import argparse,importlib.util,json,subprocess,sys,time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);p.add_argument('--stage',choices=['restarts','crash','outage'],required=True);a=p.parse_args();stage=a.stage
sys.argv=[sys.argv[0],'--evidence',str(a.evidence)]
sys.dont_write_bytecode=True
s=importlib.util.spec_from_file_location('lab',Path(__file__).with_name('run-phase3.py'));lab=importlib.util.module_from_spec(s);s.loader.exec_module(lab)
E=lab.E

def probes(stage,count=2000):
 for attempt in range(3 if stage in ['verify','wal-verify'] else 1):
  try:
   lab.run(['python3',str(Path(__file__).with_name('run-phase4.py')),'--evidence',str(E),'--stage',stage,'--count',str(count)],name=stage+'-runner.log',timeout=600);return
  except RuntimeError:
   if stage not in ['verify','wal-verify'] or attempt==2:raise
   save_probe('recovery-scan-probe.log' if stage=='verify' else 'wal-recovered-scan-probe.log',stage+'-reassignment-wait-'+str(attempt)+'.log');time.sleep(5)

def save_probe(source,target): (E/target).write_bytes((E/source).read_bytes())
def state(name):return lab.kube('get','pod','accumulo-tservers-0','-o','json',name=name)
if stage=='restarts':
 lab.run(['kubectl','-n','aws-emulation','get','pod','floci-0','-o','json'],name='floci-before-restart.json')
 lab.run(['kubectl','-n','aws-emulation','delete','pod','floci-0','--wait=true','--timeout=60s'])
 lab.run(['kubectl','-n','aws-emulation','wait','--for=condition=Ready','pod/floci-0','--timeout=120s'])
 probes('verify');save_probe('recovery-scan-probe.log','after-floci-restart-scan.log');save_probe('recovery-files-probe.log','after-floci-restart-files.log')
 state('tserver-before-restart.json');lab.kube('delete','pod','accumulo-tservers-0','--wait=true','--timeout=90s');lab.kube('wait','--for=condition=Ready','pod/accumulo-tservers-0','--timeout=120s')
 probes('verify');save_probe('recovery-scan-probe.log','after-tserver-restart-scan.log');save_probe('recovery-files-probe.log','after-tserver-restart-files.log')
elif stage=='crash':
 probes('wal');state('tserver-before-crash.json')
 lab.kube('exec','pod/floci-phase4-client','--','hdfs','dfs','-ls','-R',lab.HDFS+'/wal',name='wal-before-crash.txt')
 before_pod=json.loads((E/'tserver-before-crash.json').read_text())
 container_id=before_pod['status']['containerStatuses'][0]['containerID'].split('://',1)[1]
 assert before_pod['spec']['nodeName']=='minikube-m03','Unexpected lab tablet-server node'
 lab.run(['docker','exec','minikube-m03','ctr','-n','k8s.io','tasks','kill','--signal','SIGKILL',container_id],name='sigkill.txt')
 deadline=time.monotonic()+120
 before=json.loads((E/'tserver-before-crash.json').read_text())['status']['containerStatuses'][0]['restartCount']
 while time.monotonic()<deadline:
  pod=lab.get('pod','accumulo-tservers-0');cs=pod['status'].get('containerStatuses',[])
  if cs and cs[0]['restartCount']>before and cs[0].get('ready'):break
  time.sleep(2)
 else:raise TimeoutError('Tablet server did not restart')
 after=json.loads(state('tserver-after-crash.json').stdout)
 assert after['status']['containerStatuses'][0]['lastState']['terminated']['exitCode']==137,'Crash was not SIGKILL'
 probes('wal-verify')
 lab.kube('logs','accumulo-tservers-0',name='tserver-wal-recovery.log')
elif stage=='outage':
 # Fresh table, acknowledged WAL mutations, no S3 flush until routing is removed.
 def probe(action,*extra,name):return lab.kube('exec','pod/floci-phase4-client','--','env','CLASSPATH=/tmp/phase4.jar','accumulo','local.floci.StorageSemantics',action,'phase4_outage2',*map(str,extra),name=name)
 probe('create',name='outage-create.log');time.sleep(3);probe('write',0,100,name='outage-wal-ack.log')
 service=json.loads(lab.run(['kubectl','-n','aws-emulation','get','service','floci','-o','json'],name='floci-service-before-outage.json').stdout)
 selector=service['spec']['selector'];bad=dict(selector,phase4outage='unmatched')
 assert lab.get('statefulset','floci','aws-emulation')['spec']['replicas']==1,'Expected one running Floci backend'
 try:
  lab.run(['kubectl','-n','aws-emulation','scale','statefulset/floci','--replicas=0'])
  lab.run(['kubectl','-n','aws-emulation','wait','--for=delete','pod/floci-0','--timeout=90s'])
  lab.run(['kubectl','-n','aws-emulation','patch','service','floci','--type=json','-p',json.dumps([{'op':'replace','path':'/spec/selector','value':bad}])])
  time.sleep(3)
  endpoints=lab.run(['kubectl','-n','aws-emulation','get','endpoints','floci','-o','json'],name='outage-endpoints.json')
  assert not json.loads(endpoints.stdout).get('subsets'),'Service still has endpoints'
  # New JVM has no cached S3 client or file data. Timeout executes inside the container.
  started=time.monotonic()
  read=lab.kube('exec','pod/floci-phase4-client','--','timeout','90','env','CLASSPATH=/tmp/phase4.jar','accumulo','local.floci.StorageSemantics','files','phase4_records',name='outage-fresh-read.log',check=False,timeout=110)
  assert read.returncode!=0,'Read unexpectedly succeeded with no endpoint'
  started_flush=time.monotonic()
  flush=lab.kube('exec','pod/floci-phase4-client','--','timeout','90','env','CLASSPATH=/tmp/phase4.jar','accumulo','local.floci.StorageSemantics','flush','phase4_outage2',name='outage-flush.log',check=False,timeout=110)
  assert flush.returncode!=0,'Flush unexpectedly succeeded with no endpoint'
  hdfs=lab.kube('exec','pod/floci-phase4-client','--','hdfs','dfs','-ls','-R',lab.HDFS+'/tables',name='outage-hdfs-inventory.txt').stdout
  tid=__import__('re').search('CREATED id=(\\w+)',(E/'outage-create.log').read_text()).group(1)
  assert not any('/tables/'+tid+'/' in x and '.rf' in x for x in hdfs.splitlines()),'S3 assigned table silently wrote HDFS RFiles'
  lab.kube('logs','accumulo-tservers-0','--tail=250',name='outage-tserver-errors.log')
  (E/'outage-result.json').write_text(json.dumps({'readExit':read.returncode,'flushExit':flush.returncode,'readSeconds':round(started_flush-started,2),'flushSeconds':round(time.monotonic()-started_flush,2),'hdfsFallbackFiles':0,'scope':'client operations bounded at 90s; server flush may continue retrying until endpoint returns'},indent=2)+'\n')
 finally:
  lab.run(['kubectl','-n','aws-emulation','patch','service','floci','--type=json','-p',json.dumps([{'op':'replace','path':'/spec/selector','value':selector}])])
  lab.run(['kubectl','-n','aws-emulation','scale','statefulset/floci','--replicas=1'])
  lab.run(['kubectl','-n','aws-emulation','wait','--for=condition=Ready','pod/floci-0','--timeout=120s'])
 time.sleep(3);probe('flush',name='outage-restored-flush.log');probe('scan',100,name='outage-restored-scan.log');probe('files',name='outage-restored-files.log')
 probes('verify',count=2200);save_probe('recovery-scan-probe.log','after-endpoint-recovery-scan.log');save_probe('recovery-files-probe.log','after-endpoint-recovery-files.log')
print('Failure stage '+stage+' complete.',flush=True)
