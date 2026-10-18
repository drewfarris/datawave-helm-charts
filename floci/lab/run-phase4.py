#!/usr/bin/env python3
"""Exercise an activated, isolated Floci Accumulo lab; save all probe evidence."""
import argparse,base64,importlib.util,sys,tempfile,time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);p.add_argument('--count',type=int,default=2000);p.add_argument('--stage',choices=['baseline','lifecycle','verify','wal','default','external','gc','scanserver','wal-verify','delete'],required=True);a=p.parse_args();stage=a.stage;count=a.count
sys.argv=[sys.argv[0],'--evidence',str(a.evidence)]
sys.dont_write_bytecode=True
s=importlib.util.spec_from_file_location('lab',Path(__file__).with_name('run-phase3.py'));lab=importlib.util.module_from_spec(s);s.loader.exec_module(lab)
def probe(action,table='phase4_records',*extra,label=None):
 print('Running '+(label or action),flush=True)
 return lab.kube('exec','pod/floci-phase4-client','--','env','CLASSPATH=/tmp/phase4.jar','accumulo','local.floci.StorageSemantics',action,table,*map(str,extra),name=(label or action)+'-probe.log',timeout=240)
# Isolate test JVM memory from the manager's server JVM.
if lab.get('pod','floci-phase4-client') is None:
 import copy
 template=lab.get('deployment','accumulo-manager')['spec']['template']['spec']
 container=copy.deepcopy(template['containers'][0]);container['name']='client';container['command']=['/bin/bash','-c','exec sleep infinity'];container.pop('args',None)
 for field in ['livenessProbe','readinessProbe','startupProbe','ports','lifecycle']:container.pop(field,None)
 container['resources']={'requests':{'cpu':'50m','memory':'64Mi'},'limits':{'memory':'512Mi'}}
 podspec={k:copy.deepcopy(template[k]) for k in ['nodeSelector','securityContext','volumes'] if k in template}
 podspec.update(restartPolicy='Never',automountServiceAccountToken=False,enableServiceLinks=False,containers=[container])
 lab.apply({'apiVersion':'v1','kind':'Pod','metadata':{'name':'floci-phase4-client','namespace':lab.NS},'spec':podspec})
lab.kube('wait','--for=condition=Ready','pod/floci-phase4-client','--timeout=90s')
m2=Path.home()/'.m2/repository';jars=[m2/'org/apache/accumulo/accumulo-core/2.1.4/accumulo-core-2.1.4.jar',m2/'org/apache/hadoop/hadoop-common/3.4.1/hadoop-common-3.4.1.jar']
with tempfile.TemporaryDirectory() as td:
 td=Path(td);c=td/'classes';c.mkdir();jar=td/'phase4.jar'
 lab.run(['javac','--release','11','-cp',':'.join(map(str,jars)),'-d',str(c),str(Path(__file__).parents[1]/'s3a/java/StorageSemantics.java')],name='phase4-javac.log')
 lab.run(['jar','--create','--file',str(jar),'-C',str(c),'.'])
 lab.kube('exec','-i','pod/floci-phase4-client','--','bash','-c','base64 -d >/tmp/phase4.jar',stdin=base64.b64encode(jar.read_bytes()).decode())
if stage=='baseline':
 lab.client('config -s gc.cycle.delay=5s; config -s gc.cycle.start=5s','gc-cycle-config.txt')
 probe('create');time.sleep(3)
 for start in range(0,2000,500):
  probe('write','phase4_records',start,500,label='write-'+str(start));probe('flush',label='flush-'+str(start))
 probe('scan','phase4_records',2000,label='baseline-scan');probe('files',label='baseline-files');lab.client('scan -t accumulo.metadata -c file -np','baseline-metadata.txt')
elif stage=='lifecycle':
 probe('compact',label='internal-compaction');probe('scan','phase4_records',2000,label='post-internal-scan');probe('files',label='post-internal-files');probe('split');probe('scan','phase4_records',2000,label='post-split-scan');probe('files',label='post-split-files')
elif stage=='verify':
 probe('scan','phase4_records',count,label='recovery-scan');probe('files',label='recovery-files')
elif stage=='wal':
 probe('write','phase4_records',2000,200,label='unflushed-wal-write');probe('files',label='pre-crash-files')
elif stage=='wal-verify':
 probe('scan','phase4_records',2200,label='wal-recovered-scan');probe('flush',label='wal-recovered-flush');probe('files',label='wal-recovered-files')
elif stage=='external':
 probe('external');time.sleep(5);probe('compact',label='external-compaction');probe('files',label='external-files');lab.client('config -t phase4_records -d table.compaction.dispatcher.opts.service','restore-internal-compaction-policy.log')
elif stage=='gc':
 import re
 paths=set()
 for filename in ['baseline-files-probe.log','post-internal-files-probe.log','delete-files-probe.log']:
  paths.update(re.findall(r'RFILE (\S+)',(lab.E/filename).read_text()))
 deadline=time.monotonic()+180
 while True:
  try:
   probe('absent','phase4_records',*sorted(paths),label='gc-obsolete-objects');break
  except RuntimeError:
   if time.monotonic()>=deadline:raise
   time.sleep(5)
 probe('files',label='gc-live-files');probe('scan','phase4_records',2000,label='gc-live-scan')
elif stage=='scanserver':
 probe('eventual','phase4_records',2000,label='scan-server-scan')
elif stage=='delete':
 probe('create','phase4_delete');time.sleep(3);probe('write','phase4_delete',0,100,label='delete-write');probe('flush','phase4_delete',label='delete-flush');probe('files','phase4_delete',label='delete-files');probe('delete','phase4_delete',label='delete-table')
elif stage=='default':
 probe('create-default','phase4_default');time.sleep(3);probe('write','phase4_default',0,100,label='default-write');probe('flush','phase4_default',label='default-flush');probe('scan','phase4_default',100,label='default-scan');probe('files','phase4_default',label='default-files')
print('Stage '+stage+' complete.',flush=True)
