#!/usr/bin/env python3
"""Capture real DataWave S3 RFile references from an isolated temporary JVM Pod."""
import argparse,base64,copy,importlib.util,json,re,sys,tempfile
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);p.add_argument('--flush',action='store_true');p.add_argument('--rollback',action='store_true');p.add_argument('--compact',action='store_true');a=p.parse_args();do_flush=a.flush;do_rollback=a.rollback;do_compact=a.compact
sys.argv=[sys.argv[0],'--evidence',str(a.evidence)];sys.dont_write_bytecode=True
ROOT=Path(__file__).resolve().parent
s=importlib.util.spec_from_file_location('lab',ROOT.parent/'lab/run-phase3.py');lab=importlib.util.module_from_spec(s);s.loader.exec_module(lab)
name='floci-phase5-files'
if lab.get('pod',name) is None:
 template=lab.get('deployment','accumulo-manager')['spec']['template']['spec'];container=copy.deepcopy(template['containers'][0]);container['name']='files';container['command']=['sleep','infinity'];container.pop('args',None)
 for field in ['livenessProbe','readinessProbe','startupProbe','ports','lifecycle']:container.pop(field,None)
 container['resources']={'requests':{'cpu':'50m','memory':'64Mi'},'limits':{'memory':'512Mi'}}
 podspec={k:copy.deepcopy(template[k]) for k in ['nodeSelector','securityContext','volumes'] if k in template};podspec.update(restartPolicy='Never',automountServiceAccountToken=False,enableServiceLinks=False,containers=[container])
 lab.apply({'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'namespace':lab.NS},'spec':podspec})
lab.kube('wait','--for=condition=Ready','pod/'+name,'--timeout=90s')
with tempfile.TemporaryDirectory(prefix='floci-datawave-java-') as td:
 td=Path(td);classes=td/'classes';classes.mkdir();m2=Path.home()/'.m2/repository';jars=[m2/'org/apache/accumulo/accumulo-core/2.1.5/accumulo-core-2.1.5.jar',m2/'org/apache/hadoop/hadoop-common/3.4.1/hadoop-common-3.4.1.jar']
 lab.run(['javac','--release','11','-cp',':'.join(map(str,jars)),'-d',str(classes),str(ROOT/'DataWaveFiles.java'),str(ROOT/'RFileReadProbe.java'),str(ROOT/'LabRollback.java')],name='datawave-probes-compile.log')
 jar=td/'datawave-files.jar';lab.run(['jar','--create','--file',str(jar),'-C',str(classes),'.'])
 encoded=base64.b64encode(jar.read_bytes()).decode();lab.kube('exec','-i',name,'--','bash','-c','base64 -d >/tmp/datawave-files.jar',stdin=encoded)
 # Retain the small probe as a ConfigMap for direct-reader checks; no credentials included.
 lab.apply({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'floci-datawave-probes','namespace':lab.NS},'binaryData':{'datawave-probes.jar':encoded}})
if do_rollback:
 result=lab.kube('exec',name,'--','env','CLASSPATH=/tmp/datawave-files.jar','accumulo','local.floci.LabRollback',name='lab-rollback.log',timeout=600)
 assert 'LAB_ROLLBACK_RESTORE=PASS' in result.stdout
 print('Lab offline-table backup/restore and default-placement rollback passed.');sys.exit(0)
result=lab.kube('exec',name,'--','env','CLASSPATH=/tmp/datawave-files.jar','accumulo','local.floci.DataWaveFiles',*(['compact'] if do_compact else ['flush'] if do_flush else []),name='datawave-files.log',timeout=480)
files=[{'table':x[0],'uri':x[1],'bytes':int(x[2]),'sha256':x[3]} for x in re.findall(r'DWFILE table=(\S+) uri=(\S+) bytes=(\d+) sha256=(\w+)',result.stdout)]
assert files and 'DATAWAVE_FILES=PASS' in result.stdout
(lab.E/'datawave-files.json').write_text(json.dumps({'files':files,'totalBytes':sum(x['bytes'] for x in files),'namespace':lab.NS,'instance':'floci-lab'},indent=2)+'\n')
print('Verified '+str(len(files))+' DataWave S3 RFile references and hashes.',flush=True)
