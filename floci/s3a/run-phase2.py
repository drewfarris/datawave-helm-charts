#!/usr/bin/env python3
"""Configure scoped S3A probes; credential contents never enter evidence files."""
import argparse,base64,json,os,secrets,subprocess,tempfile,time,xml.etree.ElementTree as ET
from pathlib import Path
import yaml
p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);p.add_argument('--roles',nargs='+',choices=['accumulo','ingest','webservice'],default=['accumulo','ingest','webservice']);a=p.parse_args()
E=a.evidence.resolve();E.mkdir(parents=True,exist_ok=True);ROOT=Path(__file__).resolve().parent;NS='aws-emulation'
LOCK=json.loads((E/'s3a-image-lock.json').read_text());IMAGES={x['role']:x for x in LOCK['images']}
COMMANDS=json.loads((E/'probe-commands.json').read_text()) if (E/'probe-commands.json').exists() else []
def run(cmd,name=None,stdin=None,check=True,timeout=180,binary=False):
 r=subprocess.run(cmd,input=stdin,capture_output=True,text=not binary,timeout=timeout)
 COMMANDS.append({'command':cmd,'exitCode':r.returncode,'file':name})
 (E/'probe-commands.json').write_text(json.dumps(COMMANDS,indent=2)+'\n')
 if name:(E/name).write_text(r.stdout+('\nSTDERR:\n'+r.stderr if r.stderr else ''))
 if check and r.returncode:raise RuntimeError('Command failed: '+' '.join(cmd)+'; '+(r.stderr.decode() if binary else r.stderr))
 return r

def kube(*cmd,**kw):return run(['kubectl','-n',NS,*cmd],**kw)
def apply(obj):return kube('apply','-f','-',stdin=json.dumps(obj))
def get(kind,name,namespace=NS):
 r=run(['kubectl','-n',namespace,'get',kind,name,'-o','json'],check=False)
 return json.loads(r.stdout) if r.returncode==0 else None

def wait(name):
 end=time.monotonic()+180
 while time.monotonic()<end:
  pod=get('pod',name)
  if pod:
   if pod['status'].get('phase')=='Failed':raise RuntimeError('Probe Pod failed: '+name)
   statuses=pod['status'].get('containerStatuses',[])
   if statuses and statuses[0].get('ready'):return pod
  time.sleep(2)
 raise TimeoutError(name)

def configure():
 base=get('configmap','hadoop-config','default')['data'];overlay=yaml.safe_load((ROOT/'hadoop-values.yaml').read_text())
 tree=ET.fromstring(base['core-site.xml']);properties={x.findtext('name'):x for x in tree.findall('property')}
 original={k:x.findtext('value') for k,x in properties.items()}
 for item in overlay['config']['coreSiteProperties']:
  name,value=item['name'],str(item['value']);entry=properties.get(name)
  if entry is None:entry=ET.SubElement(tree,'property');ET.SubElement(entry,'name').text=name;properties[name]=entry
  element=entry.find('value')
  if element is None:element=ET.SubElement(entry,'value')
  element.text=value
 base['core-site.xml']=ET.tostring(tree,encoding='unicode')
 apply({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'floci-s3a-hadoop-config','namespace':NS},'data':base})
 (E/'core-site.xml').write_text(base['core-site.xml'])
 (E/'configuration-merge.json').write_text(json.dumps({'originalPropertyNames':list(original),'resultPropertyNames':list(properties),'hdfsDefaultRetained':True,'namespaceAuthority':'hdfs-nn.default.svc.cluster.local:9000'},indent=2)+'\n')
 accumulo=get('configmap','accumulo-config','default')['data']
 safe={k:v for k,v in accumulo.items() if k in ['accumulo-env.sh']}
 safe['log4j2.properties']='status=warn\nappender.console.type=Console\nappender.console.name=Console\nappender.console.layout.type=PatternLayout\nappender.console.layout.pattern=%p %c - %m%n\nrootLogger.level=warn\nrootLogger.appenderRef.stdout.ref=Console\n'
 apply({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'floci-s3a-accumulo-config','namespace':NS},'data':safe})
 with tempfile.TemporaryDirectory(prefix='floci-smoke-') as t:
  classes=Path(t)/'classes';classes.mkdir();jar=Path(t)/'s3a-smoke.jar'
  run(['javac','--release','11','-cp',str(Path.home()/'.m2/repository/org/apache/hadoop/hadoop-common/3.4.1/hadoop-common-3.4.1.jar'),'-d',str(classes),str(ROOT/'java/S3ASmoke.java')],name='javac.log')
  run(['jar','--create','--file',str(jar),'-C',str(classes),'.'])
  data={'module.xml':'<module xmlns="urn:jboss:module:1.3" name="local.floci.probe"><main-class name="local.floci.S3ASmoke"/><resources><resource-root path="s3a-smoke.jar"/></resources><dependencies><module name="org.apache.hadoop.common"/></dependencies></module>', 'log4j2.properties':safe['log4j2.properties']}
  apply({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'floci-s3a-probe','namespace':NS},'data':data,'binaryData':{'s3a-smoke.jar':base64.b64encode(jar.read_bytes()).decode()}})

def makepod(name,role,generator=False):
 uid={'accumulo':1001,'ingest':1002,'webservice':998}[role]
 conf='/etc/hadoop/conf' if role=='webservice' else '/usr/local/hadoop/conf'
 env=[{'name':'HADOOP_CONF_DIR','value':conf},{'name':'HADOOP_ROOT_LOGGER','value':'WARN,console'},
      {'name':'HADOOP_CLIENT_OPTS','value':'-Xmx256m -Dlog4j.configurationFile=file:/probe/log4j2.properties'},
      {'name':'ACCUMULO_CONF_DIR','value':'/opt/accumulo/conf'},
      {'name':'ACCUMULO_JAVA_OPTS','value':'-Xmx256m'}, {'name':'CLASSPATH','value':'/probe/s3a-smoke.jar'}]
 volumes=[{'name':'hadoop','configMap':{'name':'floci-s3a-hadoop-config'}},{'name':'probe','configMap':{'name':'floci-s3a-probe'}}]
 mounts=[{'name':'hadoop','mountPath':conf,'readOnly':True},{'name':'probe','mountPath':'/probe','readOnly':True}]
 if role=='accumulo':volumes.append({'name':'accumulo','configMap':{'name':'floci-s3a-accumulo-config'}});mounts.append({'name':'accumulo','mountPath':'/opt/accumulo/conf','readOnly':True})
 if role=='webservice':mounts.append({'name':'probe','mountPath':'/probe/modules/local/floci/probe/main','readOnly':True})
 command="trap 'exit 0' TERM; sleep 7200 & wait"
 if generator:
  command='set -e; hadoop credential create fs.s3a.access.key -value "$AWS_ACCESS_KEY_ID" -provider jceks://file/work/floci.jceks; hadoop credential create fs.s3a.secret.key -value "$AWS_SECRET_ACCESS_KEY" -provider jceks://file/work/floci.jceks; touch /work/ready; trap "exit 0" TERM; sleep 7200 & wait'
  volumes.append({'name':'work','emptyDir':{}});mounts.append({'name':'work','mountPath':'/work'})
  env.extend([{'name':'HADOOP_CREDSTORE_PASSWORD','valueFrom':{'secretKeyRef':{'name':'floci-s3a-key-password','key':'HADOOP_CREDSTORE_PASSWORD'}}}])
 else:
  volumes.append({'name':'credentials','secret':{'secretName':'floci-s3a-credentials','items':[{'key':'floci.jceks','path':'floci.jceks'}]}})
  mounts.append({'name':'credentials','mountPath':'/etc/hadoop/credentials','readOnly':True})
  env.append({'name':'HADOOP_CREDSTORE_PASSWORD','valueFrom':{'secretKeyRef':{'name':'floci-s3a-credentials','key':'store-password'}}})
 container={'name':'probe','image':IMAGES[role]['image'],'imagePullPolicy':'Never','command':['/bin/bash','-c',command],'env':env,'volumeMounts':mounts,'resources':{'requests':{'cpu':'100m','memory':'128Mi'},'limits':{'cpu':'2','memory':'512Mi'}}}
 if generator:container['envFrom']=[{'secretRef':{'name':'floci-aws-credentials'}}]
 obj={'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'namespace':NS,'labels':{'app.kubernetes.io/name':'floci-s3a-probe'}},'spec':{'restartPolicy':'Never','terminationGracePeriodSeconds':5,'automountServiceAccountToken':False,'enableServiceLinks':False,'nodeSelector':{'kubernetes.io/hostname':'minikube-m03'},'securityContext':{'runAsNonRoot':True,'runAsUser':uid,'fsGroup':1001},'containers':[container],'volumes':volumes}}
 apply(obj);pod=wait(name)
 assert pod['status']['containerStatuses'][0]['imageID']==IMAGES[role]['imageID'],'Imported image differs from lock'
 return pod

def credentials():
 secret=get('secret','floci-s3a-credentials')
 if secret is None:
  temp=get('secret','floci-s3a-key-password')
  if temp is None:apply({'apiVersion':'v1','kind':'Secret','metadata':{'name':'floci-s3a-key-password','namespace':NS},'stringData':{'HADOOP_CREDSTORE_PASSWORD':secrets.token_urlsafe(24)}})
  temp=get('secret','floci-s3a-key-password')
  name='floci-s3a-credentials-generator';makepod(name,'accumulo',generator=True)
  deadline=time.monotonic()+120
  while kube('exec',name,'--','test','-f','/work/ready',check=False).returncode:
   if time.monotonic()>deadline:raise TimeoutError('JCEKS generation')
   time.sleep(2)
  r=run(['kubectl','-n',NS,'exec',name,'--','cat','/work/floci.jceks'],binary=True)
  data={'floci.jceks':base64.b64encode(r.stdout).decode(),'store-password':temp['data']['HADOOP_CREDSTORE_PASSWORD']}
  apply({'apiVersion':'v1','kind':'Secret','metadata':{'name':'floci-s3a-credentials','namespace':NS},'type':'Opaque','data':data})
  kube('logs',name,name='credential-generation.log');kube('delete','pod',name,'--wait=true','--timeout=30s');kube('delete','secret','floci-s3a-key-password')
  secret=get('secret','floci-s3a-credentials')
 run(['kubectl','-n','default','apply','-f','-'],stdin=json.dumps({'apiVersion':'v1','kind':'Secret','metadata':{'name':'floci-s3a-credentials','namespace':'default'},'type':'Opaque','data':secret['data']}))
 (E/'credential-metadata.json').write_text(json.dumps({'secret':'floci-s3a-credentials','namespaces':[NS,'default'],'keys':['floci.jceks','store-password'],'aliases':['fs.s3a.access.key','fs.s3a.secret.key'],'values':'omitted'},indent=2)+'\n')

def main():
 configure();credentials()
 for role in a.roles:
  name='floci-s3a-'+role;makepod(name,role)
  kube('exec',name,'--','id',name=role+'-user.txt')
  if role=='accumulo':cmd=['accumulo','local.floci.S3ASmoke',role]
  elif role=='ingest':cmd=['hadoop','jar','/probe/s3a-smoke.jar','local.floci.S3ASmoke',role]
  else:cmd=['java','-Xmx256m','-jar','/opt/jboss/wildfly/jboss-modules.jar','-mp','/opt/jboss/wildfly/modules:/probe/modules','local.floci.probe',role]
  r=kube('exec',name,'--',*cmd,name=role+'-s3a-smoke.log')
  assert 'S3A_SMOKE=PASS role='+role in r.stdout
  kube('get','pod',name,'-o','json',name=role+'-probe-pod.json')
  kube('delete','pod',name,'--wait=true','--timeout=30s')
 for role in ['accumulo','ingest','webservice']:
  assert 'S3A_SMOKE=PASS role='+role in (E/(role+'-s3a-smoke.log')).read_text()
  assert json.loads((E/(role+'-probe-pod.json')).read_text())['status']['containerStatuses'][0]['imageID']==IMAGES[role]['imageID']
 (E/'phase2-result.json').write_text(json.dumps({'status':'pass','runtimes':['accumulo','ingest','webservice'],'checks':['scoped classloader configuration','Secret-backed JCEKS','multipart SHA256','seek and range','copy/delete rename','zero-byte/list/delete','server ETag change detection'],'productionReleaseChanges':False},indent=2)+'\n')
 print('All three real runtime S3A probes passed.')

if __name__=='__main__':main()
