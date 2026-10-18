#!/usr/bin/env python3
"""Build S3A derivatives of the captured baseline; large artifacts stay outside Git."""
import argparse,json,hashlib,subprocess,tempfile,shutil
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);args=p.parse_args()
root=Path(__file__).resolve().parent;e=args.evidence.resolve();e.mkdir(parents=True,exist_ok=True)
bases=[
 {'role':'accumulo','version':'3.4.1','base':'ghcr.io/nationalsecurityagency/datawave-stack-accumulo:2.1.4@sha256:eb7deb4f6c2d1822c26ba602ed873fbea72ca9058fa7b0af4f3269164c63007b','image':'local/datawave/accumulo-s3a:2.1.4-hadoop3.4.1','file':'hadoop.Dockerfile','user':'accumulo'},
 {'role':'ingest','version':'3.4.2','base':'local/datawave/ingest-kubernetes:8.4.0-SNAPSHOT','expected':'sha256:726d62fedc3302722443fe7a39f6d22153e1ab1980e689fb8ce3aaf84d2dfa7f','image':'local/datawave/ingest-s3a:8.4.0-SNAPSHOT-hadoop3.4.2','file':'hadoop.Dockerfile','user':'datawave'},
 {'role':'webservice','version':'3.4.1','base':'local/datawave/webservice-kubernetes:8.4.0-SNAPSHOT','expected':'sha256:21fa2580ff876a352ec77016f2e582f2f39030d83e4188ef9a0d57d2b78f35f4','image':'local/datawave/webservice-s3a:8.4.0-SNAPSHOT-hadoop3.4.1','file':'wildfly.Dockerfile','user':'datawave'}]
def run(cmd,log):
 with (e/log).open('w') as f:r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
 if r.returncode:raise RuntimeError('Failed '+log+'; see evidence')
def inspect(image):return json.loads(subprocess.check_output(['docker','image','inspect',image]))[0]
with tempfile.TemporaryDirectory(prefix='floci-s3a-build-') as temp:
 temp=Path(temp);jars={}
 for version in ['3.4.1','3.4.2']:
  dest=temp/version;dest.mkdir();shutil.copy(root/'dependencies-pom.xml',dest/'pom.xml')
  run(['mvn','-B','-f',str(dest/'pom.xml'),'-Dhadoop.version='+version,'org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies','-DincludeScope=runtime','-DoutputDirectory='+str(dest/'libs')],'resolve-'+version+'.log')
  jars[version]=[]
  for f in sorted((dest/'libs').glob('*.jar')):
   h=hashlib.sha256()
   with f.open('rb') as stream:
    for block in iter(lambda:stream.read(1048576),b''):h.update(block)
   jars[version].append({'name':f.name,'bytes':f.stat().st_size,'sha256':h.hexdigest()})
  print('Resolved Hadoop '+version+' S3A libraries',flush=True)
 for base in bases:
  try:info=inspect(base['base'])
  except subprocess.CalledProcessError:
   if base.get('expected'):raise
   run(['docker','pull',base['base']],'pull-'+base['role']+'.log');info=inspect(base['base'])
  if base.get('expected'):assert info['Id']==base['expected'],'Baseline image tag changed: '+base['role']
  base['baseID']=info['Id']
  # Freeze local SNAPSHOT tags through a unique, verified base tag for this build.
  frozen='local/datawave/floci-base:'+base['role']+'-'+info['Id'].split(':')[1][:12]
  subprocess.run(['docker','tag',info['Id'],frozen],check=True)
  context=temp/base['version'];dockerfile=context/(base['role']+'.Dockerfile')
  text=(root/'images'/base['file']).read_text()
  if base['file']=='hadoop.Dockerfile':text+='USER '+base['user']+'\n'
  dockerfile.write_text(text)
  run(['docker','build','--build-arg','BASE_IMAGE='+frozen,'-f',str(dockerfile),'-t',base['image'],str(context)],'build-'+base['role']+'.log')
  base['imageID']=inspect(base['image'])['Id'];print('Built '+base['image']+' '+base['imageID'],flush=True)
(e/'s3a-image-lock.json').write_text(json.dumps({'images':bases,'jars':jars},indent=2)+'\n')
