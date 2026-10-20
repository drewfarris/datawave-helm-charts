#!/usr/bin/env python3
"""Prepare the local fresh-instance overlay and namespace-local Hadoop credentials."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import shlex
import sys
import time
import uuid
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'floci/tools'))
from deploy import apply, get, merge, run

SERVER = 'local/datawave/accumulo-s3a-bootstrap:2.1.4-hadoop3.4.1'
IMAGES = [SERVER, 'local/datawave/ingest-s3a:8.4.0-SNAPSHOT-hadoop3.4.2',
          'local/datawave/webservice-s3a:8.4.0-SNAPSHOT-hadoop3.4.1']
STORE = 'floci-driver-s3a-credentials'


def overlay(values, endpoint, region, s3):
    accu = merge(yaml.safe_load((ROOT / 'accumulo/values.yaml').read_text()), values.get('dwv-accumulo', {}))
    glob = merge(accu['global'], values.get('global', {}))
    name = glob['accumulo']['instanceName']
    zk = glob['accumulo']['zookeepers']
    hdfs = os.environ.get('FLOCI_ACCUMULO_HDFS_VOLUME') or 'hdfs://' + glob['hdfs']['namenode'] + accu['storage']['hdfsDirectory']
    if not hdfs.startswith('hdfs://') or not s3.startswith('s3a://'):
        raise RuntimeError('Hybrid storage requires HDFS and S3A volume URIs')
    password = values.get('global', {}).get('accumulo', {}).get('password', accu['config']['userManagement']['rootPassword'])
    env = [{'name': 'HADOOP_CREDSTORE_PASSWORD', 'valueFrom': {'secretKeyRef': {'name': STORE, 'key': 'store-password'}}}]
    volumes = [{'name': 'floci-driver-credentials', 'secret': {'secretName': STORE, 'items': [{'key': 'floci.jceks', 'path': 'floci.jceks'}]}}]
    mounts = [{'name': 'floci-driver-credentials', 'mountPath': '/etc/hadoop/credentials', 'readOnly': True}]
    def shared(base):
        result = {}
        for field, additions in [('extraEnv', env), ('extraVolumes', volumes), ('extraVolumeMounts', mounts)]:
            named = {item['name']: item for item in base.get(field, [])}
            named.update({item['name']: item for item in additions})
            result[field] = list(named.values())
        return result
    hadoop = merge(yaml.safe_load((ROOT / 'hadoop-config/values.yaml').read_text()), values.get('dwv-hadoop-config', {}))
    props = {p['name']: p for p in hadoop['config']['coreSiteProperties']}
    if not str(props.get('fs.defaultFS', {}).get('value', '')).startswith('hdfs://'):
        raise RuntimeError('fs.defaultFS must remain HDFS')
    for p in yaml.safe_load((ROOT / 'floci/s3a/hadoop-values.yaml').read_text())['config']['coreSiteProperties']:
        if p['name'] != 'fs.defaultFS':
            props[p['name']] = p
    props['fs.s3a.endpoint']['value'] = endpoint
    props['fs.s3a.endpoint.region']['value'] = region
    ingest = merge(yaml.safe_load((ROOT / 'ingest/values.yaml').read_text()), values.get('dwv-ingest', {}))
    discover = '''for library in /lib/hadoop/hadoop-common.jar /lib/hadoop/hadoop-annotations.jar /lib/hadoop/lib/commons-collections-*.jar /lib/hadoop-hdfs/hadoop-hdfs-client.jar /lib/hadoop-mapreduce/hadoop-mapreduce-client-{core,common,jobclient}.jar /lib/hadoop-yarn/hadoop-yarn-{client,common,api}.jar /lib/hadoop-yarn/lib/websocket-{api,client,common}-*.jar; do
 [[ ! -f "$library" ]] || ADDITIONAL_INGEST_LIBS+=":$library"
done
export ADDITIONAL_INGEST_LIBS
export LOAD_JOBCACHE_CPU_MULTIPLIER=1
export JOB_CACHE_REPLICATION=2
'''
    setup = '''/opt/accumulo/bin/accumulo shell -u root -p "$PASSWORD" -e 'setauths -u root -s JBOSS_ADMIN,DW_ADMIN,AUTH_USER,DW_SERV,BAR,FOO,PRIVATE,PUBLIC'
/opt/datawave-ingest/current/bin/ingest/create-all-tables.sh -force
/opt/accumulo/bin/accumulo shell -u root -p "$PASSWORD" -e tables > /tmp/datawave-table-list
for table in datawave.queryMetrics_s datawave.queryMetrics_m; do
 if ! grep -Fxq "$table" /tmp/datawave-table-list; then
  /opt/accumulo/bin/accumulo shell -u root -p "$PASSWORD" -e "createtable $table"
 fi
done
/opt/datawave-ingest/current/bin/ingest/load-job-cache.sh
'''
    # The existing entry script creates the password symlink before setup; source it explicitly.
    setup = ('source /opt/datawave-ingest/ingest-passwd.sh\n'
             'cd /opt/datawave-ingest/current/bin/ingest\n'
             'if ! source ./ingest-env.sh; then echo "ingest-env.sh returned nonzero; validating required settings explicitly"; fi\n'
             ': "${PASSWORD:?}" "${USERNAME:?}" "${BASE_WORK_DIR:?}"\n'
             'cd /opt/datawave-ingest/current/bin/ingest\n') + setup
    setup += ('hdfs dfs -ls -R "${BASE_WORK_DIR}/jobCache*" > /tmp/datawave-jobcache-files\n'
              'grep -q datawave-ingest-core /tmp/datawave-jobcache-files\n'
              'grep -q hadoop-aws-3.4.2 /tmp/datawave-jobcache-files\n'
              'grep -q bundle-2.29.52 /tmp/datawave-jobcache-files\n')
    web = values.get('datawave-monolith-umbrella', {}).get('dwv-web', {})
    additions = web.get('datawaveRuntimeConfig', {}).get('additions', '')
    for group in ['warehouse', 'metrics', 'uuid']:
        additions += f'\n/system-property=dw.{group}.instanceName:write-attribute(name=value,value="{name}")\n/system-property=dw.{group}.zookeepers:write-attribute(name=value,value="{zk}")\n'
    acc = {**shared(accu), 'image': {'repository': SERVER.rsplit(':', 1)[0], 'tag': SERVER.rsplit(':', 1)[1], 'pullPolicy': 'Never'},
           'javaOptions': accu['javaOptions'] + ([] if any(x.startswith('-Xmx') for x in accu['javaOptions']) else ['-Xmx256m', '-Xms64m']),
           'initialization': {'auto': False}, 'hybridStorage': {'enabled': True, 'hdfsVolume': hdfs, 's3Volume': s3},
           'storage': {'volumes': [hdfs, s3]}, 'manager': {'replicaCount': 1},
           'tabletServers': {'javaOptions': ['-Xmx768m', '-Xms128m'], 'resources': {'limits': {'memory': '1536Mi'}}},
           'config': {'userManagement': {'rootPassword': password}, 'accumuloSite': {'general.volume.chooser': 'org.apache.accumulo.core.spi.fs.PreferredVolumeChooser',
                       'general.custom.volume.preferred.default': s3, 'general.custom.volume.preferred.init': hdfs,
                       'general.custom.volume.preferred.logger': hdfs}}}
    ing = {**shared(ingest), 'image': {'repository': IMAGES[1].rsplit(':', 1)[0], 'tag': IMAGES[1].rsplit(':', 1)[1], 'pullPolicy': 'Never'},
           'libs': ':'.join('/usr/lib/hadoop/lib/' + jar for jar in ['hadoop-aws-3.4.2.jar', 'bundle-2.29.52.jar', 'analyticsaccelerator-s3-1.2.1.jar', 'wildfly-openssl-2.1.4.Final.jar']),
           'deployment': {'strategy': {'type': 'Recreate'}, 'resources': {'requests': {'memory': '256Mi'}, 'limits': {'memory': '2Gi'}}},
           'startupReadyFile': '/tmp/datawave-ingest-ready',
           'config': {'instanceName': name, 'zookeeper': zk, 'accumuloPassword': password,
                      'ingestPasswd': 'export PASSWORD=' + shlex.quote(password) + '\nexport KEYSTORE_PASSWORD=root\nexport TRUSTSTORE_PASSWORD=root\n',
                      'ingestEnv': discover + ingest['config']['ingestEnv'], 'initIngestScript': setup}}
    web_overlay = {**shared(web), 'datawaveDeployment': {'image': {'name': IMAGES[2].rsplit(':', 1)[0], 'tag': IMAGES[2].rsplit(':', 1)[1], 'pullPolicy': 'Never'}},
                   'datawaveRuntimeConfig': {'additions': additions}}
    web_overlay['extraEnv'] = [e for e in web_overlay['extraEnv'] if e['name'] != 'JAVA_OPTS'] + [{'name': 'JAVA_OPTS', 'value': '-Xms128m -Xmx1024m -XX:MaxMetaspaceSize=384m -Djava.net.preferIPv4Stack=true -Dcom.google.protobuf.use_unsafe_pre22_gencode=true'}]
    web_overlay['datawaveDeployment']['replication'] = {'maxSurge': 0, 'maxUnavailable': 1}
    web_overlay['datawaveDeployment']['resources'] = {'requests': {'memory': '512Mi'}, 'limits': {'memory': '2Gi'}}
    return {'global': {**glob, 'accumulo': {**glob['accumulo'], 'password': password}}, 'dwv-accumulo': acc,
            'dwv-hadoop-config': {'config': {'coreSiteProperties': list(props.values())}}, 'dwv-ingest': ing,
            'datawave-monolith-umbrella': {'dwv-web': web_overlay}}


def credentials(namespace, aws_secret, image, node_selector=None):
    aws = get('secret', aws_secret, namespace)
    if not aws:
        raise RuntimeError('Deploy Floci and its namespace-local AWS Secret first')
    fingerprint = hashlib.sha256(json.dumps(aws['data'], sort_keys=True).encode()).hexdigest()
    old = get('secret', STORE, namespace)
    if old:
        if old['metadata'].get('annotations', {}).get('datawave/credential-source') != fingerprint:
            raise RuntimeError('Existing JCEKS credential source differs; refusing to overwrite it')
        return
    suffix = uuid.uuid4().hex[:8]
    name = 'floci-jceks-' + suffix
    password_name = name + '-password'
    password = secrets.token_urlsafe(24)
    apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': password_name, 'namespace': namespace}, 'stringData': {'password': password}})
    command = 'set -e; hadoop credential create fs.s3a.access.key -value "$AWS_ACCESS_KEY_ID" -provider jceks://file/work/floci.jceks; hadoop credential create fs.s3a.secret.key -value "$AWS_SECRET_ACCESS_KEY" -provider jceks://file/work/floci.jceks; touch /work/ready; sleep infinity'
    pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name, 'namespace': namespace}, 'spec': {
        'restartPolicy': 'Never', 'automountServiceAccountToken': False, 'enableServiceLinks': False,
        'securityContext': {'runAsUser': 1001, 'fsGroup': 1001},
        'containers': [{'name': 'credentials', 'image': image, 'imagePullPolicy': 'Never', 'command': ['bash', '-c', command],
            'envFrom': [{'secretRef': {'name': aws_secret}}], 'env': [{'name': 'HADOOP_CREDSTORE_PASSWORD', 'valueFrom': {'secretKeyRef': {'name': password_name, 'key': 'password'}}}],
            'volumeMounts': [{'name': 'work', 'mountPath': '/work'}], 'resources': {'requests': {'memory': '128Mi', 'cpu': '50m'}, 'limits': {'memory': '512Mi'}}}],
        'volumes': [{'name': 'work', 'emptyDir': {}}]}}
    if node_selector:
        pod["spec"]["nodeSelector"] = node_selector
    apply(pod)
    run('kubectl', '-n', namespace, 'wait', '--for=condition=Ready', 'pod/' + name, '--timeout=180s')
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        # Avoid treating a merely Running container as a completed credential store.
        status = __import__('subprocess').run(['kubectl', '-n', namespace, 'exec', name, '--', 'test', '-f', '/work/ready'], capture_output=True)
        if status.returncode == 0:
            break
        time.sleep(2)
    else:
        raise RuntimeError('JCEKS generation failed; inspect ' + name)
    encoded = run('kubectl', '-n', namespace, 'exec', name, '--', 'base64', '-w0', '/work/floci.jceks').strip()
    apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': STORE, 'namespace': namespace, 'annotations': {'datawave/credential-source': fingerprint}},
           'data': {'floci.jceks': encoded, 'store-password': base64.b64encode(password.encode()).decode()}})
    run('kubectl', '-n', namespace, 'delete', 'pod', name, '--wait=false')
    run('kubectl', '-n', namespace, 'delete', 'secret', password_name)


def load_image(image, node):
    # Stream directly into Docker-backed Minikube, avoiding multi-GB cache archives.
    run('docker', 'container', 'inspect', node)
    with tempfile.TemporaryFile() as errors:
        producer = subprocess.Popen(['docker', 'image', 'save', image], stdout=subprocess.PIPE, stderr=errors)
        try:
            consumer = subprocess.run(['docker', 'exec', '-i', node, 'ctr', '-n', 'k8s.io', 'images', 'import', '-'],
                                      stdin=producer.stdout, capture_output=True)
        finally:
            producer.stdout.close()
        saved = producer.wait()
        if saved or consumer.returncode:
            errors.seek(0)
            raise RuntimeError('Image import failed on ' + node + ': ' + errors.read().decode(errors='replace') + consumer.stderr.decode(errors='replace'))


def ensure_images(profile, nodes, images=IMAGES):
    def catalog(node):
        return json.loads(run('minikube', '-p', profile, 'ssh', '-n', node,
                              '--', 'sudo', 'crictl', 'images', '-o', 'json'))['images']
    names = [n['metadata']['name'] for n in nodes]
    catalogs = {name: catalog(name) for name in names}
    for image in images:
        expected = json.loads(run('docker', 'image', 'inspect', image))[0]['Id']
        tag = 'docker.io/' + image
        def present(items):
            return any(i['id'] == expected and tag in i.get('repoTags', []) for i in items)
        if all(present(catalogs[name]) for name in names):
            print('Verified cached image on all nodes: ' + image, flush=True)
            continue
        print('Loading ' + image + ' into Minikube', flush=True)
        for name in names:
            if not present(catalogs[name]):
                load_image(image, name)
        for name in names:
            catalogs[name] = catalog(name)
            if not present(catalogs[name]):
                raise RuntimeError('Imported image ID does not match Docker on node ' + name + ': ' + image)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--namespace', required=True)
    p.add_argument('--values', type=Path, required=True)
    p.add_argument('--overlay', type=Path)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--floci-namespace', default='aws-emulation')
    p.add_argument('--floci-release', default='floci')
    p.add_argument('--s3-volume', default='s3a://dwv-accumulo/accumulo')
    a = p.parse_args()
    values = merge(yaml.safe_load((ROOT / 'datawave-stack/values.yaml').read_text()), yaml.safe_load(a.values.read_text()) or {})
    if a.overlay:
        merge(values, yaml.safe_load(a.overlay.read_text()) or {})
    floci = merge(yaml.safe_load((ROOT / 'floci/values.yaml').read_text()), json.loads(run('helm', 'get', 'values', a.floci_release, '-n', a.floci_namespace, '-o', 'json')) or {})
    config = get('configmap', 'floci-aws-config', a.namespace)
    generated = overlay(values, config['data']['AWS_ENDPOINT_URL'], floci['region'], a.s3_volume)
    nodes = json.loads(run('kubectl', 'get', 'nodes', '-o', 'json'))['items']
    profiles = {n['metadata']['labels'].get('minikube.k8s.io/name') for n in nodes}
    if len(profiles) != 1 or None in profiles:
        raise RuntimeError('Fresh hybrid driver currently supports the validated local Minikube images only')
    for node in nodes:
        try:
            run('docker', 'container', 'inspect', node['metadata']['name'])
        except RuntimeError as error:
            raise RuntimeError('Fresh hybrid storage requires Docker-backed Minikube nodes') from error
    # Build in the exact runtime; javac and all matching API JARs are already in it.
    for image in ['local/datawave/accumulo-s3a:2.1.4-hadoop3.4.1', *IMAGES[1:]]:
        run('docker', 'image', 'inspect', image)
    print('Building the fresh Accumulo bootstrap image', flush=True)
    run('docker', 'build', '-t', SERVER, str(ROOT / 'floci/fresh'))
    ensure_images(next(iter(profiles)), nodes)
    credentials(a.namespace, floci['existingSecret'], SERVER)
    a.output.write_text(yaml.safe_dump(generated, sort_keys=False))
    print('Fresh hybrid overlay prepared; existing/unowned data will be rejected before manager startup.')


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, ValueError, KeyError, OSError) as error:
        print('Fresh Accumulo setup failed: ' + str(error), file=sys.stderr)
        sys.exit(1)
