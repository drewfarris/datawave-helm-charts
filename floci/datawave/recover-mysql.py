#!/usr/bin/env python3
"""Validate (default) or recover this existing MySQL group after a complete outage."""
import argparse,json,base64,subprocess,sys
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument("--apply",action="store_true",help="Recover after reviewing the default dry run; never uses force")
args=parser.parse_args()
s=json.loads(subprocess.check_output(['kubectl','-n','default','get','secret','mysql-cluster-privsecrets','-o','json']))['data']
u=base64.b64decode(s['clusterAdminUsername']).decode();p=base64.b64decode(s['clusterAdminPassword']).decode()
code='import mysqlsh\nshell=mysqlsh.globals.shell\ndba=mysqlsh.globals.dba\nshell.connect('+repr({'user':u,'password':p,'host':'mysql-cluster-0.mysql-cluster-instances.default.svc.cluster.local','port':3306})+')\n'
if args.apply:code+='dba.reboot_cluster_from_complete_outage("mysql_cluster")\n'
else:code+='dba.reboot_cluster_from_complete_outage("mysql_cluster", {"dryRun":True})\n'
r=subprocess.run(["kubectl","-n","default","exec","-i","mysql-cluster-0","-c","sidecar","--","bash","-c",'umask 077; script=$(mktemp); trap \'rm -f "$script"\' EXIT; cat >"$script"; mysqlsh --py --file="$script"'],input=code,text=True,capture_output=True)
print(r.stdout.replace(p,'<redacted>'));print(r.stderr.replace(p,'<redacted>'));sys.exit(r.returncode)
