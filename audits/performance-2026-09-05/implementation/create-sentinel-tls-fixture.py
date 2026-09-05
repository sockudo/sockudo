from pathlib import Path
import subprocess
root=Path('/tmp/sockudo-perf-fanout-tls');root.mkdir(exist_ok=True)
def openssl(args):
 subprocess.run(['openssl',*args],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
openssl(['req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=Audit Fixture CA','-keyout',str(root/'ca.key'),'-out',str(root/'ca.crt')])
openssl(['req','-new','-newkey','rsa:2048','-nodes','-subj','/CN=localhost','-keyout',str(root/'fixture.key'),'-out',str(root/'fixture.csr')])
(root/'cert.ext').write_text('basicConstraints=critical,CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=IP:127.0.0.1,DNS:localhost\n')
openssl(['x509','-req','-in',str(root/'fixture.csr'),'-CA',str(root/'ca.crt'),'-CAkey',str(root/'ca.key'),'-set_serial','1','-days','1','-extfile',str(root/'cert.ext'),'-out',str(root/'fixture.crt')])
for port in [16397,26397]:
 name=f'sockudo-perf-fanout-tls-{port}'
 config=root/f'{port}.conf'
 base=f'bind 127.0.0.1\nport 0\ntls-port {port}\ntls-cert-file /tls/fixture.crt\ntls-key-file /tls/fixture.key\ntls-ca-cert-file /tls/ca.crt\ntls-auth-clients yes\ntls-replication yes\n'
 if port==26397:base+='requirepass fixture-sentinel\nsentinel monitor fanout-tls 127.0.0.1 16397 1\nsentinel auth-pass fanout-tls fixture-data\n'
 else:base+='requirepass fixture-data\nsave ""\nappendonly no\n'
 config.write_text(base)
 cmd=['podman','run','-d','--name',name,'--network','host','--user','0','--entrypoint','redis-server','-v',f'{root}:/tls:Z','docker.io/library/redis:7.4.5-alpine',f'/tls/{port}.conf']
 if port==26397:cmd+=['--sentinel']
 subprocess.run(cmd,check=True)
