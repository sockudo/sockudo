"""Create only the audit-owned local Redis/Sentinel fixture; no external service changes."""
from pathlib import Path
import subprocess
image='docker.io/library/redis:7.4.5-alpine'
for port in (16395,16396,26395):
 name=f'sockudo-perf-fanout-sentinel-{port}'
 config=Path('/tmp')/f'{name}.conf'
 base=f'bind 127.0.0.1\nport {port}\nprotected-mode yes\nsave ""\nappendonly no\n'
 if port==26395:
  base += 'requirepass fixture-sentinel\nsentinel monitor fanout 127.0.0.1 16395 1\nsentinel auth-pass fanout fixture-data\nsentinel down-after-milliseconds fanout 1000\nsentinel failover-timeout fanout 10000\nsentinel parallel-syncs fanout 1\n'
 else:
  base += 'requirepass fixture-data\nmasterauth fixture-data\n'
  if port==16396:base += 'replicaof 127.0.0.1 16395\n'
 config.write_text(base)
 cmd=['podman','run','-d','--name',name,'--network','host','--user','0','--entrypoint','redis-server','-v',f'{config}:/fixture.conf:Z',image,'/fixture.conf']
 if port==26395:cmd += ['--sentinel']
 subprocess.run(cmd,check=True)
