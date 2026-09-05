from pathlib import Path
import os,subprocess,json,time,urllib.request
root=Path(__file__).resolve().parent;out=root/'results/core';repo=Path.cwd()
binary=root/'target-final-workspace/debug/sockudo'
env={**os.environ,'SOCKUDO_BASE_URL':'http://127.0.0.1:16001','SOCKUDO_WS_URL':'ws://127.0.0.1:16001/app/app-key?protocol=2&client=ait-conformance&version=0','ABLY_PORT':'16001','ABLY_ENDPOINT':'127.0.0.1'}
manifest=[]
with (out/'final-server.log').open('w') as log:
 server=subprocess.Popen([str(binary),'--config',str(root/'conformance.toml')],stdout=log,stderr=subprocess.STDOUT,env=env)
 try:
  for _ in range(120):
   if server.poll() is not None:raise RuntimeError('final server exited before readiness')
   try:
    with urllib.request.urlopen('http://127.0.0.1:16001/up',timeout=1) as r:
     if r.status==200:break
   except Exception:time.sleep(.25)
  else:raise RuntimeError('final server readiness timed out')
  for name,cmd,cwd in [('ai-live',['scripts/ai-conformance-node.sh'],repo),('ably-stock-sdk',['npm','test'],repo/'tests/ably-compat')]:
   with (out/(name+'-final.txt')).open('w') as output:
    try:status=subprocess.run(cmd,cwd=cwd,env=env,stdout=output,stderr=subprocess.STDOUT,timeout=180).returncode
    except subprocess.TimeoutExpired:status='timeout180s'
   manifest.append({'command':cmd,'cwd':str(cwd),'status':status});print(name,status,flush=True)
   (out/'final-conformance-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
 finally:
  server.terminate()
  try:server.wait(timeout=15)
  except subprocess.TimeoutExpired:server.kill();server.wait()
