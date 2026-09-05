from pathlib import Path
import subprocess, json, hashlib, datetime
root=Path(__file__).resolve().parent
manifest={'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'profile':'release; LTO; codegen-units=1','repetitions':5,'runtime':'Tokio current_thread, 1ms healthy timer','workload':'8,64,192 concurrent device verifications; every result counted as completed-valid or explicit overload; <=64 all complete','baseline':'unchanged baseline production','after':'baseline + crypto.rs + public exports only','commands':[],'sha256':{}}
for mode,folder in [('baseline','target-core-bench-baseline'),('after','target-core-bench-c12')]:
 p=root/folder/'release/sockudo-core-audit-bench';manifest['sha256'][mode]=hashlib.sha256(p.read_bytes()).hexdigest()
for repeat in range(1,6):
 for mode,folder in ([('baseline','target-core-bench-baseline'),('after','target-core-bench-c12')] if repeat%2 else [('after','target-core-bench-c12'),('baseline','target-core-bench-baseline')]):
  binary=root/folder/'release/sockudo-core-audit-bench'
  output=root/'results/core'/f'c12-{mode}-{repeat}.csv'; stats=output.with_suffix('.stderr')
  command=['/usr/bin/time','-v',str(binary)];manifest['commands'].append(command)
  with output.open('w') as out, stats.open('w') as err:
   subprocess.run(command,stdout=out,stderr=err,check=True)
(root/'results/core/c12-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
