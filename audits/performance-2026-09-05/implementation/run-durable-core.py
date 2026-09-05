"""Actual-backend isolated C3/C7 probes. Run only in a coordinated quiet window."""
from pathlib import Path
import datetime, hashlib, json, os, platform, subprocess, sys
ROOT=Path(__file__).resolve().parent
OUT=ROOT/'results/core'
finding=sys.argv[1]
backends=sys.argv[2:] or ['postgres','mysql','dynamodb','scylladb','surrealdb']
binaries={'baseline':ROOT/'binaries/core-durable-baseline', 'after':ROOT/('target-durable-c3' if finding=='c3' else 'target-durable-baseline')/'release/deps/sockudo-26866ec1cc67b2f2'}
case='history::audit_annotation_bench::benchmark_durable_annotation_reuse' if finding=='c3' else 'history::audit_version_batch_bench::benchmark_durable_latest_batch'
manifest={'baseline_commit':(ROOT/'baseline-commit.txt').read_text().strip(),'started':datetime.datetime.now(datetime.timezone.utc).isoformat(),'profile':'release, all five database features and versioned-messages','host':platform.platform(),'cpu':subprocess.check_output(['lscpu'],text=True),'binaries':{side:{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for side,p in binaries.items()},'runs':[]}
assert manifest['binaries']['baseline']['sha256'] != manifest['binaries']['after']['sha256']
for backend in backends:
 for count in ([0] if finding=='c3' else [int(n) for n in os.environ.get('AUDIT_COUNTS','1000,10000').split(',')]):
  for repeat in range(1,4):
   for side in (['baseline','after'] if repeat%2 else ['after','baseline']):
    prefix=OUT/f'{finding}-durable-{backend}-{count}-{side}-{repeat}'
    command=['/usr/bin/time','-v',str(binaries[side]),case,'--exact','--ignored','--nocapture','--test-threads=1']
    env={**os.environ,'AUDIT_BACKEND':backend,'AUDIT_MESSAGES':str(count)}
    with prefix.with_suffix('.txt').open('w') as out,prefix.with_suffix('.resources.txt').open('w') as err:
     try:
      result=subprocess.run(command,env=env,stdout=out,stderr=err,timeout=1200)
      status=result.returncode
     except subprocess.TimeoutExpired:
      status='timeout1200seconds'
    if status == 0 and '1 passed; 0 failed' not in prefix.with_suffix('.txt').read_text():
     status='missing executed benchmark'
    manifest['runs'].append({'backend':backend,'count':count,'side':side,'repeat':repeat,'command':command,'environment':{'AUDIT_BACKEND':backend,'AUDIT_MESSAGES':str(count)},'exit_code':status})
    (OUT/f'{finding}-durable-{backends[0]}-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(f'{finding} {backend} {count} {side} {repeat}: {status}',flush=True)
    if status:
     print('Correctness/runtime failure retained; stop this group for diagnosis',flush=True)
     sys.exit(1)
