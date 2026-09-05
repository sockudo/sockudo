from pathlib import Path
import hashlib,json,subprocess,time,os
root=Path(__file__).resolve().parent
manifest={'profile':'release (codegen-units=1, LTO=true)','features':'default v2 + scylladb,versioned-messages','scope':'isolated production read_page and bounded Scylla query helper; fixture/session instrumentation identical','baseline_commit':(root/'baseline-commit.txt').read_text().strip(),'samples_per_shape':9,'fixture':'sockudo-perf-fanout-scylla at localhost:19043; Scylla2025.3,2shards,2GiB' ,'commands':[],'binaries':{}}
for variant in ['baseline','after']:
 binary=root/'binaries'/f'c8-{variant}'
 manifest['binaries'][variant]=hashlib.sha256(binary.read_bytes()).hexdigest()
 cmd=['/usr/bin/time','-v',str(binary),'benchmark_scylla_bounded_history_pages','--ignored','--exact','--nocapture','--test-threads=1']
 cmd[3]='history::scylla::page_tests::benchmark_scylla_bounded_history_pages'
 manifest['commands'].append(cmd)
 with (root/'results/fanout'/f'C8-{variant}.txt').open('w') as out, (root/'results/fanout'/f'C8-{variant}.resources.txt').open('w') as err:
  result=subprocess.run(cmd,stdout=out,stderr=err,env={**os.environ,'SOCKUDO_C8_SCYLLA_ADDR':'127.0.0.1:19043'})
 manifest.setdefault('exit_codes',{})[variant]=result.returncode
 if result.returncode:break
(root/'results/fanout/C8-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
if any(manifest['exit_codes'].values()):raise SystemExit(1)
