"""Final root-only build cache: never shared with source-isolation variants."""
from pathlib import Path
import subprocess,os,json,hashlib,datetime
root=Path(__file__).resolve().parent
out=root/'results/core';target=root/'target-final-workspace'
env={**os.environ,'CARGO_TARGET_DIR':str(target),'CARGO_BUILD_JOBS':'8'}
checks=[('final-workspace-tests',['cargo','test','--offline','--workspace']),('final-workspace-clippy',['cargo','clippy','--offline','--workspace','--all-targets','--','-D','warnings']),('final-full-build',['cargo','build','--offline','-p','sockudo','--features','full'])]
manifest={'target':str(target),'started':datetime.datetime.now(datetime.timezone.utc).isoformat(),'checks':[]}
for name,cmd in checks:
 with (out/(name+'.txt')).open('w') as log:
  status=subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT).returncode
 manifest['checks'].append({'command':cmd,'exit_code':status,'log':name+'.txt'})
 (out/'final-workspace-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
 print(name,status,flush=True)
