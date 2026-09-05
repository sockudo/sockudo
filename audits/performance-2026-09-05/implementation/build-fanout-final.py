from pathlib import Path
import subprocess,shutil,hashlib,json,re,sys,os
root=Path(__file__).resolve().parent
repo=root.parents[2]
results=root/'results/fanout'
features='versioned-messages,ai-transport,postgres,mysql,dynamodb,surrealdb,scylladb'
builds={
 'f2-rabbit-baseline':('f2-f5-baseline-instrumented','sockudo-adapter',['--features','redis,nats,kafka,rabbitmq','--test','fanout_performance_live'],'f2-rabbit-baseline'),
 'f2-rabbit':('f2','sockudo-adapter',['--features','redis,nats,kafka,rabbitmq','--test','fanout_performance_live'],'f2-rabbit-after'),
 'f2':('f2','sockudo-adapter',['--features','redis,nats,kafka','--test','fanout_performance_live'],'f2-after'),
 'f5':('f5','sockudo-adapter',['--features','redis,nats,kafka','--test','fanout_performance_live'],'f5-after'),
 'c6-baseline':('c6-baseline-instrumented','sockudo',['--features',features,'--bin','sockudo'],'c6-baseline'),
 'c6':('c6','sockudo',['--features',features,'--bin','sockudo'],'c6-after'),
 'f1':('f1','sockudo-adapter',['--lib'],'f1-after'),
}
manifest={}
for key in sys.argv[1:]:
 variant,package,args,name=builds[key]
 for file in (root/'variants'/variant/'crates').glob('*/src/**/*.rs'):file.touch()
 cmd=['cargo','test','--manifest-path',str(root/'variants'/variant/'Cargo.toml'),'-p',package,'--release',*args,'--no-run']
 print('building',key,flush=True)
 log=results/f'{key}-final-build.txt'
 with log.open('w') as out: result=subprocess.run(cmd,stdout=out,stderr=subprocess.STDOUT,cwd=repo,env={**os.environ,'CARGO_TARGET_DIR':str(repo/'target')})
 manifest[key]={'command':cmd,'exit_code':result.returncode}
 if not result.returncode:
  matches=re.findall(r'Executable .*?\(([^)]+)\)',log.read_text())
  assert len(matches)==1,matches
  artifact=Path(matches[0]);artifact=artifact if artifact.is_absolute() else repo/artifact
  shutil.copyfile(artifact,root/'binaries'/name);shutil.copymode(artifact,root/'binaries'/name)
  manifest[key]['binary_sha256']=hashlib.sha256((root/'binaries'/name).read_bytes()).hexdigest()
 (results/'final-build-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
 if result.returncode:raise SystemExit(result.returncode)
