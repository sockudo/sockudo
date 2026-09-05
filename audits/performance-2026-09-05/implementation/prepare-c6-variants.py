from pathlib import Path
import shutil,json
root=Path(__file__).resolve().parents[3]
audit=Path(__file__).resolve().parent
base=audit/'baseline';variants=audit/'variants';hist=Path('crates/sockudo-server/src/history')
files={
'postgres':['schema.rs','writers.rs','history_store_impl.rs'],
'mysql':['schema.rs','writes.rs','store_impl.rs','mod.rs'],
'dynamodb':['degraded.rs','items.rs','mod.rs','queries.rs','store_impl.rs','writes.rs'],
'surreal':['entries.rs','records.rs','resources.rs','state.rs','store_impl.rs','mod.rs'],
'scylla':['entries.rs','mod.rs','schema.rs','store_impl.rs','retention.rs'],
}
def method(text,name):
 start=text.rfind('\n',0,text.index('async fn '+name))+1;body=text.index('{',text.index('async fn '+name));depth=1;end=body+1
 while depth:
  if text[end]=='{':depth+=1
  elif text[end]=='}':depth-=1
  end+=1
 return text[start:end]
def replace(text,name,new):
 old=method(text,name);return text.replace(old,new,1)
for name in ['c6-baseline-instrumented','c6']:
 dest=variants/name
 if not dest.exists():shutil.copytree(base,dest)
 if name=='c6':
  for backend,names in files.items():
   for filename in names:shutil.copyfile(root/hist/backend/filename,dest/hist/backend/filename)
  p=dest/hist/'scylla/entries.rs';s=p.read_text();s=replace(s,'load_history_page',method((base/hist/'scylla/entries.rs').read_text(),'load_history_items_for_stream'));p.write_text(s)
  p=dest/hist/'scylla/store_impl.rs';s=p.read_text();s=replace(s,'read_page',method((base/hist/'scylla/store_impl.rs').read_text(),'read_page'));p.write_text(s)
  p=dest/hist/'scylla/mod.rs';s=p.read_text().replace('HistoryAppendRecord, HistoryCursor,','HistoryAppendRecord, HistoryCursor, HistoryDirection,');p.write_text(s)
  shutil.copyfile(root/hist/'scylla/page_tests.rs',dest/hist/'scylla/page_tests.rs')
 else:
  p=dest/hist/'scylla/mod.rs';s=p.read_text();current=(root/hist/'scylla/mod.rs').read_text();a=current.index('    async fn new(');b=current.index('    async fn load_stream_record(',a);sa=s.index('    async fn new(');sb=s.index('    async fn load_stream_record(',sa);s=s[:sa]+current[a:b]+s[sb:];s=s.replace('pub(super) mod tests;','mod tests;').replace('mod tests;','pub(super) mod tests;');p.write_text(s)
 shutil.copyfile(root/hist/'scylla/tests.rs',dest/hist/'scylla/tests.rs')
 shutil.copyfile('/tmp/history-benchmark-tests.rs',dest/hist/'benchmark_tests.rs')
 p=dest/hist/'mod.rs';s=p.read_text();s=s.replace('\n#[cfg(test)]\nmod benchmark_tests;\n','');s+='\n#[cfg(test)]\nmod benchmark_tests;\n';p.write_text(s)
(audit/'results/fanout/C6-source-manifest.json').write_text(json.dumps({'baseline_commit':(audit/'baseline-commit.txt').read_text().strip(),'production_files':files,'isolation':'C8 page methods restored to unchanged baseline in C6 variant; no version or annotation store changes','instrumentation':'shared source benchmark_tests.rs; injected Scylla session fixture constructor outside timed region'},indent=2)+'\n')
