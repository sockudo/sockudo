from pathlib import Path
import csv,json,statistics
root=Path(__file__).resolve().parent/'results/core'
def aggregate(paths, keys):
 data={}
 for path in paths:
  for row in csv.DictReader(path.open()):
   if not row or not row.get(keys[0]): continue
   key=tuple(row[k] for k in keys)
   for name,value in row.items():
    if name in keys: continue
    try: number=float(value)
    except (TypeError,ValueError): continue
    data.setdefault(key,{}).setdefault(name,[]).append(number)
 return [{**dict(zip(keys,key)),**{name:{'min':min(values),'median':statistics.median(values),'max':max(values),'repetitions':len(values)} for name,values in fields.items()}} for key,fields in data.items()]
result={}
for mode in ['baseline','after']:
 result['C12_'+mode]=aggregate(sorted(root.glob(f'c12-{mode}-*.csv')),['scenario','offered'])
for mode in ['baseline','c1-after','c5-after']:
 result[mode]=aggregate(sorted(root.glob(f'state_stores-{mode}-*.csv')),['operation','retained','payload_bytes'])
(root/'component-summary.json').write_text(json.dumps(result,indent=2)+'\n')
for name in ['C12_baseline','C12_after']:
 for row in result[name]:
  if row['offered']=='64':print(name,json.dumps(row))
