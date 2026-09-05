from pathlib import Path
import json,statistics,csv
root=Path(__file__).resolve().parent/'results/core'
summary={}
for backend in ['postgres','mysql','dynamodb','scylladb','surrealdb']:
 for side in ['baseline','after']:
  groups={}
  for p in root.glob(f'c3-durable-{backend}-0-{side}-[123].txt'):
   for line in p.read_text().splitlines():
    if line.startswith(f'ANNOTATION_CSV,{backend},'):
     row=line.split(',');groups.setdefault(row[2],[]).append(int(row[4]))
  summary[f'c3-{backend}-{side}']={k:{'samples':len(v),'median_us':statistics.median(v),'min_us':min(v),'max_us':max(v)} for k,v in groups.items()}
for side in ['baseline','after']:
 groups={}
 for p in root.glob(f'codec-{side}-*.csv'):
  lines=[line for line in p.read_text().splitlines() if line.startswith('CODEC_CSV,')]
  for row in csv.DictReader(lines):
   k=row['appends']
   for field,value in row.items():
    if field not in ['CODEC_CSV','appends']:groups.setdefault(k,{}).setdefault(field,[]).append(int(value))
 summary['codec-'+side]={k:{field:{'samples':len(v),'median':statistics.median(v),'min':min(v),'max':max(v)} for field,v in fields.items()} for k,fields in groups.items()}
(root/'durable-core-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
