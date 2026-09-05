from pathlib import Path
import json,statistics
root=Path(__file__).resolve().parent/'results/fanout'
for finding,fields,metrics in [('F1',['count','payload','mixed'],['prepare_ns','verified_ns']),('F4',['count','payload','unique'],['ns']),('C6',['cap','payload'],['ns'])]:
 variants={}
 for variant in ['baseline','after']:
  files=[root/f'{finding}-{variant}.txt'] if finding!='C6' else list(root.glob(f'C6-*-{variant}.txt'))
  shapes={}
  for file in files:
   if not file.exists(): continue
   for line in file.read_text().splitlines():
    if finding+',' not in line:continue
    line=finding+','+line.split(finding+',',1)[1]
    row=dict(part.split('=',1) for part in line.split(',')[1:])
    key=','.join(f'{key}={row[key]}' for key in fields)
    if finding=='C6':key=file.name.split('-')[1]+','+key
    shapes.setdefault(key,[]).append(row)
  if not shapes:break
  assert all(len(rows)==9 for rows in shapes.values()), [(key,len(rows)) for key,rows in shapes.items()]
  variants[variant]={key:{metric:{'median':statistics.median(int(row[metric]) for row in rows),'min':min(int(row[metric]) for row in rows),'max':max(int(row[metric]) for row in rows),'samples':len(rows)} for metric in metrics} for key,rows in shapes.items()}
 if variants:
  (root/f'{finding}-summary.json').write_text(json.dumps(variants,indent=2)+'\n')
  if finding=='F1':
   for shape in variants['baseline']:
    print(shape,[(metric,round(variants['baseline'][shape][metric]['median']/1e6,3),round(variants['after'][shape][metric]['median']/1e6,3)) for metric in metrics])
