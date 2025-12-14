import json,sys,subprocess
graph=json.loads(open(sys.argv[1]).read())
print('digraph G {')
for src,dsts in graph.items():
    for dst in dsts:print(f'{src} -> {dst};')
print('}')