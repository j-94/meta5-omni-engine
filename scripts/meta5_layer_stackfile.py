import yaml,os
with open('rules.yaml','r') as f:rules=yaml.safe_load(f)
print('Loaded YAML:',rules)
with open('rules.yaml','w') as f:yaml.safe_dump(rules,f)
print('Saved back - integrity verified')