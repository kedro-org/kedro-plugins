import os
env = os.environ

for k,v in env.items():
    if k.startswith("GITHUB") or k.startswith("CIRCLE"):
        print(k, ":", v)