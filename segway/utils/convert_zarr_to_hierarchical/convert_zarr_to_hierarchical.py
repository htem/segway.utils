import json
import glob
import sys
import os
import numpy as np

dir = sys.argv[1]

zarray_fname = f'{dir}/.zarray'
with open(zarray_fname, 'r') as f:
    f = f.read()
zarray = json.loads(f)
if zarray.get('dimension_separator', '.') == '/':
    print("Zarr is already hierarchical, exiting...")
    exit()

chunks = np.array(zarray['chunks'])
shape = np.array(zarray['shape'])
print(f'chunks: {chunks}')
print(f'shape: {shape}')
chunks_shape = np.ceil(shape/chunks).astype('uint32')
print(f'chunks_shape: {chunks_shape}')

print("recursive_makedirs:")

def recursive_makedirs(lv, prefix):
    if lv == len(chunks_shape)-1:
        newdir = f'{dir}/{prefix}'
        os.makedirs(newdir, exist_ok=True)
    else:
        for i in range(chunks_shape[lv]):
            recursive_makedirs(lv+1, prefix=f'{prefix}/{i}')

for i in range(chunks_shape[0]):
    print(i, end='..', flush=True)
    recursive_makedirs(1, str(i))
print()

print("recursive_move:")

def recursive_move(lv, prefix):
    if lv == len(chunks_shape):
        src = prefix.replace('/', '.')
        src = f'{dir}/{src}'
        dst = f'{dir}/{prefix}'
        try:
            os.rename(src, dst)
        except:
            print(f'Failed moving {src} to {dst}')
    else:
        for i in range(chunks_shape[lv]):
            recursive_move(lv+1, prefix=f'{prefix}/{i}')

for i in range(chunks_shape[0]):
    print(i, end='..', flush=True)
    recursive_move(1, str(i))
print()

zarray['dimension_separator'] = '/'
new_json = json.dumps(zarray, sort_keys=True, indent=4)
with open(zarray_fname, 'w') as f:
    f.write(new_json)
