import json
import glob
import sys
import os
import argparse
from concurrent import futures
import numpy as np

ap = argparse.ArgumentParser()
ap.add_argument("dir", type=str, help='Input zarr volume')
ap.add_argument("--n_threads", type=int, default=None, help='')
ap.add_argument("--skip_makedirs", type=int, default=0, help='')
config = ap.parse_args()
arg_config = vars(ap.parse_args())
for k, v in arg_config.items():
    globals()[k] = v


zarray_fname = f'{dir}/.zarray'
with open(zarray_fname, 'r') as f:
    f = f.read()
zarray = json.loads(f)
if zarray.get('dimension_separator', '.') == '/':
    print("Zarr is already hierarchical, exiting...")
    # exit()

chunks = np.array(zarray['chunks'])
shape = np.array(zarray['shape'])
print(f'chunks: {chunks}')
print(f'shape: {shape}')
chunks_shape = np.ceil(shape/chunks).astype('uint32')
print(f'chunks_shape: {chunks_shape}')

def recursive_makedirs(lv, prefix):
    if lv == len(chunks_shape)-1:
        newdir = f'{dir}/{prefix}'
        os.makedirs(newdir, exist_ok=True)
    else:
        for i in range(chunks_shape[lv]):
            recursive_makedirs(lv+1, prefix=f'{prefix}/{i}')

if not skip_makedirs:
    print("recursive_makedirs:")
    for i in range(chunks_shape[0]):
        print(i, end='..', flush=True)
        recursive_makedirs(1, str(i))
    print()

print("recursive_move:")

def recursive_move(lv, prefix):
    if lv == 1:
        print(prefix, end='..', flush=True)
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

with futures.ProcessPoolExecutor() as tpe:
    for i in range(chunks_shape[0]):
        # recursive_move(1, str(i))
        tpe.submit(recursive_move, 1, str(i))

tpe.shutdown()
print()

zarray['dimension_separator'] = '/'
new_json = json.dumps(zarray, sort_keys=True, indent=4)
with open(zarray_fname, 'w') as f:
    f.write(new_json)

