import struct
import sys
import numpy as np
import os
import re
import json
import logging
import datetime
import random
import time
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

import trimesh
import open3d

from .mesh_getter import MeshGetter
from .checker import Checker
from .output_format import trimesh_to_blender_obj, trimesh_to_ply, trimesh_to_precomputed


class NeuronGetter:

    def __init__(self, neuron_db):
        self.neuron_db = neuron_db

    def get_all_neuron_name(self, with_children=False):
        nids = list(set(self.neuron_db.find_neuron({})))
        if not with_children:
            # filter out children objects which have `.` in their names
            nids = list(filter(lambda x: '.' not in x, nids))
        return nids

    # def is_sub_object(self, nid):
    #     return bool(re.search('axon|dendrite|soma|unknown_segment', nid))

    def get_segments(self, nid):
        """Get all segments composing nid.
        If nid is a basic object, also return segments of its children/sub objects.
        If nid is a sub object, if nid is not found, iterate through and find if 
        it has sub-compartments. E.g., a.axon -> a.axon_0, a.axon_1, etc..."""
        # TODO: implement get segments for sub-objects
        obj = self.neuron_db.get_neuron(nid, with_children=True)
        return obj.segments

def process_(obj,
                validate=False,
                merge_tex=None,
                merge_norm=None):
    # https://github.com/mikedh/trimesh/blob/fdf2de11c5d65bb54cdfa3bc2241b4593496d133/trimesh/base.py#L197
    # we need to fix the merge tolerance, the default (1e-8) is too aggressive for some meshes
    merge_tolerance = 1e-9
    if obj.is_empty:
        return obj
    with obj._cache:
        obj.remove_infinite_values()
        obj.merge_vertices(merge_tex=merge_tex,
                            merge_norm=merge_norm)
        if validate:
            obj.remove_duplicate_faces()
            obj.remove_degenerate_faces(height=merge_tolerance)
            obj.fix_normals()
    obj._cache.clear(exclude={'face_normals',
                               'vertex_normals'})
    obj.metadata['processed'] = True
    return obj

def simplify(obj, decimate_pct):
    face_count0 = len(obj.faces)
    return obj.simplify_quadratic_decimation(int(face_count0*decimate_pct))

class MeshAssembler:

    def __init__(
            self,
            out_path,
            mesh_getter,
            write_ext=None,
            decimate_pct=None,
            merge_vertices=None,
            ):

        if decimate_pct is not None:
            assert decimate_pct > 0 and decimate_pct <= 1.0

        os.makedirs(out_path, exist_ok=True)
        self.out_path = out_path
        self.mesh_getter = mesh_getter
        # self.neuron_getter = neuron_getter
        self.write_ext = write_ext
        self.decimate_pct = decimate_pct
        self.merge_vertices = merge_vertices

    def process(self, nid, segments):
        """Combine mesh files for one object and writes to disk"""

        meshes = self.mesh_getter.get_meshes(segments)

        meshes = list(map(lambda x: process_(x, validate=True), meshes))

        # if self.decimate_pct is not None:
        #     meshes = list(map(lambda x: simplify(x, self.decimate_pct), meshes))

        combined_mesh = trimesh.util.concatenate(meshes)

        if self.merge_vertices:
            combined_mesh.merge_vertices(digits_vertex=self.merge_vertices)

        if self.decimate_pct:
            combined_mesh = simplify(combined_mesh, self.decimate_pct)

        fname = os.path.join(self.out_path, nid)
        if self.write_ext is not None:
            if self.write_ext == "ply":
                trimesh_to_ply(combined_mesh, fname)
            elif self.write_ext == "obj":
                trimesh_to_blender_obj(combined_mesh, fname)
            else:
                raise RuntimeError(f"Unsupported write_ext {self.write_ext}")
        else:
            b_file = trimesh_to_precomputed(combined_mesh, fname)

    # def get_subpart(self, n_list):
    #     # get sub part of a neuron list
    #     # [interneuron_1, grc_30] -> [interneuroon_1.axon_0, grc_30.axon_0 ....]
    #     subparts = []
    #     for n in n_list:
    #         if not self.is_sub_object(n):
    #             subparts.extend(self.neuron_getter.get_children(n))
    #     return subparts
    
    # def __hash_segments(self, seg_set):
    #     # hash a list or set of segments
    #     segs_frozen = frozenset(map(int, seg_set))
    #     return str(hash(segs_frozen))

    # def _is_modified(self, nid):
    #     """Check the current list of segments for `nid` against what had been
    #     processed before.
    #     """
    #     segments = self.neuron_getter.getNeuronSegId(nid, with_child=True)
    #     return self.neuron_checker.hash(segments) == self.neuron_checker.get(nid)

def worker_fn_(assembler, nid, segments):
    assembler.process(nid, segments)

def assemble_meshes(out_path,
                mesh_path,
                neuron_list,
                neuron_db,
                num_workers=8,
                also_sub_objects=False,
                overwrite=False,
                write_ext=None,
                decimate_pct=None,
                merge_vertices=None,
                mesh_hierarchical_size=10000,
                ):

    neuron_getter = NeuronGetter(neuron_db)

    mesh_getter = MeshGetter(mesh_path=mesh_path,
                            # neuron_db=neuron_db,
                            mesh_hierarchical_size=mesh_hierarchical_size,
                            )

    checker = Checker()

    assembler = MeshAssembler(out_path=out_path,
                              write_ext=write_ext,
                              decimate_pct=decimate_pct,
                              merge_vertices=merge_vertices,
                              # neuron_getter=neuron_getter,
                              mesh_getter=mesh_getter,
                              )

    if neuron_list is None:
        neuron_list = neuron_getter.get_all_neuron_name(with_children=also_sub_objects)
    else:
        if also_sub_objects:
            # we need to add associated sub objects to the list
            pass  # TODO

    def worker_fn(nid):
        # if overwrite or checker.is_modified(nid):
            # assembler.process(nid)
        assembler.process(nid)

    fs = {}
    with ProcessPoolExecutor(max_workers=num_workers) as executor:

        for nid in neuron_list:
            segments = neuron_getter.get_segments(nid)
            fs[executor.submit(worker_fn_, assembler, nid, segments)] = nid

        p_bar = tqdm(total=len(neuron_list))
        for future in concurrent.futures.as_completed(fs):
            completed = fs[future]
            try:
                future.result()
            except Exception as e:
                print(e)
                print(f'Failed to mesh {completed}')
            checker.mark_done(completed)
            p_bar.update(1)
            p_bar.write(f'Finished {completed}')

    checker.commit()
