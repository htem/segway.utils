import trimesh
import os
import struct
import numpy as np
import sys

class MeshGetter:

    def __init__(
            self,
            mesh_path,
            # neuron_db,
            mesh_hierarchical_size=10000,
            ):
        self.mesh_path = mesh_path
        assert os.path.exists(self.mesh_path), f'mesh_path {mesh_path} does not exist!'
        self.mesh_hierarchical_size = mesh_hierarchical_size
        # self.neuron_db = neuron_db

    def getHierarchicalMeshPath(self, object_id):
        # finds the path to mesh files based on segment numbers
        assert object_id != 0
        level_dirs = []
        num_level = 0
        while object_id > 0:
            level_dirs.append(int(object_id % self.mesh_hierarchical_size))
            object_id = int(object_id / self.mesh_hierarchical_size)
        num_level = len(level_dirs) - 1
        level_dirs = [str(lv) for lv in reversed(level_dirs)]
        return os.path.join(str(num_level), *level_dirs)

    # def getNeuronSubsegments(self, nid, segment_name):
    #     # collects segment numbers for all meshes designated as nid.segmentName_# for any int(#)
    #     num = 0
    #     out_segments = list()
    #     while True:
    #         try:
    #             new_segments = self.neuron_db.get_neuron(
    #                 nid + '.' + segment_name + '_' + str(num)).to_json()['segments']
    #             num += 1
    #             try:
    #                 out_segments.extend(new_segments)
    #             except:
    #                 out_segments = new_segments
    #         except:
    #             break
    #     return out_segments

    def get_mesh(self, segmentNum, raw=False):
        '''opens mesh file from local directory and parses it
        returning a trimesh object.
        Returns `None` if segment does not exist
        '''
        try:
            base = self.mesh_path
            workfile = os.path.join(base,self.getHierarchicalMeshPath(int(segmentNum)))
            totalSize = os.stat(workfile).st_size
            with open(workfile, 'rb') as f:
                num_vertices = struct.unpack('<I', memoryview(f.read(4)))[-1]
                vertices = np.empty((num_vertices, 3))
                for i in range(num_vertices):
                    vertices[i, ] = struct.unpack(
                        '<fff', memoryview(f.read(12)))
                num_triangles = int((totalSize - (num_vertices * 12 + 4)) / 12)
                triangles = np.empty((num_triangles, 3))
                for i in range(num_triangles):
                    triangles[i, ] = struct.unpack(
                        '<III', memoryview(f.read(12)))
            if raw:
                return (vertices, triangles)
            else:
                mesh = trimesh.Trimesh(vertices=vertices, faces=triangles)
                return mesh
        except:
            return None

    def get_meshes(self, seg_num_list, raw=False):
        meshes = []
        iT, iF = 0, 0
        for num in seg_num_list:
            m = self.get_mesh(int(num), raw=raw)
            if m:
                iT += 1
                meshes.append(m)
            else:
                iF += 1
        assert iT
        return meshes
