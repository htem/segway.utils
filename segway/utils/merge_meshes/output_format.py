
import trimesh

def trimesh_to_blender_obj(trimesh_obj, fname):
    fname += '.obj'
    faces = trimesh_obj.faces
    vertices = trimesh_obj.vertices
    obj = open(fname, 'w')
    for item in vertices:
        obj.write(f"v {item[2]*1000000} {item[1]*1000000} {item[0]*1000000}\n")
    for item in faces:
        obj.write(f"f {item[2]+1} {item[1]+1} {item[0]+1}\n")
    obj.close()

def trimesh_to_ply(trimesh_obj, fname):
    fname += '.ply'
    for item in trimesh_obj.vertices:
        item[0] *= 1000000
        item[1] *= 1000000
        item[2] *= 1000000
        item[2], item[1] = item[1], item[2]
    binary = trimesh.exchange.ply.export_ply(trimesh_obj)
    with open(fname, 'wb') as f:
        f.write(binary)

def trimesh_to_precomputed(trimesh_obj, fname):
    triangles = np.array(trimesh_obj.faces).flatten()
    vertices = np.array(trimesh_obj.vertices)
    num_vertices = len(vertices)
    b_array = bytearray(4 + 4 * 3 * len(vertices) + 4 * len(triangles))
    struct.pack_into('<I', b_array, 0, num_vertices)
    struct.pack_into('<' + 'f' * len(vertices.flatten()),
                     b_array,
                     4,
                     *vertices.flatten())
    struct.pack_into('<' + 'I' * len(triangles),
                     b_array,
                     4 + 4 * 3 * num_vertices,
                     *triangles)
    with open(fname, 'wb') as f:
        f.write(b_array)