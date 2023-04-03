
import sqlite3
import pymongo
from pymongo import MongoClient
import bson
import pickle

def check_keys(
        mongo_host,
        mongo_db_name,
        mongo_collection_name,
        ):

    client = MongoClient(mongo_host) 
    database = client[mongo_db_name]
    collection = database[mongo_collection_name] 
    # fetch one to get keys
    item = collection.find_one()
    keys = ', '.join(item.keys())
    return keys

def add_reverse_segment_map(
        in_file,
        in_collection='neurons',
        out_collection='segments',
        ):

    con = sqlite3.connect(in_file)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    schema = 'segment_id, neuron_name'
    cur.execute(f"CREATE TABLE IF NOT EXISTS {out_collection}({schema})")
    cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS segment_id on {out_collection} (segment_id)')

    for item in cur.execute(f"SELECT neuron_name, segments FROM {in_collection}").fetchall():
        kv_list = []
        nid = item["neuron_name"]
        segments = pickle.loads(item['segments'])
        # for sid in segments.split(','):
        for sid in segments:
            kv_list.append((int(sid),nid))
        # cur.executemany(f'INSERT INTO {out_collection}(segment_id, neuron_name) VALUES(?, ?) ON CONFLICT DO NOTHING', kv_list)
        cur.executemany(f'REPLACE INTO {out_collection}(segment_id, neuron_name) VALUES(?, ?)', kv_list)

    con.commit()
    con.close()

def export_neurondb(
        mongo_host,
        mongo_db_name,
        mongo_collection_name,
        out_file,
        out_table=None,

        data_keys=None,
        data_keys_missing_ok=None,

        indices=None,
        unique_indices=None,

        input_list=None,
        input_list_key=None,
        ):

    assert type(data_keys) == list, "data_keys must be provided as a list"

    if out_table is None:
        out_table = mongo_collection_name
    if indices is None:
        indices = []
    if unique_indices is None:
        unique_indices = []

    if input_list is not None:
        assert type(input_list) == list
        assert input_list_key is not None, "if input_list is provided, input_list_key must also be provided"

    client = MongoClient(mongo_host) 
    database = client[mongo_db_name]
    collection = database[mongo_collection_name]
    con = sqlite3.connect(out_file)
    cur = con.cursor()
    schema = ', '.join(data_keys)
    placeholders_str = ', '.join(['?' for k in data_keys])

    print(f'schema: {schema}')

    cur.execute(f"CREATE TABLE IF NOT EXISTS {out_table}({schema})")

    if len(indices):
        for index in indices:
            unique_str = ''
            if index in unique_indices:
                unique_str = 'UNIQUE'
            cur.execute(f'CREATE {unique_str} INDEX IF NOT EXISTS {index} on {out_table} ({index})')

    def convert_to_str(value):
        if type(value) in [str, int, bool]:
            return value
        elif type(value) is bson.int64.Int64:
            return int(value)
        else:
            return pickle.dumps(value)

    def add_item(item):
        data = []
        for k in data_keys:
            if k not in item:
                if k in data_keys_missing_ok:
                    data.append('')
                    continue
                else:
                    print(item)
                    raise RuntimeError(f'Missing data {k} in {e}')
            data.append(convert_to_str(item[k]))
        cur.execute(f'INSERT INTO {out_table} VALUES({placeholders_str})', data)

    if input_list is not None:
        for e in input_list:
            item = collection.find_one({input_list_key: e})
            if item is None:
                raise RuntimeError(f'{e} does not exist in db')
            add_item(item)
            # handle children objects
            for child in collection.find({'parent_segment': e}):
                add_item(child)
    else:
        find_ret = collection.find({})
        for item in find_ret:
            add_item(item)

    con.commit()
    con.close()

