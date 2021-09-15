import jsonpickle, json
import jsonpickle.ext.pandas as jsonpickle_pd
jsonpickle_pd.register_handlers()
# from numba import njit

def dump_obj(obj, path):
    path = 'cache/' + path
    jp = jsonpickle.encode(obj, keys = True)
    with open(path, 'w') as f:
        f.write(jp)
        print("Dumped %s" % path)

def restore_obj(path, verbose=True):
    path = 'cache/' + path
    try:
        with open(path) as f:
            s = f.read()
            data = jsonpickle.decode(s, keys = True)
            if verbose:
                print("Loaded %s" % path)
            return data
    except Exception as e:
        print("failed to restore", path, e)
        return None

def is_close(n1, n2):
    return abs(n1-n2) <= 0.0001

def dump_cache(data, name):
    if name.endswith('.json'):
        return self.dump_json(data, name)
    elif name.endswith('.df'):
        return self.dump_feather(data, name)
    assert(False)

def load_json(name):
    return json.load(open('cache/%s' % name))
    print("%s loaded" % name)

def dump_json(data, name):
    json.dump(data, open('cache/%s' % name, 'w'))
    print("%s cached" % name)

def dump_feather(df, name):
    feather.write_dataframe(df, 'cache/%s' % name)
    print("%s cached" % name)

# @njit does not pass but fast enough
def filter_by_numpy(array, predicates):
    index = None
    # print(predicates)
    for dimension, preds in enumerate(predicates):
        d_idx = None
        for pred in preds:
            idx = (array[:, dimension] >= pred[0]) & (array[:, dimension] <= pred[1])
            if d_idx is None:
                d_idx = idx
            else:
                d_idx |= idx
        if index is None:
            index = d_idx
        else:
            index &= d_idx
    return index

