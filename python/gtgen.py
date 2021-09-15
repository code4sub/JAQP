import pandas as pd
import sys, os, json, time
from tqdm import tqdm
from dataset import *
from utils import *
# generate ground truth with the first p% of data. using kafka is just too inefficient.

class QGenConfig():
    def __init__(self):
        self.config = {
            'intel':{
                'p_attrs': ['itime'],
                't_attrs': ['light'],
                'ds_cls': IntelWireless,
                'ds': None
            },
            'insta':{
                'p_attrs': ['product_id'],
                't_attrs': ['reordered'],
                'ds_cls': Instacart,
                'ds': None
            },
            'taxi':{
                'p_attrs': ['pickup_time'],
                't_attrs': ['trip_distance'],
                'ds_cls': Taxi,
                'ds': None
            },
            'taxipdt':{
                'p_attrs': ['pickup_datetime'],
                't_attrs': ['trip_distance'],
                'ds_cls': TaxiPDT,
                'ds': None
            },
            'etf':{
                'p_attrs': ['open', 'high', 'low', 'close', 'idate'],
                't_attrs': ['volume'],
                'ds_cls': ETF,
                'ds': None,
            }
        }
    def get_config(self, name):
        if self.config[name]['ds'] is None:
            self.config[name]['ds'] = self.config[name]['ds_cls']() #do not initialize the dataset, we may not need to
        return self.config[name]

class Query:
    def __init__(self, aggr, ranges, qs):
        self.target_attr = aggr
        self.predicates = ranges
        self.qs = qs

    def eval(self, df):
        #this is not a valid idea because of hash difference.
        data = df
        for attr in self.predicates.keys():
            pred = self.predicates[attr]
            data = data[(data[attr] >= pred[0]) & (data[attr] < pred[1])]

        suma = sum(data[self.target_attr])

        self.sum = suma
        self.cnt = data.shape[0]
        self.avg = data[self.target_attr].mean()
        return self.sum, self.cnt, self.avg

    def __str__(self):
        return target_attr+";"+','.join([k + str(v) for k, v in self.predicates.items()])

def parseQuery(qs):
    toks = qs.split(';')
    aggr = None
    ranges = {}
    for i, t in enumerate(toks):
        if i == 0:
            aggr = t
        else:
            a = t.split(':')[0]
            l = float(t.split(':')[1].replace("[", "").replace("]", "").split(',')[0])
            r = float(t.split(':')[1].replace("[", "").replace("]", "").split(',')[1])
            ranges[a] = (l, r)
    return Query(aggr, ranges, qs)


def solveall(config, qpath, topath):
    ds = config['ds']
    ds.load_data()
    df = ds.df
    queries = []
    gts = {}
    with open(qpath) as f:
        for line in f:
            queries.append(parseQuery(line.strip()))

    for p in range(1, 10):
        p *= 0.1
        d = {}
        sample = df[:int(df.shape[0]*p)]
        print(sample.shape)
        for q in tqdm(queries):
            d[q.qs] = q.eval(sample)
        gts[p] = d
    dump_json(gts, topath)

if __name__ == "__main__":
    dsname = sys.argv[1]
    query_path = sys.argv[2]
    saveto = sys.argv[3]
    config = QGenConfig()
    solveall(config.get_config(dsname), query_path, saveto)
