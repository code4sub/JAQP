#!/usr/bin/env python3
import json, sys, os
import pandas as pd
import numpy as np

def get_percentile_re(hashq, baseline, gtp, qtype, percentile):
    qtypeid={'cnt':1, 'sum':0, 'avg': 2}
    re = []
    for i in range(baseline.shape[0]):
        r = baseline.iloc[i]
        q = hashq[r['qhash']]
        gtv = gtp[q][qtypeid[qtype]]
        re.append(abs(r[qtype] - gtv)/gtv)
    return np.percentile(re, percentile)

def getre(p, g, ps=[25, 50, 95], qtype="sum"):
    hashq = {}
    for i in range(p.shape[0]):
        r = p.iloc[i]
        hashq[r['qhash']] = r['query']
    for per in ps:
        print("P", per, "PASS:", get_percentile_re(hashq, p, g, qtype, per))

base = pd.read_csv(sys.argv[1])
gt = json.load(open(sys.argv[2]))
key = sys.argv[3]

getre(base, gt[key])
