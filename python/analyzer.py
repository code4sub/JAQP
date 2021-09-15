import math
from heapq import *
# from utils import *
from collections import defaultdict
import numpy as np
import os, sys, json
import pandas as pd
# from dataset import *
# import matplotlib.pyplot as plt

class Analyzer(object):
    def __init__(self, path, ground_truth, base=None):
        self.ground_truth = ground_truth
        self.basename = base
        self.baselines = None
        self.load_results(path, ground_truth)

    def load_results(self, path, ground_truth):
        ret = {}
        for subdir, _, files in os.walk(path):
            for file in files:
                result = pd.read_csv(os.path.join(subdir, file))
                if ground_truth.lower() in file.lower():
                    self.ground_truth = result
                    print("Loaded ground truth: %s, %s rows" % (file, result.shape[0]))
                else:
                    if self.basename is not None:
                        if self.basename in file.lower():
                            ret[file] = result
                            print("Loaded baseline: %s, %s rows"  %(file, result.shape[0]))
                    else:
                        ret[file] = result
                        print("Loaded baseline: %s, %s rows"  %(file, result.shape[0]))
        self.baselines = ret

    def metric(self):
        raise NotImplemented

    def analyze(self,  verbose = True, percentiles = [50]):
        cnt_results = defaultdict(list)
        sum_results = defaultdict(list)
        avg_results = defaultdict(list)
        ground_truth = self.ground_truth
        worst = defaultdict(lambda: [(float('-inf'), None)])
        hashcol = 'query'
        print(list(ground_truth.columns))
        if 'qhash' in list(ground_truth.columns):
            hashcol = 'qhash'

        print("%s " % (self.__class__.__name__))
        for baseline, perf in self.baselines.items():
            for idx, row in perf.iterrows():
                gt = ground_truth[(ground_truth[hashcol] == row[hashcol])].iloc[0]
                # print(gt.shape, type(gt.iloc[0]['cnt']))
                if gt[hashcol] != row[hashcol]:
                    print(gt[hashcol], row[hashcol], baseline)
                    return
                # if row['query'] == '26db9afe68':
                #     print(row)
                #     print(gt)
                #     print(self.metric(gt['sum'], row['sum']))
                cnt_results[baseline].append(self.metric(gt['cnt'], row['cnt']))
                sum_results[baseline].append(self.metric(gt['sum'], row['sum']))
                avg_results[baseline].append(self.metric(gt['avg'], row['avg']))

                heappush(worst[baseline], (cnt_results[baseline][-1], row['query']))
                while len(worst[baseline]) > 10:
                    heappop(worst[baseline])

            if verbose is True:
                for percentile in percentiles:
                    print("\tP%s \t%s\t%s %s %s" % (percentile, baseline,
                                    "Cnt: %.5f" % (np.percentile(cnt_results[baseline], percentile)),
                                    "Sum: %.5f" % (np.percentile(sum_results[baseline], percentile)),
                                    "Avg: %.5f" % (np.percentile(avg_results[baseline], percentile)))
                                    )

        # print("Top 10 worst perf queries")
        # for k,vs in worst.items():
        #     print("\t", k) #.split('_')[0].split('.')[1])
        #     for v in vs:
        #         print("\t\t", v)

        return cnt_results, sum_results, avg_results

    def analyze_and_save(self, name):
        c, s, m = self.analyze()
        dump_obj([c, s, m], name)

class RelativeError(Analyzer):
    def metric(self, gt, result):
        if gt == 0: return abs(result)
        return abs(float(gt - result)/float(gt))

if __name__ == '__main__':
    path = sys.argv[1]
    gt = sys.argv[2]
    base = None
    if len(sys.argv) > 3:
        base = sys.argv[3]

    re_analyzer = RelativeError(path, gt, base)

    analyzed_result = re_analyzer.analyze(verbose=True, percentiles = [50, 95])
    # analyzed_result = re_analyzer.analyze(verbose=True, percentile = 50)
    # analyzed_result = re_analyzer.analyze(verbose=True, percentile = 95)

