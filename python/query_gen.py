from utils import *
from tqdm import tqdm
import random
import hashlib
import sys, os

class Query(object):
    def __init__(self, predicates, s, c, a, mi, ma, t):
        self.predicates = predicates
        self.target_attr = t

        #ground truth: sum and cnt
        self.sum = s
        self.cnt = c
        self.avg = a
        self.min = mi
        self.max = ma

        s = ''
        for attr, value in predicates.items():
            s += attr + str(predicates[attr])
        self.id = hashlib.sha1(s.encode()).hexdigest()[:10]

    def update(self, df):
        # update the ground truth when predicates is changed
        data = df

        for attr in self.predicates.keys():
            pred = self.predicates[attr]
            data = data[(data[attr] >= pred[0]) & (data[attr] <= pred[1])]

        suma = sum(data[self.target_attr])

        self.sum = suma
        self.cnt = data.shape[0]
        self.avg = data[self.target_attr].mean()

        s = ''
        for attr, value in self.predicates.items():
            s += attr + str(predicates[attr])
        self.id = hashlib.sha1(s.encode()).hexdigest()[:10]

    def __str__(self):
        s = 'Q:'
        for k, v in self.predicates.items():
            s += k + '[' + ','.join([str(i) for i in v]) + '];'
        s += ';cnt: ' + str(self.cnt) + '; sum: ' + str(self.sum) + '; avg: ' + str(self.avg)
        return s

class QueryGenerator(object):
    def __init__(self, min_p=None, max_p=None):
        self.min_p = min_p
        self.max_p = max_p
        if min_p is None:
            self.min_max_p_str = ""
        else:
            attr = list(self.min_p.keys())[0]
            self.min_max_p_str = "-" + attr +"-"+str(self.min_p[attr])+"-"+str(self.max_p[attr])

    def generate_special_query(self, df, attr, sum_attr):
        preds = {}
        data = df
        min_a, max_a = 6597, 6619
        pred = [min_a, max_a]
        data = data[(data[attr] >= pred[0]) & (data[attr] <= pred[1])]
        preds[attr] = pred
        suma = sum(data[sum_attr])
        avg = data[sum_attr].mean() if data.shape[0] > 0 else 0
        q = Query(preds, c = data.shape[0], s = suma, a = avg, t = sum_attr)
        print('Special query:', q)
        return q

    def preprocessing(self, df):
        return df

    def append_min_max(self, df, q):
        preds = q.predicates
        data = df
        sum_attr = q.target_attr
        for attr in preds.keys():
            pred = preds[attr]
            data = data[(data[attr] >= pred[0]) & (data[attr] <= pred[1])]
        suma = sum(data[sum_attr])
        avg = data[sum_attr].mean() if data.shape[0] > 0 else 0
        # print(q.sum, suma, q.avg, avg)
        assert(is_close(suma, q.sum) and is_close(avg, q.avg)) #upgrading env could cause float results diff

        return Query(preds, c = data.shape[0], s = suma, a = avg, mi=min(data[sum_attr]), ma=max(data[sum_attr]), t = sum_attr)


    def generate_queries(self, dataset, pred_attrs, target_attr, n_query):
        ret = []

        pred_attrs = sorted(pred_attrs)

        target_name = ("../files/%s-%sQuery-%s-%s-%s%s.txt" %
                        (dataset.__class__.__name__,
                        self.__class__.__name__,
                        '_'.join(pred_attrs),
                        target_attr, n_query, self.min_max_p_str))
        if os.path.isfile(target_name):
            print(target_name, " already exist.")
            return

        print("Generating queries")
        dataset.load_data()

        df = self.preprocessing(dataset.df)
        for n in tqdm(range(n_query)):
            ret.append(self.generate_query(df, pred_attrs, target_attr))

        with open(target_name, 'w') as f:
            for q in ret:
                f.write(q+"\n")
        print("Saved ", n_query, " queries to ", target_name)

    def generate_query(self, dataset, p_attrs, t_attr):
        raise NotImplementedError

class RangeSum(QueryGenerator):
    def generate_query(self, df, attrs, sum_attr):
        s = sum_attr
        data = df
        try:
            samples = data.sample(n=2)
            for attr in attrs:

                max_a = int(max(samples[attr]))
                min_a = int(min(samples[attr]))

                if min_a == max_a:
                    raise Exception("Too Small")

                pred = [min_a, max_a]
                data = data[(data[attr] >= pred[0]) & (data[attr] <= pred[1])]
                if data.shape[0] < 20:
                    raise Exception("Too Small")
                s += ";"+attr + ":["+str(min_a)+", "+str(max_a)+"]"
            return s
        except Exception as e:
            print(e)
            return self.generate_query(df, attrs, sum_attr)
