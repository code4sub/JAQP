import sys
from utils import *

class Query:
    def __init__(self, aggr, ranges, dataset):
        str_p = ''
        for k,v in ranges.items():
            if str_p != '':
                str_p += " AND "
            str_p += k + ">=" + str(v[0]) + " AND " + k + "<=" + str(v[1])
        sql = "SELECT SUM(" + aggr + ") FROM " + dataset + " WHERE " + str_p + ";"
        print(sql)

def parseQuery(qs, dataset):
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
    return Query(aggr, ranges, dataset)


def convert(qpath, dataset):
    queries = []
    with open(qpath) as f:
        for line in f:
            queries.append(parseQuery(line.strip(), dataset))

if __name__ == "__main__":
    dataset = sys.argv[1]
    query_path = sys.argv[2]

    convert(query_path, dataset)
