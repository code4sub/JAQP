from dataset import *
from query_gen import *
from utils import *
import sys, os
class QGenConfig():
    def __init__(self):
        self.config = {
            'etf':{
                # 'p_attrs': ['pickup_datetime'],
                'p_attrs': ['open', 'high', 'low', 'close', 'idate'],
                't_attrs': ['volume'],
                'ds_cls': ETF,
                'ds': None,
            },
            'stock':{
                # 'p_attrs': ['pickup_datetime'],
                'p_attrs': ['open', 'high', 'low', 'close', 'idate'],
                't_attrs': ['volume'],
                'ds_cls': Stock,
                'ds': None,
            }
        }
    def get_config(self, name):
        if self.config[name]['ds'] is None:
            self.config[name]['ds'] = self.config[name]['ds_cls']() #do not initialize the dataset, we may not need to
        return self.config[name]

def generate(config, n_queries):
    dataset = config['ds']
    p_attrs = config['p_attrs']
    t_attrs = config['t_attrs']
    RangeSum().generate_queries(dataset, p_attrs, t_attrs[0], n_queries)

if __name__ == "__main__":
    dsname = sys.argv[1]
    nqueries = int(sys.argv[2])
    config = QGenConfig()
    generate(config.get_config(dsname), nqueries)

