from dataset import *
from query_gen import *
from utils import *
import sys, os
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

