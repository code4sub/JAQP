import pandas as pd
from datetime import datetime

class Dataset(object):
    def __init__(self):
        self.df = None
        # self.load_data() #do not load data yet, we might not need it.
    def load_data(self):
        raise NotImplementedError


class ETF(Dataset):
    def load_data(self, attrs=['open', 'high', 'low', 'close', 'idate', 'volume']):
        print("\nLoading ETF dataset...")
        etf = pd.read_csv('../data/etf.csv')
        self.df = etf[attrs]
        print(self.df.describe())

class Stock(Dataset):
    def load_data(self, attrs=['open', 'high', 'low', 'close', 'idate', 'volume']):
        print("\nLoading Stock2010 dataset...")
        df = pd.read_csv('../data/stock2010.csv')
        self.df = df[attrs]
        print(self.df.describe())

class TaxiKD(Dataset):
    def taxi_date_to_int(self, strdate):
        return int(strdate.split(' ')[0].replace('-', ''))

    def taxi_time_to_int(self, strdate):
        return int(strdate.split(' ')[1].replace(':', ''))

    def load_data(self, attrs=['pickup_date', 'pickup_time', 'dropoff_date',
                                'dropoff_time', 'trip_distance', 'PULocationID','DOLocationID']):
        path = '../data/taxi6-fixed.csv'
        # path = '../data/taxi.csv'
        self.df = pd.read_csv(path)


class Taxi1D(TaxiKD):
    pass

class Taxi2D(TaxiKD):
    pass

class Taxi3D(TaxiKD):
    pass

class Taxi4D(TaxiKD):
    pass

class Taxi5D(TaxiKD):
    pass

class Taxi6D(TaxiKD):
    pass

class Taxi(Dataset):
    def taxi_date_to_epoch(self, strdate):
        return int(int(datetime.strptime(strdate, '%Y-%m-%d %H:%M:%S').strftime('%s')))

    def load_data(self, attrs=['pickup_time', 'trip_distance']):
        print("\nLoading NYC Taxi dataset...")
        if self.df is not None:
            print("Dataset already loaded", self.df.shape)
            return
        taxi = pd.read_csv('../data/taxi6-fixed.csv')
        self.df = taxi[attrs]
        print(self.df.describe())

class TaxiPDT(Dataset):
    def load_data(self, attrs=['pickup_datetime', 'trip_distance']):
        print("\nLoading NYC Taxi PDT dataset...")
        if self.df is not None:
            print("Dataset already loaded", self.df.shape)
            return
        taxi = pd.read_csv('../data/taxi_pickupdatetime_sorted.csv')
        self.df = taxi[attrs]
        print(self.df.describe())

class Instacart(Dataset):
    # https://www.instacart.com/datasets/grocery-shopping-2017
    def load_data(self):
        if self.df is not None:
            print("Dataset already loaded", self.df.shape)
            return
        print("\nLoading Instacart dataset...")
        df = pd.read_csv('../data/instacart_2017_05_01/order_products__train.csv')
        df = df[df.isnull().any(axis=1) == False]
        self.df = df
        # print(df.describe())

class Data(Dataset):
    # https://www.instacart.com/datasets/grocery-shopping-2017
    def load_data(self):
        print("loading dataset...")
        df = pd.read_csv('../data/Data.csv')
        df = df[df.isnull().any(axis=1) == False]
        self.df = df
        # print(df.describe())

class Data1M(Dataset):
    # https://www.instacart.com/datasets/grocery-shopping-2017
    def load_data(self):
        if self.df is not None:
            print("Dataset already loaded", self.df.shape)
            return
        print("loading dataset...")
        df = pd.read_csv('../data/Data1M.csv')
        df = df[df.isnull().any(axis=1) == False]
        self.df = df
        print(df.describe())

class NYAB(Dataset):
    #https://www.kaggle.com/dgomonov/new-york-city-airbnb-open-data
    def load_data(self):
        print("loading dataset...")
        ab = pd.read_csv('../data/AB_NYC_2019.csv')
        ab = ab[ab.isnull().any(axis=1) == False]
        ab['latitude'] = ab['latitude']*100000
        ab['latitude'] = ab['latitude'].astype(int)
        ab['longitude'] = ab['longitude']*100000
        ab['longitude'] = ab['longitude'].astype(int)
        self.df = ab
        print(ab.describe())

class MexBorder(Dataset):
    #https://www.kaggle.com/akhilv11/border-crossing-entry-data
    def date_to_epoch(self, strdate):
        strdate = strdate.split(' ')[0]
        return int(datetime.strptime(strdate, '%m/%d/%Y').strftime('%s'))/1000000

    def load_data(self):
        ds = pd.read_csv('../data/Border_Crossing_Entry_Data.csv')
        ds = ds[ds.isnull().any(axis=1) == False]
        ds['idate'] = ds['Date'].apply(self.date_to_epoch)
        ds['port'] = ds['Port Code']
        self.df = ds
        print(ds.describe())

class IntelWireless(Dataset):
    def load_data(self):
        if self.df is not None:
            print("Dataset already loaded", self.df.shape)
            return
        print("\nLoading IntelWireless dataset...")
        intel = pd.read_csv('../data/intel-w-header.csv')
        self.df = intel
        print("Loaded %s tuples" % self.df.shape[0])

class Shopping(Dataset):
    #https://www.kaggle.com/akhilv11/border-crossing-entry-data
    def load_data(self):
        print("\nLoading Shopping dataset...")
        df = pd.read_csv('../data/shopping.csv')
        df['DATE'] = df['TIME'].str.split(' ', expand=True)[0]
        df['TIME'] = df['TIME'].str.split(' ', expand=True)[1]
        df['AMOUNT'] = df['COUNT']
        df['idate'] = 1*(pd.to_numeric(df['DATE'].str.replace('-', '')) - 20170100)
        df['itime'] = 1*(pd.to_numeric(df['TIME'].str.replace(':', '')))
        self.df = df
        print("Loaded %s tuples" % self.df.shape[0])
