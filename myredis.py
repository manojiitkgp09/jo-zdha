import os
import redis

LOCAL_REDIS_SERVER_URL = 'redis://127.0.0.1:6379/'


class MyRedis(object):

    r = redis.from_url(os.environ.get("REDIS_URL", LOCAL_REDIS_SERVER_URL))
    columns = ['code', 'name', 'open', 'high', 'low', 'close']

    def create_stock(self, data):
        pipe = self.r.pipeline()
        primary_key = 'code:{}::name:{}'.format(data['code'], data['name'].lower())
        pipe.hmset(primary_key, data)
        pipe.zadd('top_ten', {primary_key: data['code']})  # use data['turnover'] if needed
        pipe.execute()

    def get_top_ten(self):
        top_ten = self.r.zrange('top_ten', 0, 9)
        return self.get_stocks(top_ten)

    def get_stock_by_name(self, name):
        stock_gen = self.r.scan_iter('*name:{}*'.format(name.lower()), 10)
        return self.get_stocks(stock_gen)

    def get_stocks(self, iterator):
        stocks = []
        for item in iterator:
            stock = dict()
            stock_prop_values = self.r.hmget(item, self.columns)
            for ind, prop in enumerate(stock_prop_values):
                stock[self.columns[ind]] = prop.decode("utf-8")
            stocks.append(stock)
        return stocks
