import redis

from parade.connection import Connection
import pandas as pd


class RedisCounterConnection(Connection):
    def _gen_key(self, table, suffix=None, key=None):
        if not suffix:
            return table
        suffix2 = None
        if isinstance(table, tuple) and len(table) > 1:
            suffix2 = '-'.join(table[1:])
            table = table[0]
        cache_key = table + '-' + str(key) + ':counter:' + suffix if key else table + ":counter:" + suffix
        if suffix2:
            cache_key += ":" + suffix2
        return cache_key

    def store(self, df, table, **kwargs):
        pkey = kwargs.get('pkey', None)
        pkey = pkey if isinstance(pkey, tuple) else (pkey,)

        pipe = self.open().pipeline()

        if isinstance(df, pd.DataFrame):
            assert pkey, 'pkey must be set to store dataframe'
            for row_idx, row in df.iterrows():
                row_key = '-'.join([str(row[x]) for x in pkey])
                row_dict = row.drop(list(pkey)).to_dict()

                for key, val in row_dict.items():
                    cache_key = self._gen_key(table, key, row_key)
                    pipe.set(cache_key, val)

        elif isinstance(df, dict):
            for key, val in df.items():
                pipe.set(self._gen_key(table, key), val)

        elif type(df) in (int, float, str):
            pipe.set(table, df)

        else:
            raise TypeError('not supported data type')

        pipe.execute()

    def open(self):
        host = self.datasource.host if self.datasource.host else 'localhost'
        port = self.datasource.port if self.datasource.port else 6379
        db = self.datasource.db if self.datasource.db else 0

        conn = redis.StrictRedis(host=host, port=port, db=db,
                                 password=self.datasource.password) if self.datasource.password else redis.StrictRedis(
                host=host, port=port, db=db)

        return conn
