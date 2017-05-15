import datetime
import redis

from parade.connection import Connection


class RedisZSetConnection(Connection):
    def _gen_key(self, table, suffix=None, row_key=None):
        if not suffix:
            return table
        suffix2 = None
        if isinstance(table, tuple) and len(table) > 1:
            suffix2 = '-'.join(table[1:])
            table = table[0]
        cache_key = table + '-' + str(row_key) + ':set:' + suffix if row_key else table + ":set:" + suffix
        if suffix2:
            cache_key += ":" + suffix2
        return cache_key

    def store(self, df, table, **kwargs):
        pipe = self.open().pipeline()

        # only store set for one day
        expire_time = datetime.datetime.combine(datetime.date.today() + datetime.timedelta(days=1), datetime.time())

        assert isinstance(df, dict), 'not supported data type'

        for suffix, subset in df.items():
            cache_key = self._gen_key(table, suffix=suffix)

            assert isinstance(subset, dict), 'not supported data type'
            for value, score in subset.items():
                pipe.zincrby(cache_key, value, score)

            pipe.expireat(cache_key, expire_time)

        pipe.execute()

    def open(self):
        host = self.datasource.host if self.datasource.host else 'localhost'
        port = self.datasource.port if self.datasource.port else 6379
        db = self.datasource.db if self.datasource.db else 0

        conn = redis.StrictRedis(host=host, port=port, db=db,
                                 password=self.datasource.password) if self.datasource.password else redis.StrictRedis(
                host=host, port=port, db=db)

        return conn
