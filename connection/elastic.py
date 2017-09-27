# -*- coding:utf-8 -*-
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pandas as pd
from parade.connection import Datasource, Connection


class ElasticConnection(Connection):
    def initialize(self, context, conf):
        Connection.initialize(self, context, conf)
        assert self.datasource.host is not None, 'host of connection is required'
        assert self.datasource.port is not None, 'port of connection is required'
        assert self.datasource.db is not None, 'db of connection is required'
        assert self.datasource.driver is not None and self.datasource.driver == 'elastic', 'driver mismatch'

    def open(self):
        uri = self.datasource.uri
        if uri is None:
            authen = None
            uripart = self.datasource.host + ':' + str(self.datasource.port) + '/' + self.datasource.db
            if self.datasource.user is not None:
                authen = self.datasource.user
            if authen is not None and self.datasource.password is not None:
                authen += ':' + self.datasource.password
            if authen is not None:
                uripart = authen + '@' + uripart
            protocol = 'http'
            if self.datasource.protocol is not None:
                protocol = self.datasource.protocol
            uri = protocol + '://' + uripart

        return Elasticsearch(uri)

    def load(self, table, **kwargs):
        raise NotImplementedError

    def load_query(self, query, **kwargs):
        raise NotImplementedError

    def store(self, df, table, **kwargs):
        if isinstance(df, pd.DataFrame):
            es = self.open()

            records = df.to_dict(orient='records')

            if df.index.name:
                actions = [{
                    "_index": self.datasource.db,
                    "_type": table,
                    "_id": record[df.index.name],
                    "_source": record
                } for record in records]
            else:
                actions = [{
                    "_index": self.datasource.db,
                    "_type": table,
                    "_source": record
                } for record in records]

            if len(actions) > 0:
                helpers.bulk(es, actions)
