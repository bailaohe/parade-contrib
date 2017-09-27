import base64
import datetime
import time
import hashlib
import hmac
import json
from functools import reduce
import pandas as pd

import requests

from parade.connection import Connection, Datasource


class Loghub(Connection):
    CONNECTION_TIME_OUT = 20
    API_VERSION = '0.6.0'
    USER_AGENT = 'log-python-sdk-v-0.6.1'

    def initialize(self, context, conf):
        Connection.initialize(self, context, conf)

    def _getGMT(self):
        return datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')

    def build_url(self):
        uri = self.datasource.uri
        if uri is None:
            uripart = self.datasource.db + "." + self.datasource.host
            if self.datasource.port:
                uripart += ':' + str(self.datasource.port)
            uri = self.datasource.protocol + '://' + uripart
        return uri

    def _getHttpResponse(self, method, url, params, body, headers):  # ensure method, url, body is str
        headers['User-Agent'] = Loghub.USER_AGENT
        r = None
        if method.lower() == 'get':
            r = requests.get(url, params=params, data=body, headers=headers, timeout=Loghub.CONNECTION_TIME_OUT)
        elif method.lower() == 'post':
            r = requests.post(url, params=params, data=body, headers=headers, timeout=Loghub.CONNECTION_TIME_OUT)
        elif method.lower() == 'put':
            r = requests.put(url, params=params, data=body, headers=headers, timeout=Loghub.CONNECTION_TIME_OUT)
        elif method.lower() == 'delete':
            r = requests.delete(url, params=params, data=body, headers=headers, timeout=Loghub.CONNECTION_TIME_OUT)
        return r.status_code, r.content.decode(), r.headers

    def _sendRequest(self, method, url, params, body, headers, respons_body_type='json'):
        (status, respText, respHeader) = self._getHttpResponse(method, url, params, body, headers)
        header = {}
        for key, value in respHeader.items():
            header[key] = value

        request_id = header['x-log-requestid'] if 'x-log-requestid' in header else ''
        exJson = None

        if status == 200:
            if respons_body_type == 'json':
                exJson = json.loads(respText)
                return exJson, header
            else:
                return respText, header

        ex_json = json.loads(respText, request_id)

        if 'errorCode' in ex_json and 'errorMessage' in ex_json:
            raise Exception(ex_json['errorCode'], ex_json['errorMessage'], request_id)
        else:
            ex_json = '. Return json is ' + str(ex_json) if ex_json else '.'
            raise Exception('LogRequestError',
                            'Request is failed. Http code is ' + str(status) + ex_json, request_id)

    @staticmethod
    def canonicalized_log_headers(headers):
        content = ''
        for key in sorted(headers.keys()):
            if key[:6] == 'x-log-' or key[:6] == 'x-acs-':  # x-log- header
                content += key + ':' + str(headers[key]) + "\n"
        return content

    @staticmethod
    def canonicalized_resource(resource, params):
        if params:
            urlString = ''
            for key in sorted(params.keys()):
                value = params[key]
                if not isinstance(value, str):  # int, float, str to unicode
                    value = str(value)
                urlString += key + '=' + value + '&'
            resource = resource + '?' + urlString[:-1]  # strip the last &
        return resource

    @staticmethod
    def hmac_sha1(content, key):
        content = content.encode()
        hashed = hmac.new(key.encode(), content, hashlib.sha1).digest()
        return base64.encodestring(hashed).rstrip()

    @staticmethod
    def get_request_authorization(method, resource, key, params, headers):
        if not key:
            return ''
        content = method + "\n"
        if 'Content-MD5' in headers:
            content += headers['Content-MD5']
        content += '\n'
        if 'Content-Type' in headers:
            content += headers['Content-Type']
        content += "\n"
        content += headers['Date'] + "\n"
        content += Loghub.canonicalized_log_headers(headers)
        content += Loghub.canonicalized_resource(resource, params)
        return Loghub.hmac_sha1(content, key)

    def _send(self, method, body, resource, params, headers, respons_body_type='json'):
        if body:
            headers['Content-Length'] = str(len(body))
            headers['Content-MD5'] = hashlib.md5(body).hexdigest().upper()
        else:
            headers['Content-Length'] = '0'
            headers["x-log-bodyrawsize"] = '0'

        headers['x-log-apiversion'] = Loghub.API_VERSION
        headers['x-log-signaturemethod'] = 'hmac-sha1'
        url = self.build_url()
        headers['Host'] = self.datasource.db + "." + self.datasource.host
        headers['Date'] = self._getGMT()

        signature = Loghub.get_request_authorization(method, resource,
                                                     self.datasource.password, params, headers)
        headers['Authorization'] = "LOG " + self.datasource.user + ':' + signature.decode()
        url = url + resource
        return self._sendRequest(method, url, params, body, headers, respons_body_type)

    def get_histograms(self, logstore, query, topic, from_time, to_time):
        """ Get histograms of requested query from log service.
        Unsuccessful opertaion will cause an LogException.

        :type request: GetHistogramsRequest
        :param request: the GetHistograms request parameters class.

        :return: GetHistogramsResponse

        :raise: LogException
        """
        headers = {}

        params = {'query': query, 'topic': topic, 'type': 'histogram', 'from': from_time, 'to': to_time}
        resource = "/logstores/" + logstore + "/index"
        (resp, header) = self._send("GET", None, resource, params, headers)
        return resp, header

    def get_logs(self, logstore, query, topic, from_time, to_time, offset=None, limit=None, reverse=None):
        """ Get logs from log service.
        Unsuccessful opertaion will cause an LogException.

        :type request: GetLogsRequest
        :param request: the GetLogs request parameters class.

        :return: GetLogsResponse

        :raise: LogException
        """
        headers = {}

        params = {'query': query, 'topic': topic, 'type': 'log', 'from': from_time, 'to': to_time}
        if limit is not None:
            params['line'] = limit
        if offset is not None:
            params['offset'] = offset
        if reverse is not None:
            params['reverse'] = 'true' if reverse else 'false'
        resource = "/logstores/" + logstore
        (resp, header) = self._send("GET", None, resource, params, headers)
        return resp, header

    def load_query(self, query, **kwargs):
        if isinstance(query, str):
            query = dict(map(lambda x: x.split('='), query.split('&')))

        assert isinstance(query, dict)

        # assert 'from' in query, '<from> is required in query'
        # assert 'to' in query, '<to> is required in query'
        assert 'logstore' in query, '<logstore> is required in query'
        assert 'topic' in query, '<topic> is required in query'
        assert 'query' in query, '<query> is required in query'

        logstore = query.get('logstore')
        topic = query.get('topic')
        _query = query.get('query')
        to_time = query.get('to', int(time.time()))
        from_time = query.get('from', to_time - 600)
        limit = query.get('chunk_size', -1)

        (histogram, header) = self.get_histograms(logstore, _query, topic, from_time=from_time, to_time=to_time)

        total_count = reduce(lambda x, y: x + y, map(lambda x: x['count'], histogram), 0)
        limit = total_count if limit < 0 else min(limit, total_count)

        log_lines = []
        log_line = 64
        
        for offset in range(0, limit, log_line):
            resp = None
            for retry_time in range(0, 3):
                resp, header = self.get_logs(logstore, _query, topic, from_time, to_time, offset, log_line, False)
                if resp is not None and header['x-log-progress'] == 'Complete':
                    break
                time.sleep(1)
            if resp is not None:
                log_lines.extend(resp)

        return pd.DataFrame(log_lines)

