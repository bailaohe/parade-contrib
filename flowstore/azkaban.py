# -*- coding:utf-8 -*-

import os
from bs4 import BeautifulSoup
import requests
from parade.flowstore import FlowStore
from parade.utils.log import logger
from parade.core.task import Task, Flow


class AzkabanDAGStore(FlowStore):
    host = None
    username = None
    password = None
    notify_mails = None
    project = None
    cmd = None

    def initialize(self, context, conf):
        FlowStore.initialize(self, context, conf)
        self.host = self.conf['host']
        self.username = self.conf['username']
        self.password = self.conf['password']
        self.notify_mails = self.conf['notifymail']
        self.project = self.conf['project']
        self.cmd = self.conf['cmd']

    def _call_api(self, entry, cmd, require_login=True, method='GET', attachment=None, cmd_key='ajax', **params):
        _params = self._init_param(require_login)
        _params.update({cmd_key: cmd})
        _params.update(params)

        url = self.host
        if len(entry) > 0:
            url += '/' + entry

        if method == 'GET':
            r = requests.get(url, params=_params)
        else:
            if attachment:
                r = requests.post(url, files=attachment, data=_params)
            else:
                r = requests.post(url, params=_params)

        if r.status_code != 200:
            raise RuntimeError('Azkaban API execution failed')
        try:
            resp = r.json()
        except Exception as e:
            raise RuntimeError(r.text)
        if 'error' in resp:
            raise RuntimeError(resp['error'])

        return resp

    def _load_html(self, entry, require_login=True, **params):
        _params = self._init_param(require_login)
        _params.update(params)

        url = self.host
        if len(entry) > 0:
            url += '/' + entry

        r = requests.get(url, params=_params)

        if r.status_code != 200:
            raise RuntimeError('Azkaban access failed')
        return r.text

    def load(self, flow):
        resp = self._call_api('manager', 'fetchflowgraph', flow=flow, project=self.project)
        nodes = resp['nodes']
        tasks = [n['id'] for n in nodes if n['id'] != flow]
        deps = dict([(n['id'], set(n['in'])) for n in nodes if 'in' in n and n['id'] != flow])
        return Flow(flow, tasks, deps)

    def list(self):
        resp = self._call_api('manager', 'fetchprojectflows', project=self.project)
        return list(map(lambda x: x['flowId'], resp['flows']))

    def create(self, flow, *tasks, deps=None, **kwargs):
        self._create_flow(flow, *tasks, deps=deps)
        self._schedule_period(flow, '12,10,AM,+08:00')

    def _schedule_period(self, flow, sched_time):
        self._call_api('schedule', 'scheduleFlow', projectName=self.project, flow=flow, projectId=self._project_id,
                       is_recurring='on', period='1d', scheduleDate='', scheduleTime=sched_time)

    def _schedule_cron(self, flow, cron):
        self._call_api('schedule', 'scheduleCronFlow', method='POST', projectName=self.project, flow=flow,
                       cronExpression=cron)

    def delete(self, flow_name):
        sched_id = self._get_schedule_id(flow_name)
        if sched_id:
            self._call_api('schedule', 'removeSched', method='POST', cmd_key='action', scheduleId=sched_id)

    def _login(self):
        resp = self._call_api('', 'login', require_login=False, method='POST', cmd_key='action', username=self.username,
                              password=self.password)
        return resp['session.id']

    def _init_param(self, require_login=True):
        if require_login:
            session_id = self._login()
            return {'session.id': session_id}
        return {}

    @property
    def _project_id(self):
        resp = self._load_html('manager', project=self.project)
        soup = BeautifulSoup(resp, "html.parser")

        def script_without_src(tag):
            return tag.name == "script" and not tag.has_attr("src")

        raw = soup.head.find(script_without_src).string
        lines = map(lambda line: line.strip(), raw.strip().splitlines())
        projectIdLine = list(filter(lambda line: line.find("projectId") >= 0, lines))[0]

        import re
        m = re.match("var projectId = (\d+);", projectIdLine)
        project_id = m.group(1)
        return project_id

    def _get_schedule_id(self, flow_name):
        resp = self._call_api('schedule', 'fetchSchedule', projectId=self._project_id, flowId=flow_name)
        try:
            return resp['schedule']['scheduleId']
        except:
            return None

    def _create_flow(self, flow_name, *tasks, deps):
        flow = Flow(flow_name, tasks, deps)

        flow_repodir = os.path.join(self.context.workdir, "flows")
        flow_workdir = os.path.join(flow_repodir, flow_name)
        os.makedirs(flow_workdir, exist_ok=True)
        for task in tasks:
            job_file = os.path.join(flow_workdir, task + ".job")
            with open(job_file, 'w') as f:
                f.write("type=command\n")
                if task in deps and len(deps[task]) > 0:
                    f.write("dependencies=" + ','.join(deps[task]) + "\n")
                f.write("command=" + self.cmd.format(task=task))
                f.flush()

        if len(flow.forest) > 1:
            job_file = os.path.join(flow_workdir, flow_name + ".job")
            with open(job_file, 'w') as f:
                f.write("type=command\n")
                f.write("dependencies=" + ','.join(flow.forest) + "\n")
                f.write("command=echo flow done\n")
                f.write("failure.emails=" + self.notify_mails)
                f.flush()
        logger.debug("Job files generation succeed")

        import zipfile
        def zipdir(path, ziph):
            # ziph is zipfile handle
            for root, dirs, files in os.walk(path):
                for file in files:
                    ziph.write(os.path.join(root, file))

        job_zip = os.path.join(flow_repodir, flow_name + ".zip")
        zipf = zipfile.ZipFile(job_zip, 'w', zipfile.ZIP_DEFLATED)
        zipdir(flow_workdir, zipf)
        zipf.close()

        logger.debug("Job files zipped into {}".format(job_zip))

        files = {
            'file': (flow_name + '.zip', open(job_zip, 'rb'), 'application/zip', {'Expires': '0'})
        }
        self._call_api('manager', 'upload', require_login=True, method='POST', attachment=files, project=self.project)

        import shutil
        shutil.rmtree(flow_workdir, ignore_errors=True)
        os.remove(job_zip)

        logger.info("Azkaban flow {} updated, you can go to {} to check".format(flow_name,
                                                                                self.host + "/manager?project=" + self.project + "&flow=" + flow_name))
