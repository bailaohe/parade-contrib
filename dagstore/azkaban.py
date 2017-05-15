# -*- coding:utf-8 -*-

import requests
from parade.utils.log import logger
from parade.dagstore import DAGStore
from parade.core.task import Task


class Noop(Task):
    def __init__(self, name, *deps):
        Task.__init__(self)
        self._name = name
        self._deps = set(list(deps))

    @property
    def name(self):
        return self._name

    @property
    def deps(self):
        return self._deps

    def execute_internal(self, context, **kwargs):
        return None


class AzkabanDAGStore(DAGStore):
    def __init__(self, context):
        DAGStore.__init__(self, context)
        conf = context.conf['dagstore']
        self.host = conf['azkaban.host']
        self.username = conf['azkaban.username']
        self.password = conf['azkaban.password']
        self.notify_mails = conf['azkaban.notifymail']
        self.project = conf['azkaban.project']
        self.cmd = conf['azkaban.cmd']

    def login(self):
        import requests
        login_params = {
            "action": "login",
            "username": self.username,
            "password": self.password
        }
        try:
            r = requests.post(self.host, params=login_params)
            if r.status_code == 200:
                resp = r.json()
                if 'status' in resp and resp['status'] == 'success':
                    session_id = resp['session.id']
                    logger.debug('Azkaban session updated: {}'.format(session_id))
                    return session_id
            raise RuntimeError('Azkaban login failed')
        except RuntimeError as e:
            logger.exception(str(e))

    def init_param(self):
        session_id = self.login()
        return {'session.id': session_id}

    def get_project_id(self):
        from bs4 import BeautifulSoup
        params = self.init_param()
        params.update({'project': self.project})

        r = requests.get(self.host + "/manager", params=params)
        if r.status_code != 200:
            raise RuntimeError("Azkaban access failed")
        soup = BeautifulSoup(r.text, "html.parser")

        def script_without_src(tag):
            return tag.name == "script" and not tag.has_attr("src")

        raw = soup.head.find(script_without_src).string
        lines = map(lambda line: line.strip(), raw.strip().splitlines())
        projectIdLine = list(filter(lambda line: line.find("projectId") >= 0, lines))[0]

        import re
        m = re.match("var projectId = (\d+);", projectIdLine)
        project_id = m.group(1)
        return project_id

    def get_schedule_ids(self, flow_name):
        from bs4 import BeautifulSoup
        params = self.init_param()
        r = requests.get(self.host + "/schedule", params=params)
        if r.status_code != 200:
            raise RuntimeError("Azkaban access failed")
        soup = BeautifulSoup(r.text, "html.parser")
        tbl = soup.find(id="scheduledFlowsTbl")
        sched_list = tbl.tbody.find_all("tr")

        def filter_line_with_flow(tag):
            toks = tag.find_all("td")
            sched_flow = toks[1].text.strip()
            sched_proj = toks[2].text.strip()

            return sched_proj == self.project and sched_flow == flow_name

        return list(map(lambda row: row.td.text.strip(), filter(filter_line_with_flow, sched_list)))

    def create_flow(self, flow_name, *tasks):
        tasks = [self.context.task_dict[key] for key in tasks]
        forests = DAGStore.parse_forests(*tasks)
        flow_end = Noop(flow_name, *map(lambda t: t.name, forests))

        import os
        flow_repodir = os.path.join(".", "flows")
        flow_workdir = os.path.join(flow_repodir, flow_name)
        os.makedirs(flow_workdir, exist_ok=True)
        for task in tasks:
            job_file = os.path.join(flow_workdir, task.name + ".job")
            with open(job_file, 'w') as f:
                f.write("type=command\n")
                if len(task.deps) > 0:
                    f.write("dependencies=" + ','.join(task.deps) + "\n")
                f.write("command=" + self.cmd.format(task=task.name))
                f.flush()
        job_file = os.path.join(flow_workdir, flow_name + ".job")
        with open(job_file, 'w') as f:
            f.write("type=command\n")
            f.write("dependencies=" + ','.join(flow_end.deps) + "\n")
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

        params = self.init_param()
        files = {
            'file': (flow_name + '.zip', open(job_zip, 'rb'), 'application/zip', {'Expires': '0'})
        }

        params.update({
            "ajax": "upload",
            "project": self.project
        })

        r = requests.post(self.host + "/manager", files=files, data=params)
        if r.status_code == 200:
            resp = r.json()
            if 'error' in resp:
                raise RuntimeError(resp['error'])

        import shutil
        shutil.rmtree(flow_workdir, ignore_errors=True)
        # os.remove(job_zip)

        logger.info("Azkaban flow {} updated, you can go to {} to check".format(flow_name,
                                                                                self.host + "/manager?project=" + self.project + "&flow=" + flow_name))

    def drop_flow(self, flow_name):
        pass

    def create(self, *tasks, cron=None, dag_key=None):

        self.create_flow(dag_key, *tasks)

        params = self.init_param()
        project_id = self.get_project_id()
        params.update({
            "ajax": "scheduleFlow",
            "projectName": self.project,
            "flow": dag_key,
            "projectId": project_id,
            "scheduleDate": ""
        })
        params.update({
            "is_recurring": "on",
            "period": "1d",
            "scheduleTime": "12,05,AM,+08:00"
        })
        r = requests.get(self.host + "/schedule", params=params)
        if r.status_code == 200:
            resp = r.json()
            if 'error' in resp:
                raise RuntimeError(resp['error'])

    def delete(self, flow_name):
        sched_ids = self.get_schedule_ids(flow_name)
        params = self.init_param()
        params.update({
            "action": "removeSched"
        })
        for sched_id in sched_ids:
            params["scheduleId"] = sched_id
            r = requests.get(self.host + "/schedule", params=params)
            if r.status_code != 200:
                raise RuntimeError("Unschedule failed")
            if r.json()['status'] != 'success':
                raise RuntimeError(r.json()['message'])
