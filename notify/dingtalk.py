from parade.notify import Notifier
import requests


class DingTalk(Notifier):
    API_GATEWAY = 'https://oapi.dingtalk.com/robot/send?access_token={target}'
    TEMPLATE_SUCCESS = """#### {title}\n
> 任务：{task}
    """

    TEMPLATE_FAIL = """#### {title}\n
> 任务：{task}\n
> 原因：{reason}
    """

    target = None
    attachment = None

    def initialize(self, context, conf):
        Notifier.initialize(self, context, conf)
        self.target = self.conf['target']
        self.attachment = self.conf['attachment'] if self.conf.has('attachment') else None

    @staticmethod
    def send_notify(target, title, content):
        message = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": content,
            },
            "at": {
                "isAtAll": True
            }
        }
        r = requests.post(DingTalk.API_GATEWAY.format(target=target), json=message)
        if r.status_code != 200:
            raise RuntimeError('Notify server error')
        else:
            resp = r.json()
            if resp['errcode'] != 0:
                raise RuntimeError(r.json()['errmsg'])

    def notify_error(self, task, reason, **kwargs):
        title = 'Parade任务执行失败'
        content = self.TEMPLATE_FAIL.format(title=title, task=task, reason=reason)
        self.send_notify(self.target, title, content)

    def notify_success(self, task, **kwargs):
        title = 'Parade任务执行成功'
        content = self.TEMPLATE_SUCCESS.format(title=title, task=task)
        self.send_notify(self.target, title, content)

