#!/usr/bin/env python3
import json, os, time, threading, urllib.request, urllib.error, hmac
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

HERMES_API = 'http://127.0.0.1:9123'
# v3.0.6 security review：Hermes Bearer key / cron 口令改用已有环境变量注入（源码不落硬编码密钥）
import os as _os
HERMES_KEY = _os.environ.get("STREAM_HERMES_KEY") or _os.environ.get("QL_AGENT_KEY") or ""
CRON_PASSWORD = _os.environ.get("QL_PASSWORD", "")

# ---- v4.0.x 定时任务「信息推送通道」（2026-10-14）----
# 通道是二维的（微信由 Hermes 的 deliver 决定、App 由 ql_task_push.py 决定），但用户只要一个
# 选择 → App 侧只暴露 4 档，本模块把它落成两条真值：
#   both   = 微信 + App   deliver=weixin, App 推
#   wechat = 仅微信        deliver=weixin, App 不推
#   app    = 仅 App        deliver=local,  App 推
#   none   = 不推送（只留档）deliver=local, App 不推
# 🚨 与 Hermes 的耦合点只有 deliver，且**只用它认识的 weixin / local 两个值**：
#    往 deliver 里塞自造 token（如 "app"）会让 Hermes 判「no delivery target resolved」
#    → 记 last_delivery_error + last_status=delivery_failed（App 任务行会显示成失败）。
#    「是否推 App」因此记在 data/cron_channels.json，ql_task_push.py 读同一份文件。
CHANNELS = ('both', 'wechat', 'app', 'none')
CHANNEL_LABELS = {'both': '微信 + App', 'wechat': '仅微信', 'app': '仅 App', 'none': '不推送'}
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHANNEL_FILE = os.path.join(
    os.environ.get('QL_DATA_DIR', os.path.join(os.path.dirname(_BASE_DIR), 'data')),
    'cron_channels.json')


def load_channels():
    try:
        with open(CHANNEL_FILE, encoding='utf-8') as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def save_channels(d):
    """原子写（临时文件 + os.replace）：ql_task_push.py 每分钟读一次，绝不能读到半截文件。"""
    try:
        os.makedirs(os.path.dirname(CHANNEL_FILE), exist_ok=True)
        tmp = CHANNEL_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(d, f, ensure_ascii=False)
        os.replace(tmp, CHANNEL_FILE)
        return True
    except Exception:
        return False


def set_channel(job_id, channel):
    if not job_id or channel not in CHANNELS:
        return False
    d = load_channels()
    d[str(job_id)] = channel
    return save_channels(d)


def drop_channel(job_id):
    d = load_channels()
    if str(job_id) in d:
        d.pop(str(job_id), None)
        return save_channels(d)
    return False


def deliver_for(channel):
    """通道 → Hermes deliver：只用它认识的 weixin / local。"""
    return 'weixin' if channel in ('both', 'wechat') else 'local'


def channel_of(job_id, deliver):
    """列表下发用：显式设过的按记录；没设过的按老口径推断
    （deliver 含 weixin/origin/qqbot = 微信 + App，这是 ql_task_push 的既有判据）。"""
    rec = load_channels().get(str(job_id or ''))
    if rec in CHANNELS:
        return rec
    items = deliver if isinstance(deliver, (list, tuple)) else [deliver]
    toks = set()
    for it in items:
        for x in str(it or '').replace(',', ' ').split():
            toks.add(x.strip().lower())
    if toks & {'weixin', 'origin', 'qqbot'}:
        return 'both'
    if 'all' in toks:
        return 'wechat'
    return 'none'

class Handler(BaseHTTPRequestHandler):
    def _check_auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, 'X-Cron-Password', CRON_PASSWORD)

    def do_GET(self):
        if not self._check_auth():
            self.send_json({'error': 'unauthorized'}, 401)
            return
        if self.path == '/api/cron/tasks':
            try:
                req = urllib.request.Request(f"{HERMES_API}/api/jobs?include_disabled=true", headers={
                    'Authorization': f'Bearer {HERMES_KEY}'
                })
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                    jobs = data.get('jobs', [])
                    transformed = []
                    for job in jobs:
                        transformed.append({
                            'id': job.get('id', ''),
                            'name': job.get('name', '未命名'),
                            'cron': job.get('schedule_display', job.get('schedule', {}).get('display', '')),
                            'prompt': job.get('prompt', ''),
                            'enabled': job.get('enabled', True),
                            'schedule_display': job.get('schedule_display', ''),
                            'schedule': job.get('schedule', {}),
                            'next_run_at': job.get('next_run_at'),
                            'last_run_at': job.get('last_run_at'),
                            'last_status': job.get('last_status'),
                            'skills': job.get('skills', []),
                            'model': job.get('model'),
                            'provider': job.get('provider'),
                            'deliver': job.get('deliver', 'origin'),
                            'channel': channel_of(job.get('id', ''), job.get('deliver', 'origin')),
                            'origin': job.get('origin', {})
                        })
                    self.send_json(transformed)
            except Exception as e:
                self.send_json([])
        elif self.path == '/api/cron/logs':
            try:
                req = urllib.request.Request(f"{HERMES_API}/api/jobs", headers={
                    'Authorization': f'Bearer {HERMES_KEY}'
                })
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                    jobs = data.get('jobs', [])
                    logs = []
                    for job in jobs:
                        if job.get('latest_execution'):
                            exec_data = job['latest_execution']
                            logs.append({
                                'time': exec_data.get('finished_at', exec_data.get('started_at', '')),
                                'name': job.get('name', '未命名'),
                                'status': exec_data.get('status', 'unknown'),
                                'message': f"Execution {exec_data.get('status', 'unknown')}: {job.get('name', '')}"
                            })
                    logs.sort(key=lambda x: x.get('time', ''), reverse=True)
                    self.send_json(logs[:50])
            except Exception as e:
                self.send_json([])
        else:
            self.send_error(404)

    def do_POST(self):
        if not self._check_auth():
            self.send_json({'error': 'unauthorized'}, 401)
            return
        if self.path == '/api/cron/tasks':
            length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body.decode('utf-8'))
            except Exception:
                self.send_error(400)
                return
            try:
                # v4.0.x：优先按「推送通道」落成 deliver（App 只传 channel）；
                # 老客户端/别的调用方只传 deliver 时照旧走白名单，行为零变化。
                channel = data.get('channel') if data.get('channel') in CHANNELS else None
                if channel:
                    deliver = deliver_for(channel)
                else:
                    # deliver 白名单校验，防存储型 XSS
                    deliver = data.get('deliver', 'origin')
                    if deliver not in ('origin', 'weixin', 'local', 'all'):
                        deliver = 'origin'
                payload = {
                    'name': str(data.get('name', '未命名'))[:200],
                    'prompt': str(data.get('prompt', ''))[:4000],
                    'schedule': str(data.get('cron', '0 9 * * *'))[:100],
                    'enabled': bool(data.get('enabled', True)),
                    'deliver': deliver
                }
                req = urllib.request.Request(
                    f"{HERMES_API}/api/jobs",
                    data=json.dumps(payload).encode('utf-8'),
                    headers={
                        'Authorization': f'Bearer {HERMES_KEY}',
                        'Content-Type': 'application/json'
                    },
                    method='POST'
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                    if channel:
                        set_channel(((result or {}).get('job') or {}).get('id'), channel)
                    self.send_json(result)
            except Exception as e:
                self.send_error(500, str(e))
        elif self.path.startswith('/api/cron/tasks/') and self.path.endswith('/run'):
            task_id = self.path.split('/')[-2]
            try:
                req = urllib.request.Request(
                    f"{HERMES_API}/api/jobs/{task_id}/run",
                    headers={'Authorization': f'Bearer {HERMES_KEY}'},
                    method='POST'
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                    self.send_json(result)
            except Exception as e:
                self.send_error(500, str(e))
        else:
            self.send_error(404)

    def do_PATCH(self):
        if not self._check_auth():
            self.send_json({'error': 'unauthorized'}, 401)
            return
        if self.path.startswith('/api/cron/tasks/'):
            task_id = self.path.split('/')[-1]
            length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body.decode('utf-8'))
            except Exception:
                self.send_error(400)
                return
            try:
                # 支持编辑 name/cron/prompt/deliver/enabled（带白名单与长度限制）
                payload = {}
                if 'enabled' in data:
                    payload['enabled'] = bool(data.get('enabled', True))
                if 'name' in data and data.get('name'):
                    payload['name'] = str(data.get('name'))[:200]
                if 'cron' in data and data.get('cron'):
                    payload['schedule'] = str(data.get('cron'))[:100]
                if 'prompt' in data and data.get('prompt'):
                    payload['prompt'] = str(data.get('prompt'))[:4000]
                channel = data.get('channel') if data.get('channel') in CHANNELS else None
                if channel:
                    payload['deliver'] = deliver_for(channel)
                elif 'deliver' in data and data.get('deliver'):
                    d = data.get('deliver')
                    if d in ('origin', 'weixin', 'local', 'all'):
                        payload['deliver'] = d
                req = urllib.request.Request(
                    f"{HERMES_API}/api/jobs/{task_id}",
                    data=json.dumps(payload).encode('utf-8'),
                    headers={
                        'Authorization': f'Bearer {HERMES_KEY}',
                        'Content-Type': 'application/json'
                    },
                    method='PATCH'
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                    if channel:
                        set_channel(task_id, channel)
                    self.send_json(result)
            except Exception as e:
                self.send_error(500, str(e))
        else:
            self.send_error(404)

    def do_DELETE(self):
        if not self._check_auth():
            self.send_json({'error': 'unauthorized'}, 401)
            return
        if self.path.startswith('/api/cron/tasks/'):
            task_id = self.path.split('/')[-1]
            try:
                req = urllib.request.Request(
                    f"{HERMES_API}/api/jobs/{task_id}",
                    headers={'Authorization': f'Bearer {HERMES_KEY}'},
                    method='DELETE'
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                    # 记账要在回包**之前**：放在 send_json 之后会与客户端形成竞态
                    #（实测探针删完立刻回读，记录还在 —— 服务端线程还没走到下一行）。
                    drop_channel(task_id)
                    self.send_json(result)
            except Exception as e:
                self.send_error(500, str(e))
        elif self.path == '/api/cron/logs':
            self.send_json({'ok': True})
        else:
            self.send_error(404)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Cron-Password, X-Auth-Token')
        self.end_headers()

    def send_json(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, format, *args):
        pass

def run_server(port=9125):
    server = HTTPServer(('127.0.0.1', port), Handler)
    print(f"Cron API proxy running on port {port}")
    server.serve_forever()

if __name__ == '__main__':
    run_server()
