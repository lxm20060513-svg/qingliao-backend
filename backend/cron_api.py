#!/usr/bin/env python3
import json, os, time, threading, urllib.request, urllib.error, hmac
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

HERMES_API = 'http://127.0.0.1:9123'
# v3.0.6 security review：Hermes Bearer key / cron 口令改用已有环境变量注入（源码不落硬编码密钥）
import os as _os
HERMES_KEY = _os.environ.get("STREAM_HERMES_KEY") or _os.environ.get("QL_AGENT_KEY") or ""
CRON_PASSWORD = _os.environ.get("QL_PASSWORD", "")

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
                if 'deliver' in data and data.get('deliver'):
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
