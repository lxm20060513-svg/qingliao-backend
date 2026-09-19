import json, base64, subprocess, os, sys
from http.server import BaseHTTPRequestHandler, HTTPServer

# 路由器面板后端：状态查询 + Clash 快捷启停
# 连接方式：宿主 docker exec hermes 容器内 paramiko 连路由器（NAS 宿主无 paramiko）
# 凭据：优先 secrets_api 存储的 type=router 条目，缺省读 QL_ROUTER_* 环境变量

ROUTER_DEFAULT = {'host': os.environ.get("QL_ROUTER_HOST", ""), 'port': 22, 'username': os.environ.get("QL_ROUTER_USER", "root"), 'password': os.environ.get("QL_ROUTER_PASS", "")}
CONTAINER = os.environ.get('QL_HERMES_CONTAINER', 'hermes-container')
# hermes 容器内的解释器：qingliao 容器里 sys.executable 指向的路径在目标容器不存在，须显式指定
PY = os.environ.get('QL_HERMES_PYTHON', sys.executable)
# paramiko 所在的 site-packages（宿主没装时用 QL_PARAMIKO_PATH 指过去；留空则不注入 PYTHONPATH）
PARAMIKO_PATH = os.environ.get('QL_PARAMIKO_PATH', '')

def _router_cred():
    try:
        import secrets_api
        it = secrets_api.find_router_cred()
        if it:
            return {
                'host': it.get('address') or ROUTER_DEFAULT['host'],
                'port': 22,
                'username': it.get('username') or 'root',
                'password': it.get('password') or '',
            }
    except Exception:
        pass
    return ROUTER_DEFAULT

# 在容器内执行的 paramiko 脚本模板（命令列表 + 可选 get_pty）
_SCRIPT_TMPL = r'''
import paramiko, sys, base64, json
cmds = json.loads(base64.b64decode(sys.argv[1]).decode())
pty = sys.argv[2] == "1"
c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect({host!r}, port={port}, username={user!r}, password={pw!r},
          timeout=10, look_for_keys=False, allow_agent=False)
out = []
for cmd in cmds:
    try:
        _, so, se = c.exec_command(cmd, get_pty=pty, timeout=90)
        if pty:
            # v2.0.93f：等命令真正结束再读输出（start.sh start 在 CrashCore 运行时会先 stop
            # 清防火墙再重启，全程可能 30-60s；原先 sleep(2)+read() 提前返回导致初始化未完成
            # 就判定成功 → 路由器代理不生效。recv_exit_status 阻塞至命令退出）
            so.channel.recv_exit_status()
        out.append(so.read().decode(errors="replace"))
    except Exception as e:
        out.append("ERR:" + str(e)[:200])
c.close()
print(json.dumps(out, ensure_ascii=False))
'''

def _router_exec(cmds, pty=False, timeout=45):
    cred = _router_cred()
    script = _SCRIPT_TMPL.format(host=cred['host'], port=cred['port'],
                                 user=cred['username'], pw=cred['password'])
    payload = base64.b64encode(json.dumps(cmds).encode()).decode()
    cmd = ['docker', 'exec', CONTAINER]
    if PARAMIKO_PATH:
        cmd += ['env', 'PYTHONPATH=' + PARAMIKO_PATH]
    cmd += [PY, '-c', script, payload, '1' if pty else '0']
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        # v2.0.93f：PTY 下 recv_exit_status 遇命令退出码非 0 时 paramiko 可能抛异常，
        # 但输出已写入 stdout——有输出时按输出处理
        if r.stdout.strip():
            try:
                return json.loads(r.stdout.strip().splitlines()[-1]), None
            except Exception:
                pass
        return None, r.stderr.strip()[:300]
    try:
        return json.loads(r.stdout.strip().splitlines()[-1]), None
    except Exception:
        return None, r.stdout.strip()[:300]

def _router_status():
    cmds = [
        'hostname',
        'top -n 1 -b | head -5',
        'uptime',
        'cat /sys/class/thermal/thermal_zone0/temp 2>/dev/null || echo none',
        'ps | grep -i CrashCore | grep -v grep | head -2',
        "cat /proc/net/tcp /proc/net/tcp6 | grep -E ':(1EC2|270F)' | head -4",
        "cat /proc/net/arp 2>/dev/null | grep '0x2' | grep -c 'br-lan'",
    ]
    outs, err = _router_exec(cmds)
    if outs is None:
        return {'ok': False, 'error': err or '无法连接路由器'}
    import re
    try:
        hostname = (outs[0] or '').strip() or 'unknown'
        top = outs[1] or ''
        up_raw = (outs[2] or '').strip()
        temp_raw = (outs[3] or '').strip()
        clash_proc = (outs[4] or '').strip()
        clash_port = (outs[5] or '').strip()
        try:
            online_devices = int((outs[6] or '0').strip() or 0)
        except Exception:
            online_devices = 0
        # CPU 使用率：top 的 CPU 行（100 - idle）
        cpu_pct = 0.0
        m = re.search(r'CPU:.*?([\d.]+)% idle', top)
        if m:
            cpu_pct = round(100 - float(m.group(1)), 1)
        # 内存：top 的 Mem 行（KB）
        mem_total = mem_free = 0
        m = re.search(r'Mem:\s*([\d]+)K used,\s*([\d]+)K free', top)
        if m:
            mem_total = int(m.group(1)) + int(m.group(2))
            mem_free = int(m.group(2))
        # 负载：top 的 Load average 行
        load = '--'
        m = re.search(r'Load average:\s*([\d.]+ [\d.]+ [\d.]+)', top)
        if m:
            load = m.group(1)
        # 运行时间：uptime 的 up 段
        up = '--'
        m = re.search(r'up\s+((?:\d+ days?, )?[\d:]+)', up_raw)
        if m:
            up = m.group(1)
        # 温度
        temp = '--'
        if temp_raw.isdigit():
            temp = str(round(int(temp_raw) / 1000)) + '°C'
        def kb2gb(k):
            try:
                return round(k / 1048576, 1)
            except Exception:
                return 0
        return {
            'ok': True,
            'hostname': hostname,
            'load': load,
            'cpu_pct': cpu_pct,
            'uptime': up,
            'mem_total_gb': kb2gb(mem_total),
            'mem_free_gb': kb2gb(mem_free),
            'temp': temp,
            'clash_running': bool(clash_proc),
            'clash_port': bool(clash_port),
            'online_devices': online_devices,
        }
    except Exception as e:
        return {'ok': False, 'error': '解析失败: ' + str(e)[:200]}

def _clash(action):
    if action not in ('start', 'stop'):
        return {'ok': False, 'error': 'action 需为 start/stop'}
    # 先查当前进程状态
    outs0, _ = _router_exec(['ps | grep -i CrashCore | grep -v grep | head -2'])
    running0 = bool(outs0 and outs0[0].strip())
    if action == 'start':
        # v2.0.93g：已在运行 → 直接返回，不执行 start.sh start！
        # start.sh start 检测到 CrashCore 在跑会先 stop（清防火墙/TUN）再重启，
        # OpenWrt procd 分支下 stop→start 竞态导致初始化失败 → 路由器代理失效（用户实测）。
        if running0:
            return {'ok': True, 'clash_running': True, 'note': 'Clash 已在运行'}
        # 干净启动：标准 init.d 启动（尊重 procd 管理，避免 start.sh 分支问题）
        try:
            outs, err = _router_exec(['/etc/init.d/shellcrash start 2>&1'], pty=True, timeout=90)
        except Exception as e:
            return {'ok': False, 'error': '执行异常: ' + str(e)[:200], 'clash_running': False}
        if outs is None:
            return {'ok': False, 'error': err or '执行失败', 'clash_running': False}
        # v2.0.95g：等 9999 端口 + fwmark 规则（afstart 配防火墙是后台执行，必须等它完成；
        # 否则核心起了但流量没劫持 → 显示成功但代理不生效，用户实测）
        import time
        ok = False
        for _ in range(8):
            time.sleep(5)
            outs2, _ = _router_exec(["cat /proc/net/tcp /proc/net/tcp6 | grep ':270F' | grep -c 0A"])
            port_ok = bool(outs2 and outs2[0].strip() and outs2[0].strip() != '0')
            outs3, _ = _router_exec(['ip rule show | grep -c fwmark'])
            rule_ok = bool(outs3 and outs3[0].strip() and outs3[0].strip() != '0')
            if port_ok and rule_ok:
                ok = True
                break
        if not ok:
            # 规则未配（afstart 后台执行失败/未完成）→ 手动补跑 afstart
            _router_exec(['ash /data/ShellCrash/starts/afstart.sh 2>&1'], pty=True, timeout=90)
            time.sleep(3)
            outs3, _ = _router_exec(['ip rule show | grep -c fwmark'])
            rule_ok = bool(outs3 and outs3[0].strip() and outs3[0].strip() != '0')
            if rule_ok:
                return {'ok': True, 'clash_running': True, 'note': '已启动并补齐防火墙规则'}
            return {'ok': False, 'error': '启动超时（9999 端口或防火墙规则未就绪），请稍后重试',
                    'clash_running': False}
        return {'ok': True, 'clash_running': True}
    # stop
    if not running0:
        return {'ok': True, 'clash_running': False, 'note': 'Clash 未在运行'}
    try:
        outs, err = _router_exec(['/etc/init.d/shellcrash stop 2>&1'], pty=True, timeout=90)
    except Exception as e:
        return {'ok': False, 'error': '执行异常: ' + str(e)[:200], 'clash_running': False}
    # 清残留进程（killall 触发 procd respawn 杀不干净，必须 init.d stop + pidof 精确杀）
    _router_exec(['for p in $(pidof CrashCore); do kill -9 $p 2>/dev/null; done; sleep 1'])
    return {'ok': True, 'clash_running': False, 'note': '已停止'}


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, X-Auth-Token')

    def _check_auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, "X-Router-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def _send(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self._cors()
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._check_auth():
            self._send({'ok': False, 'error': '未授权'}, 401)
            return
        if self.path == '/api/router/status' or self.path.startswith('/api/router/status?'):
            self._send(_router_status())
            return
        self._send({'ok': False, 'error': 'not found'}, 404)

    def do_POST(self):
        if not self._check_auth():
            self._send({'ok': False, 'error': '未授权'}, 401)
            return
        if self.path in ('/api/router/clash/start', '/api/router/clash/stop'):
            action = 'start' if self.path.endswith('start') else 'stop'
            self._send(_clash(action))
            return
        self._send({'ok': False, 'error': 'not found'}, 404)

    def log_message(self, fmt, *args):
        pass


def run_server(port=9136):
    server = HTTPServer(('127.0.0.1', port), Handler)
    print(f"Router API on :{port}", flush=True)
    server.serve_forever()

if __name__ == '__main__':
    run_server()
