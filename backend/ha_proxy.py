#!/usr/bin/env python3
"""HA 本地代理：/api/ha/* -> http://localhost:8123/api/*
浏览器端 :8080 -> 本代理 :9127 -> HA :8123（token 硬编码，避免 CORS 问题）
"""
import http.server
import json
import urllib.request
import urllib.error
import hmac

import json as _json, os as _os
import os
DATA_DIR = os.environ.get("QL_DATA_DIR", "/data")

def _ha_config():
    try:
        return _json.load(open(os.path.join(DATA_DIR, "ha_config.json")))
    except Exception:
        return {}

def _ha_cred():
    c = _ha_config()
    return c.get('address') or HA_URL, c.get('token') or HA_TOKEN

HA_URL = os.environ.get("QL_HA_URL", "http://localhost:8123")
HA_TOKEN = os.environ.get("QL_HA_TOKEN", "")
HA_PASSWORD = os.environ.get("QL_PASSWORD", "change-me")


def _keep_entity(e):
    """v2.0.88g：states 裁剪——只保留 App/Web 看板与设备面板用到的实体，
    207KB 全量在热点/弱网下会超时导致面板离线（其他板块秒回）。新增实体类型时在此补充。"""
    eid = e.get("entity_id", "")
    # 灯/空调全保留（设备面板按 domain 过滤）
    if eid.startswith(("light.", "climate.")):
        return True
    # 开关只保留灯面板追加的两个 NAS 插座（79 个 switch 全保留是浪费）
    if eid in ("switch.chuangmi_cn_237985068_m3_on_p_2_1",
               "switch.lumi_cn_lumi_158d00039bca0b_v1_on_p_2_1"):
        return True
    # 门锁/猫眼只保留电量 + 安防状态
    if ("bacn01" in eid or "chuangmi" in eid) and "battery_level" in eid:
        return True
    if "alarmstatus" in eid:
        return True
    # 温度传感器（看板温度卡）
    if eid.startswith("sensor.") and "temperature" in eid:
        return True
    return False


class HAProxyHandler(http.server.BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-HA-Password, X-Auth-Token")

    def _check_auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, 'X-HA-Password', HA_PASSWORD)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def _proxy(self, method="GET"):
        if not self._check_auth():
            self.send_response(401)
            self._cors()
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "unauthorized"}).encode())
            return
        if not self.path.startswith("/api/ha/"):
            self.send_error(404, "Not Found")
            return
        # /api/ha/states -> /api/states
        path = self.path.replace("/api/ha", "/api", 1)
        if "?" in path:
            path, _, query = path.partition("?")
            path = path + "?" + query
        url = HA_URL + path

        body = None
        _addr, _tok = _ha_cred()
        headers = {"Authorization": "Bearer " + _tok}
        if method == "POST":
            length = int(self.headers.get("Content-Length", 0) or 0)
            if length > 0:
                body = self.rfile.read(length)
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            resp = urllib.request.urlopen(req, timeout=10)
            data = resp.read()
            status = resp.status
            ctype = resp.headers.get("Content-Type", "application/json")
        except urllib.error.HTTPError as e:
            data = e.read()
            status = e.code
            ctype = e.headers.get("Content-Type", "application/json")
        except Exception as e:
            self.send_response(502)
            self._cors()
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())
            return

        # v2.0.88g：/api/states 全量 207KB，热点/弱网下 App 30s 超时 → 智能家居面板离线。
        # 裁剪为看板/设备面板实际使用的实体，响应降到 ~20KB（其他板块几百字节所以一直正常）。
        if method == "GET" and path.rstrip("/") == "/api/states":
            try:
                arr = json.loads(data)
                if isinstance(arr, list):
                    data = json.dumps([e for e in arr if _keep_entity(e)],
                                      ensure_ascii=False).encode("utf-8")
            except Exception:
                pass

        self.send_response(status)
        self._cors()
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_json(self, data, code=200):
        body = _json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_OPTIONS(self):
        # v2.0.116 review：补全 preflight 头（原缺失 Methods/Headers，浏览器跨域预检失败；
        # 且此定义覆盖了 62 行的同名方法——保留完整版）
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Auth-Token, X-HA-Password')
        self.end_headers()

    def do_GET(self):
        if self.path == '/api/ha/config' or self.path.startswith('/api/ha/config?'):
            c = _ha_config()
            self._send_json({'ok': True, 'address': c.get('address', ''), 'has_token': bool(c.get('token'))})
            return

        self._proxy("GET")

    def do_POST(self):
        if self.path == '/api/ha/config' or self.path.startswith('/api/ha/config?'):
            try:
                n = int(self.headers.get('Content-Length', 0))
                body = _json.loads(self.rfile.read(n).decode('utf-8'))
            except Exception:
                body = {}
            c = _ha_config()
            addr = str(body.get('address', '')).strip()
            tok = str(body.get('token', '')).strip()
            if addr:
                c['address'] = addr
            if tok:
                c['token'] = tok
            open(os.path.join(DATA_DIR, "ha_config.json"), "w").write(_json.dumps(c, ensure_ascii=False))
            _os.chmod(os.path.join(DATA_DIR, "ha_config.json"), 0o600)
            self._send_json({'ok': True})
            return

        self._proxy("POST")

    def log_message(self, fmt, *args):
        print("[HAProxy] %s - %s" % (self.address_string(), fmt % args))


if __name__ == "__main__":
    server = http.server.HTTPServer(("0.0.0.0", 9127), HAProxyHandler)
    print("HA proxy listening on :9127")
    server.serve_forever()

