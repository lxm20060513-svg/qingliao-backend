#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""硬件状态 API：CPU 温度 / NVMe 温度（内核 sysfs 直读，零依赖）

接口：GET /api/hw/status → {"cpu_temp": 51.0, "ssd_temp": 38.8}
风扇转速：内核未暴露（hwmon 无 fan 节点，UGOS 私有 API 未找到），暂不提供
"""
import json
import os
from http.server import BaseHTTPRequestHandler

THERMAL_PATH = "/sys/class/thermal/thermal_zone1/temp"      # x86_pkg_temp
SSD_PATH = "/sys/class/hwmon/hwmon1/temp1_input"            # NVMe


def _read_temp(path):
    try:
        with open(path) as f:
            v = int(f.read().strip())
        # 毫摄氏度 → 摄氏度
        return round(v / 1000.0, 1) if v > 1000 else v
    except Exception:
        return None


def _hw_status():
    return {
        "cpu_temp": _read_temp(THERMAL_PATH),
        "ssd_temp": _read_temp(SSD_PATH),
    }


class HwHandler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token")

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _check_auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, "X-HW-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/api/hw/status"):
            self._send(200, {"ok": True, **_hw_status()})
            return
        self._send(404, {"error": "Not Found"})

    def log_message(self, fmt, *args):
        pass
