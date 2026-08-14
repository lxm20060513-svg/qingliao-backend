#!/usr/bin/env python3
"""系统日志 API：/api/logs/sys?level=&q=&limit=
读取 Hermes 容器日志 + cron_api 日志，供前端日志页展示
"""
import http.server
import json
import subprocess
import urllib.parse
import time
import os
DATA_DIR = os.environ.get("QL_DATA_DIR", "/data")
import hmac

CONTAINER = os.environ.get("QL_LOG_CONTAINER", "hermes")
LOGS_PASSWORD = os.environ.get("QL_PASSWORD", "change-me")
LOG_SOURCES = [
    ("hermes", "docker logs --tail 300 " + CONTAINER + " 2>&1"),
    ("cron", "cat /tmp/cron_api.log 2>/dev/null | tail -200"),
    ("haproxy", "cat /tmp/ha_proxy.log 2>/dev/null | tail -100"),
]
# 可清除的本地日志文件（docker 日志不可清）
CLEARABLE_LOGS = [
    "/tmp/cron_api.log",
    "/tmp/ha_proxy.log",
    "/tmp/logs_api.log",
    "/tmp/files_api.log",
]


def parse_level(line):
    up = line.upper()
    if "ERROR" in up or "TRACEBACK" in up or "FATAL" in up:
        return "ERROR"
    if "WARN" in up or "WARNING" in up:
        return "WARNING"
    return "INFO"


def collect_logs(level_filter="all", keyword="", limit=200):
    logs = []
    for source, cmd in LOG_SOURCES:
        try:
            out = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=8)
            text = out.stdout or ""
        except Exception:
            text = ""
        for line in text.splitlines():
            line = line.rstrip()
            if not line.strip():
                continue
            # 提取时间（常见格式 2026-08-07 18:00:00 或 ISO）
            time_str = ""
            import re
            m = re.search(r"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})", line)
            if m:
                time_str = m.group(1)
            lvl = parse_level(line)
            if level_filter != "all" and lvl != level_filter:
                continue
            if keyword and keyword.lower() not in line.lower():
                continue
            logs.append({
                "source": source,
                "time": time_str,
                "level": lvl,
                "msg": line[:500],
            })
    # 按时间倒序（有时间的优先），无时间的排后面
    logs.sort(key=lambda x: (x["time"] or "0000"), reverse=True)
    return logs[:limit]


class LogsHandler(http.server.BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Logs-Password, X-Auth-Token")

    def _check_auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, 'X-Logs-Password', LOGS_PASSWORD)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._check_auth():
            self.send_response(401)
            self._cors()
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error":"unauthorized"}')
            return
        if not self.path.startswith("/api/logs/sys") and not self.path.startswith("/api/logs/export"):
            self.send_error(404)
            return
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        level = params.get("level", ["all"])[0]
        q = params.get("q", [""])[0]
        limit = int(params.get("limit", ["200"])[0])
        logs = collect_logs(level_filter=level, keyword=q, limit=limit)

        if self.path.startswith("/api/logs/export"):
            # 导出纯文本（不限条数，取更多）
            logs = collect_logs(level_filter=level, keyword=q, limit=2000)
            lines = []
            for l in logs:
                src = l["source"]
                t = l["time"]
                lvl = l["level"]
                lines.append(f"[{t}] [{lvl}] ({src}) {l['msg']}")
            text = "\n".join(lines).encode("utf-8")
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="qingliao_syslog_' + urllib.parse.quote(time.strftime("%Y%m%d_%H%M%S")) + '.txt"')
            self.send_header("Content-Length", str(len(text)))
            self.end_headers()
            self.wfile.write(text)
            return

        body = json.dumps({"logs": logs, "count": len(logs)}).encode()
        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if not self._check_auth():
            self.send_response(401)
            self._cors()
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error":"unauthorized"}')
            return
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith('/api/logs/crash'):
            # v2.0.43 崩溃上报：App crash handler 上传的崩溃信息，追加 JSONL
            try:
                n = int(self.headers.get('Content-Length', 0))
                raw = self.rfile.read(n).decode('utf-8') if n > 0 else '{}'
                info = json.loads(raw or '{}')
            except Exception:
                info = {}
            if not info:
                body = json.dumps({"error": "empty body"}).encode()
                self.send_response(400)
                self._cors()
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            import time as _t
            record = {
                "ts": int(_t.time()),
                "app": info.get("app") or "qingliao-ios",
                "version": info.get("version") or "",
                "platform": info.get("platform") or "",
                "device": info.get("device") or "",
                "os": info.get("os") or "",
                "type": info.get("type") or "crash",
                "stack": (info.get("stack") or "")[:4000],
            }
            try:
                # v2.0.47：崩溃日志统一放轻聊文件夹/logs/（用户要求）
                crash_dir = os.path.join(DATA_DIR, "logs")
                crash_file = os.path.join(crash_dir, 'crash_reports.log')
                os.makedirs(crash_dir, exist_ok=True)
                with open(crash_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                body = json.dumps({"ok": True}).encode()
                self.send_response(200)
            except OSError as e:
                body = json.dumps({"ok": False, "error": str(e)[:100]}).encode()
                self.send_response(500)
            self._cors()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path.startswith('/api/logs/clear'):
            cleared = []
            for p in CLEARABLE_LOGS:
                try:
                    if os.path.exists(p):
                        open(p, 'w').close()
                        cleared.append(p)
                except OSError:
                    pass
            body = json.dumps({"ok": True, "cleared": cleared}).encode()
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)

    def log_message(self, fmt, *args):
        pass  # 静默


if __name__ == "__main__":
    server = http.server.HTTPServer(("0.0.0.0", 9128), LogsHandler)
    print("Logs API listening on :9128")
    server.serve_forever()
