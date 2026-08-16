# -*- coding: utf-8 -*-
"""定时自动化（v2.0.104）：AI 生成"X分钟后执行Y"的延迟动作，到点自动执行 HA 调用，执行后自动删除。

存储：QL_DATA_DIR/automations.json  [{id, name, actions, run_at, created}]
调度：模块 import 时启动守护线程，每 3s 检查到期任务并执行（复用场景执行的 HA 调用逻辑）
"""
import json
import os
import threading
import time
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("QL_DATA_DIR", os.path.join(os.path.dirname(BASE), "data"))
AUTO_FILE = os.path.join(DATA_DIR, "automations.json")
LOG_FILE = os.path.join(DATA_DIR, "automations.log")

HA_URL = os.environ.get("QL_HA_URL", "http://localhost:8123")
HA_TOKEN = os.environ.get("QL_HA_TOKEN", "")

_lock = threading.Lock()
_scheduler_started = False


def _load():
    try:
        with open(AUTO_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def _save(items):
    with _lock:
        try:
            tmp = AUTO_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False, indent=1)
            os.replace(tmp, AUTO_FILE)
        except Exception:
            pass


def exec_actions(actions):
    """逐条执行 HA 调用（service 点号拆斜杠，与 scenes_api 同逻辑）"""
    results = []
    ok = 0
    for act in actions or []:
        entity = act.get("entity", "")
        service = act.get("service", "")
        data = act.get("data") or {}
        try:
            domain, _, svc = service.partition(".")
            svc_path = svc or domain
            body = json.dumps({"entity_id": entity, **data}).encode()
            req = urllib.request.Request(f"{HA_URL}/api/services/{domain}/{svc_path}", data=body,
                                         headers={"Authorization": "Bearer " + HA_TOKEN,
                                                  "Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req, timeout=15):
                ok += 1
                results.append(f"✅ {service} {entity}")
        except Exception as e:
            results.append(f"❌ {service} {entity}：{str(e)[:60]}")
    return ok, results


def create_automation(name, actions, delay_seconds):
    if not name or not actions or delay_seconds <= 0:
        return False, "自动化参数不完整（需要名称/动作/延迟秒数）"
    item = {
        "id": uuid.uuid4().hex[:12],
        "name": name,
        "actions": actions,
        "run_at": time.time() + delay_seconds,
        "created": time.strftime("%Y-%m-%d %H:%M"),
    }
    items = _load()
    items.append(item)
    _save(items)
    if delay_seconds < 60:
        return True, f"已创建自动化「{name}」，{delay_seconds} 秒后执行"
    m, s = divmod(delay_seconds, 60)
    if s == 0:
        return True, f"已创建自动化「{name}」，{m} 分钟后执行"
    return True, f"已创建自动化「{name}」，{m} 分 {s} 秒后执行"


def list_automations():
    now = time.time()
    items = []
    for a in _load():
        run_at = a.get("run_at") or 0
        if run_at <= now:
            continue  # 过期未执行的直接忽略
        items.append({
            "id": a.get("id"),
            "name": a.get("name"),
            "actions": a.get("actions") or [],
            "run_at": run_at,
            "remaining": int(run_at - now),
            "created": a.get("created", ""),
        })
    items.sort(key=lambda x: x["run_at"])
    return items


def cancel_automation(aid):
    items = _load()
    new = [a for a in items if a.get("id") != aid]
    if len(new) == len(items):
        return False
    _save(new)
    return True


def _scheduler():
    while True:
        try:
            now = time.time()
            items = _load()
            due = [a for a in items if (a.get("run_at") or 0) <= now]
            if due:
                remaining = [a for a in items if (a.get("run_at") or 0) > now]
                for a in due:
                    ok, results = exec_actions(a.get("actions", []))
                    try:
                        with open(LOG_FILE, "a", encoding="utf-8") as f:
                            f.write(f"[{time.strftime('%m-%d %H:%M:%S')}] 「{a.get('name')}」"
                                    f"{'执行成功' if ok else '执行失败'} | {'; '.join(results)}\n")
                    except Exception:
                        pass
                    # v2.0.113：执行结果 → 微信推送队列（Hermes cron 每分钟投递）
                    try:
                        import push_api
                        push_api.enqueue(f"⏱ 自动化「{a.get('name')}」已执行："
                                         f"{'✅ 成功' if ok else '❌ 失败'}"
                                         + (f"（{'; '.join(results[:2])}）" if results else ""))
                    except Exception:
                        pass
                _save(remaining)
        except Exception:
            pass
        time.sleep(3)


def start_scheduler():
    global _scheduler_started
    if _scheduler_started:
        return
    _scheduler_started = True
    threading.Thread(target=_scheduler, daemon=True, name="ql-auto-scheduler").start()


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Auth-Token, X-Automations-Password")

    def _send(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            pass

    def _auth(self):
        pw = os.environ.get("QL_PASSWORD", "change-me")
        return (self.headers.get("X-Automations-Password") == pw
                or self.headers.get("X-Auth-Token") == pw)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if self.path.startswith("/api/automations/list"):
            self._send(200, {"ok": True, "automations": list_automations()})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        if self.path.startswith("/api/automations/create"):
            try:
                n = int(self.headers.get("Content-Length") or 0)
                d = json.loads(self.rfile.read(n) or b"{}")
                ok, msg = create_automation(d.get("name", ""), d.get("actions") or [],
                                            int(d.get("delay_seconds") or 0))
                self._send(200, {"ok": ok, "message": msg, "automations": list_automations()})
            except Exception as e:
                self._send(400, {"ok": False, "error": str(e)[:200]})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def do_DELETE(self):
        if self.path.startswith("/api/automations/"):
            aid = self.path.rsplit("/", 1)[-1]
            ok = cancel_automation(aid)
            self._send(200, {"ok": ok, "automations": list_automations()})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, *a):
        pass


# import 时启动调度器（qingliao_all 单进程 import 各模块）
start_scheduler()
