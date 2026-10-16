#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""日程预排接口（v4.0.118 · 推送及时性 ①：把已知时刻的事件预排成本地通知）

为什么有这一层
--------------
iOS 不给 App 常驻，后台唤醒（BGAppRefreshTask / background fetch）**时刻由系统决定**，
所以「定时任务到点」这件事靠"唤醒后补拉"永远会迟到。而 `UNCalendarNotificationTrigger`
是**系统级闹钟**：请求一登记，到点必响，App 被杀 / 手机重启 / 用户从没打开过都照样响。

⇒ 把"已知时刻"的事件（Hermes 侧的定时任务：每日早报、收盘播报、每晚记账提醒…）
提前算好时刻表交给 App，App 用它预排系统级通知 —— 准点必响，不依赖任何唤醒。

分工（谁算时刻、谁排通知）
--------------------------
- **Hermes 侧**（`/opt/data/scripts/ql_sched/push_schedule.py`）最懂 cron 表达式：读
  `cron/jobs.json`、展开未来 48h 的触发时刻 → `POST` 到本模块**整体覆盖**写入。
- **本模块**只做「存 + 校验 + 读」：不解析 cron、不猜时刻（无状态最省事，也最好测）。
- **App 侧**（`ScheduleStore`）拉清单 → 用稳定 identifier 重排系统通知（幂等替换、清孤儿）。

端点
----
  GET  /api/agent/schedule   （或 /api/schedule）→ 未来窗口内的时刻表
       鉴权：X-Auth-Token（App 登录 token）或 X-Inbox-Token（服务间 token）
       返回：{"ok":true,"now":1712345678,"updatedAt":1712345000,
              "items":[{"id":"973ab2b2438f_1712361600","jobId":"973ab2b2438f",
                        "title":"每日早报","fireAt":1712361600,"note":"每天 08:00"}]}
  POST /api/schedule         → Hermes 侧整体覆盖写入（**只认服务间 token**）
       body：{"items":[{"id":..,"jobId":..,"title":..,"fireAt":<epoch 秒>,"note":..}]}
       返回：{"ok":true,"count":N}；非法条目**逐条丢弃并计数**（不让一条坏数据毁整批）

约定
----
- 只存**未来**条目：写入时丢弃已过时刻（`now - GRACE_S` 之前）；读取时再过滤一次
  （清单可能几天没更新，读到过期条目会让 App 排一条永远不响的通知，白占系统配额）。
- `id` 稳定且**每次出现唯一**：`<jobId>_<fireAt>`（同一任务的不同天/不同时刻不同 id，
  同一时刻重排时 id 相同 → App 端同 identifier 是**替换**语义，天然不堆叠）。
- iOS 每个 App 最多 64 条挂起通知 → 读取时按时刻升序截断 `MAX_RETURN` 条，让 App 只排最近的。
- 写盘原子（mkstemp + os.replace）并**显式保住属主**：mode 与 owner 是两件事，
  原子写只沿用 mode，漏 chown 会让 root 属主的文件被 Hermes（uid 10000）读不了。
"""
import hmac
import json
import os
import re
import tempfile
import time
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("QL_DATA_DIR", os.path.join(os.path.dirname(BASE), "data"))
STORE_FILE = os.path.join(DATA_DIR, "schedule.json")
# 服务间 token 与 inbox_api 同源（同一个容器 env，Hermes 侧读同一个文件）
INBOX_TOKEN = os.environ.get("QL_INBOX_TOKEN", "")

# 时刻表最长覆盖（写作时丢弃更远/更早的条目；48h 够 App 攒一天半余量）
WINDOW_S = 48 * 3600
# 写入/读取时允许的"刚过"宽限（App 拉到手时可能已经过了几秒）
GRACE_S = 120
# 单次下发上限（iOS 每 App 最多 64 条挂起通知，给用户自建提醒留余量）
MAX_RETURN = 24
# 写入上限（防一条脚本 bug 灌爆文件）
MAX_STORE = 200

_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,80}$")


def _now():
    return int(time.time())


def _load():
    """读时刻表；任何异常都退化成空表（时刻表坏了绝不能拖垮 App 拉取）。"""
    try:
        with open(STORE_FILE, encoding="utf-8") as f:
            d = json.load(f)
        if isinstance(d, dict) and isinstance(d.get("items"), list):
            return d
    except Exception:
        pass
    return {"updatedAt": 0, "items": []}


def _save(items):
    """原子写 + 显式复原属主（mode 与 owner 是两件事，漏 chown = Hermes 侧读不了）。"""
    payload = {"updatedAt": _now(), "items": items}
    os.makedirs(DATA_DIR, exist_ok=True)
    try:
        st = os.stat(STORE_FILE)
        uid, gid = st.st_uid, st.st_gid
        mode = st.st_mode & 0o777
    except Exception:
        uid = gid = None
        mode = 0o644
    fd, tmp = tempfile.mkstemp(dir=DATA_DIR, prefix=".schedule-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.chmod(tmp, mode)
        if uid is not None:
            try:
                os.chown(tmp, uid, gid)
            except Exception:
                pass
        os.replace(tmp, STORE_FILE)
    except Exception:
        try:
            os.unlink(tmp)
        except Exception:
            pass
        raise
    return payload


def _clean_items(raw):
    """校验 + 归一。返回 (items, dropped)：坏条目**逐条丢**，不让一条毁整批。"""
    now = _now()
    out, dropped = [], 0
    seen = set()
    for it in (raw or []):
        try:
            if not isinstance(it, dict):
                dropped += 1
                continue
            iid = str(it.get("id") or "").strip()
            title = str(it.get("title") or "").strip()
            fire = int(float(it.get("fireAt")))
            if not _ID_RE.match(iid) or not title or not fire:
                dropped += 1
                continue
            if fire < now - GRACE_S or fire > now + WINDOW_S:
                dropped += 1
                continue
            if iid in seen:
                dropped += 1
                continue
            seen.add(iid)
            out.append({
                "id": iid[:80],
                "jobId": str(it.get("jobId") or "")[:64],
                "title": title[:60],
                "fireAt": fire,
                "note": str(it.get("note") or "")[:40],
            })
        except Exception:
            dropped += 1
    out.sort(key=lambda x: x["fireAt"])
    if len(out) > MAX_STORE:
        dropped += len(out) - MAX_STORE
        out = out[:MAX_STORE]
    return out, dropped


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, X-Auth-Token, X-Inbox-Token")

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

    def _auth_app(self):
        try:
            import auth_api
            return auth_api.check_auth(self.headers, "X-Schedule-Password", "")
        except Exception:
            return False

    def _auth_hermes(self):
        return bool(INBOX_TOKEN) and hmac.compare_digest(
            self.headers.get("X-Inbox-Token", ""), INBOX_TOKEN)

    def do_OPTIONS(self):
        # CORS 预检不经鉴权（返回 401 会让预检直接失败，表现为"接口完全打不通"）
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not (self._auth_app() or self._auth_hermes()):
            self._send(401, {"ok": False, "error": "unauthorized"})
            return
        try:
            args = parse_qs(urlparse(self.path).query)
        except Exception:
            args = {}
        try:
            limit = int(float((args.get("limit") or [str(MAX_RETURN)])[0]))
        except Exception:
            limit = MAX_RETURN
        limit = max(1, min(limit, MAX_RETURN))
        st = _load()
        now = _now()
        items = [it for it in st.get("items", [])
                 if isinstance(it, dict) and int(it.get("fireAt") or 0) > now - GRACE_S]
        items.sort(key=lambda x: x["fireAt"])
        self._send(200, {"ok": True, "now": now,
                         "updatedAt": int(st.get("updatedAt") or 0),
                         "count": len(items), "items": items[:limit]})

    def do_POST(self):
        # 写入只认服务间 token：这是"App 要照着排系统通知"的数据，不能让任意客户端灌
        if not self._auth_hermes():
            self._send(401, {"ok": False, "error": "unauthorized"})
            return
        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n).decode("utf-8")) if n else {}
        except Exception:
            self._send(200, {"ok": False, "error": "bad json"})
            return
        if not isinstance(body, dict):
            self._send(200, {"ok": False, "error": "bad body"})
            return
        items, dropped = _clean_items(body.get("items"))
        try:
            _save(items)
        except Exception as e:
            self._send(200, {"ok": False, "error": "save failed: %s" % str(e)[:120]})
            return
        self._send(200, {"ok": True, "count": len(items), "dropped": dropped})
