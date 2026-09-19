# -*- coding: utf-8 -*-
"""轻聊 App 收件箱（inbox）：Hermes 主动推送给轻聊App 的消息队列。

背景（v3.0.8x）：轻聊 App 是「App 主动请求 → 服务端响应」模型，没有服务端主动
向 App 塞消息的通道。本模块给 App 提供一条可轮询的收件箱流——Hermes（agent/cron/
其他事件方）把要主动推给用户的消息入队，App 后台/回前台时轮询拉取，
拉到后注入当前聊天会话 + 弹本地通知，再标记已读。

存储：QL_DATA_DIR/inbox_queue.json  [{id, text, source_task_id, task_type, ts, status: pending|sending|done}]
task_type（v3.4.x 任务中心分类）：reply=AI回复 / cron=定时·自动任务 / system=系统通知 /
progress=长任务进行中进度（v3.7.1：App 注入 🔔 进度气泡，不进模型上下文）。
与 push_api 同款 RLock + JSON 队列模式（RLock 防嵌套自死锁，v3.0.28 教训）。

接口（统一路由 9127 /api/inbox/*）：
  GET  /api/inbox                 App 轮询：拉待推送消息（pending→sending）
  POST /api/inbox/{id}/done       App 消费后标记已读（从队列移除）
  POST /api/inbox/push            Hermes 主动推消息入队（鉴权 X-Inbox-Token）

鉴权：
  - 轮询/已读：走 X-Auth-Token（App 登录 token，check_auth）
  - 推送：走 X-Inbox-Token（服务间 token，环境变量 QL_INBOX_TOKEN）

v3.4.8：push 记录新增 source_task_id（本轮回复的 taskId）→ App 端用不可变 taskId 去重，
根治「流式回复 + 收件箱推送」内容比对去重的竞态漏网。
v3.4.16：滞留治理——① sending 超时重投改按 sent_ts（标 sending 的时间）判定，
不再用入队 ts（老消息一拉就被反复重置重投）；② 新增滞留 TTL：pending/sending
超过 STALE_TTL（24h）仍未 done 直接舍弃，防 App 永不确认的僵尸永久占队列。
"""
import json
import os
import re
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("QL_DATA_DIR", os.path.join(os.path.dirname(BASE), "data"))
QUEUE_FILE = os.path.join(DATA_DIR, "inbox_queue.json")
# BE3：默认值曾是公开仓库里可读的常量，改空串=拒绝一切（fail-closed）；
# 部署时 .env 必须注入 QL_INBOX_TOKEN，且与 Hermes 插件侧使用同一个值。
INBOX_TOKEN = os.environ.get("QL_INBOX_TOKEN", "")
# 收件箱上限（防堆积）
QUEUE_LIMIT = 100
# sending 未 done 超过此时长（秒）→ 重置回 pending 重投
SENDING_TIMEOUT = 60
# 滞留 TTL（秒）：pending/sending 超过此时长仍未 done → 直接舍弃（清理僵尸）
STALE_TTL = 24 * 3600

_lock = threading.RLock()


def _load():
    try:
        with open(QUEUE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def _save(items):
    with _lock:
        try:
            tmp = QUEUE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False, indent=1)
            os.replace(tmp, QUEUE_FILE)
        except Exception:
            pass


def push(text, task_id=None, task_type="reply"):
    """Hermes 事件方调用：推送一条消息到轻聊 App 收件箱。
    v3.4.8：task_id 为该回复的流式任务 id（source_task_id），App 端用作不可变去重标识。
    v3.4.x：task_type 区分来源（reply/cron/system）→ App 端任务中心分类。
    """
    text = (text or "").strip()
    if not text:
        return False, "消息为空"
    # v3.7.1：放行 progress（长任务进行中进度）——否则会被改写成 reply，App 就当普通回复处理
    if task_type not in ("reply", "cron", "system", "progress"):
        task_type = "reply"
    with _lock:
        items = _load()
        items.append({"id": uuid.uuid4().hex[:12], "text": text,
                      "ts": time.time(), "status": "pending",
                      "source_task_id": task_id, "task_type": task_type})
        if len(items) > QUEUE_LIMIT:
            items = items[-QUEUE_LIMIT:]
        _save(items)
    return True, "已推送"


def pop_pending():
    """App 轮询：拉取待推送消息并标记 sending（防重复显示）。

    v3.4.16 滞留治理：
    - sending 重投判定用 sent_ts（标 sending 的时间），旧数据无 sent_ts 回退用 ts；
      避免「入队很久的消息一被拉到就因 ts 超时被反复重置重投」。
    - pending/sending 滞留超过 STALE_TTL（24h）直接舍弃，防 App 永不 markDone 的僵尸。
    """
    now = time.time()
    items = _load()
    picked = []
    rest = []
    for it in items:
        st = it.get("status")
        ts = it.get("ts") or 0
        if st == "pending":
            if now - ts > STALE_TTL:
                continue  # 滞留过久（App 长期未确认）→ 舍弃
            it["status"] = "sending"
            it["sent_ts"] = now
            picked.append(it)
        elif st == "sending":
            if now - ts > STALE_TTL:
                continue  # 滞留过久 → 舍弃
            sent_ts = it.get("sent_ts") or ts
            if now - sent_ts > SENDING_TIMEOUT:
                # 僵尸 sending（App 拉到但没来得及 done）→ 重置回 pending 重投
                it["status"] = "pending"
                it.pop("sent_ts", None)
                rest.append(it)
            else:
                rest.append(it)
        elif st == "done" and now - ts > 3600:
            continue  # 清理已完成的旧消息
        else:
            rest.append(it)
    _save(rest + picked)
    return picked


def mark_done(mid):
    with _lock:
        items = _load()
        new = [it for it in items if it.get("id") != mid]
        if len(new) != len(items):
            _save(new)
            return True
    return False


# ---- v3.4.23 搭载投递（piggyback）：stream poll 响应捎带待推消息 ----

def peek_pending():
    """只读快照：返回当前 pending 的消息（不改状态）。
    stream_api 在 App 流式轮询时调用，把结果搭在 poll 响应里送达（滞后归零）。
    """
    now = time.time()
    out = []
    for it in _load():
        st = it.get("status")
        ts = it.get("ts") or 0
        if st == "pending" and now - ts <= STALE_TTL:
            out.append({
                "id": it["id"], "text": it.get("text", ""), "ts": ts,
                "source_task_id": it.get("source_task_id"),
                "task_type": it.get("task_type", "reply"),
            })
    return out


def mark_sending(mid):
    """搭载投递后标记 sending（App 确认/超时重投逻辑与 pop_pending 一致）。"""
    now = time.time()
    with _lock:
        items = _load()
        changed = False
        for it in items:
            if it.get("id") == mid and it.get("status") == "pending":
                it["status"] = "sending"
                it["sent_ts"] = now
                changed = True
        if changed:
            _save(items)
        return changed


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token, X-Inbox-Token")

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
        """App 端鉴权：X-Auth-Token 登录 token。"""
        try:
            import auth_api
            return auth_api.check_auth(self.headers, "X-Inbox-Password", "")
        except Exception:
            return False

    def _auth_hermes(self):
        """Hermes 端鉴权：X-Inbox-Token 服务间 token。"""
        import hmac
        return bool(INBOX_TOKEN) and hmac.compare_digest(
            self.headers.get("X-Inbox-Token", ""), INBOX_TOKEN)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        # GET /api/inbox —— App 轮询拉取待推送消息
        if self.path.startswith("/api/inbox"):
            if not self._auth_app():
                self._send(401, {"ok": False, "error": "unauthorized"})
                return
            items = pop_pending()
            self._send(200, {"ok": True, "items": [{
                "id": it["id"], "text": it["text"], "ts": it.get("ts", 0),
                "source_task_id": it.get("source_task_id"),
                "task_type": it.get("task_type", "reply")
            } for it in items]})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        # POST /api/inbox/{id}/done —— App 消费后标记已读
        m = re.match(r"^/api/inbox/([0-9a-f]+)/done$", self.path)
        if m:
            if not self._auth_app():
                self._send(401, {"ok": False, "error": "unauthorized"})
                return
            ok = mark_done(m.group(1))
            self._send(200, {"ok": ok})
            return
        # POST /api/inbox/push —— Hermes 主动推消息
        if self.path.startswith("/api/inbox/push"):
            if not self._auth_hermes():
                self._send(401, {"ok": False, "error": "unauthorized"})
                return
            try:
                n = int(self.headers.get("Content-Length") or 0)
                d = json.loads(self.rfile.read(n) or b"{}")
                ok, msg = push(d.get("text", ""), d.get("source_task_id"), d.get("task_type", "reply"))
                self._send(200, {"ok": ok, "message": msg})
            except Exception as e:
                self._send(400, {"ok": False, "error": str(e)[:200]})
            return
        self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, *a):
        pass
