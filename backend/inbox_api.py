# -*- coding: utf-8 -*-
"""轻聊 App 收件箱（inbox）：Hermes 主动推送给轻聊App 的消息队列。

背景（v3.0.8x）：轻聊 App 是「App 主动请求 → 服务端响应」模型，没有服务端主动
向 App 塞消息的通道。本模块给 App 提供一条可轮询的收件箱流——Hermes（agent/cron/
其他事件方）把要主动推给用户的消息入队，App 后台/回前台时轮询拉取，
拉到后注入当前聊天会话 + 弹本地通知，再标记已读。

存储：QL_DATA_DIR/inbox_queue.json  [{id, text, source_task_id, task_type, ts, status: pending|sending|done}]
task_type（v3.4.x 任务中心分类）：reply=AI回复 / cron=定时·自动任务 / system=系统通知 /
progress=长任务进行中进度（v3.7.1：App 注入 🔔 进度气泡，不进模型上下文）/
question=AI 任务中途追问（v3.9.83：App 任务中心渲染成「可回答的问题卡」，用户在 App 作答后
AI 侧从 GET /api/inbox/answer 取走答案继续跑；answer/answered_ts 两字段只出现在 question 条目上）。
与 push_api 同款 RLock + JSON 队列模式（RLock 防嵌套自死锁，v3.0.28 教训）。

接口（统一路由 9127 /api/inbox/*）：
  GET  /api/inbox                 App 轮询：拉待推送消息（pending→sending）
  POST /api/inbox/{id}/done       App 消费后标记已读（从队列移除）
  POST /api/inbox/push            Hermes 主动推消息入队（鉴权 X-Inbox-Token）
  POST /api/inbox/answer          App 作答：{id,text} → 写 answer/answered_ts（v3.9.83）
  GET  /api/inbox/answer?id=&wait= AI 侧长轮询取答案（wait 上限 30s、1 秒一跳，v3.9.83）

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
from urllib.parse import parse_qs, urlparse

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


# ---- v3.9.57 消息归档：被丢弃/被确认的消息一律落 archive/ 可回溯（修"静默蒸发"） ----
ARCHIVE_DIR = os.path.join(DATA_DIR, "inbox_archive")


def _archive(entries, reason=None):
    """把即将从队列消失/已被确认的消息落盘归档。
    entries 支持两种形态：
      - [(item, reason), ...]  逐条带自己的原因（推荐）
      - [item, ...]            统一用 reason 参数
    铁律：归档失败不得影响主流程。
    """
    if not entries:
        return
    norm = []
    for e in entries:
        if isinstance(e, (tuple, list)) and len(e) == 2:
            norm.append((e[0], e[1] or reason or "unknown"))
        else:
            norm.append((e, reason or "unknown"))
    try:
        os.makedirs(ARCHIVE_DIR, exist_ok=True)
        ts = time.strftime("%Y%m%d")
        path = os.path.join(ARCHIVE_DIR, "inbox_%s.jsonl" % ts)
        with open(path, "a", encoding="utf-8") as f:
            for it, why in norm:
                rec = {
                    "archivedAt": time.time(),
                    "archivedAtStr": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "reason": why,
                    "id": it.get("id"),
                    "ts": it.get("ts") or 0,
                    "tsStr": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(it.get("ts") or 0)),
                    "status": it.get("status"),
                    "source_task_id": it.get("source_task_id"),
                    "task_type": it.get("task_type", "reply"),
                    "session_id": it.get("session_id"),
                    "text": it.get("text", ""),
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        print("[inbox] 归档失败（不影响主流程）:", str(e)[:120], flush=True)


def push(text, task_id=None, task_type="reply", want_id=False, session_id=None):
    """Hermes 事件方调用：推送一条消息到轻聊 App 收件箱。
    v3.4.8：task_id 为该回复的流式任务 id（source_task_id），App 端用作不可变去重标识。
    v3.4.x：task_type 区分来源（reply/cron/system）→ App 端任务中心分类。
    v3.9.83：放行 question（AI 中途追问卡片）——同样**不写投递会话**，
    投递会话依旧只装 cron/system（下面 if 一行不动即保证该行为不变）。
    v3.9.83：新增 want_id=False 开关（默认 False = 老行为，返回值仍是"已推送"）。
            AI 追问链路必须拿到 id 才能去 GET /api/inbox/answer 轮询答案，故传 want_id=True
            → 成功时返回 (True, "<12位hex id>")。老调用方未传该参数，行为零变化。
    """
    text = (text or "").strip()
    if not text:
        return False, "消息为空"
    # v3.7.1：放行 progress（长任务进行中进度）——否则会被改写成 reply，App 就当普通回复处理
    # v4.0.11：放行 agent（主动 Agent 中枢投递）——proactive_agent 投的主动消息
    # 必须走**会话气泡可回复**这条路，不能落到任务中心（那是 cron/system 的形态）。
    if task_type not in ("reply", "cron", "system", "progress", "question", "agent"):
        task_type = "reply"
    mid = uuid.uuid4().hex[:12]
    with _lock:
        items = _load()
        items.append({"id": mid, "text": text,
                      "ts": time.time(), "status": "pending",
                      "source_task_id": task_id, "task_type": task_type,
                      "session_id": (session_id or "").strip() or None})
        if len(items) > QUEUE_LIMIT:
            items = items[-QUEUE_LIMIT:]
        _save(items)
    # v3.9.71 delivery: cron/system 类投递详情同步写入固定会话「轻聊投递」；
    # reply/progress 是正常 AI 回复链路，刻意不进（用户要求）
    if task_type in ("cron", "system"):
        try:
            import sessions_api
            sessions_api.append_delivery_message(text, task_type=task_type)
        except Exception as e:
            print('[delivery] 投递写入异常: %s' % e, flush=True)
    # v4.0.x: 主动 Agent 消息写入固定主动会话「轻聊主动」——
    # 原先只进 inbox 池 → App 侧 InboxStore.consumeOne 注入「当前会话」→
    # 主动消息串进用户正在聊的正常会话。主动消息必须有自己归属的会话。
    # 仍保留下面 push 到 inbox 池那步：App 靠它弹通知（isPush 气泡 + 红点）。
    if task_type == "agent":
        try:
            import sessions_api
            sessions_api.append_proactive_message(text, task_type=task_type)
        except Exception as e:
            print('[proactive] 主动会话写入异常: %s' % e, flush=True)
    if want_id:
        return True, mid
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
    dropped = []  # v3.9.57：即将消失的消息 → 先归档再丢，杜绝静默蒸发
    for it in items:
        st = it.get("status")
        ts = it.get("ts") or 0
        if st == "pending":
            if now - ts > STALE_TTL:
                dropped.append((it, "pending_stale_%dh" % (STALE_TTL // 3600)))
                continue  # 滞留过久（App 长期未确认）→ 归档后舍弃
            it["status"] = "sending"
            it["sent_ts"] = now
            picked.append(it)
        elif st == "sending":
            if now - ts > STALE_TTL:
                dropped.append((it, "sending_stale_%dh" % (STALE_TTL // 3600)))
                continue  # 滞留过久 → 归档后舍弃
            sent_ts = it.get("sent_ts") or ts
            if now - sent_ts > SENDING_TIMEOUT:
                # 僵尸 sending（App 拉到但没来得及 done）→ 重置回 pending 重投
                it["status"] = "pending"
                it.pop("sent_ts", None)
                rest.append(it)
            else:
                rest.append(it)
        elif st == "done" and now - ts > 3600:
            dropped.append((it, "done_aged_1h"))
            continue  # 清理已完成的旧消息 → 归档后清理
        else:
            rest.append(it)
    _save(rest + picked)
    # v3.9.57：归档 + 落日志（此前三条 continue 都是静默丢弃，出事无法查证）
    if dropped:
        _archive(dropped)  # dropped 已是 [(item, reason), ...]，逐条保留原因
        for d, reason in dropped:
            print("[inbox] 消息离开队列 id=%s task=%s 原因=%s 字数=%d（已归档）" % (
                d.get("id"), d.get("source_task_id"), reason, len(d.get("text") or "")), flush=True)
    return picked


def mark_done(mid):
    with _lock:
        items = _load()
        hit = [it for it in items if it.get("id") == mid]
        new = [it for it in items if it.get("id") != mid]
        if len(new) != len(items):
            # v3.9.57：App 确认前先把内容归档——此前是物理删除，App 未存住即永久丢失
            _archive(hit, "mark_done")
            _save(new)
            return True
    return False


# ---- v3.9.83 任务中途追问（question）：AI 发问题卡 → 用户在 App 作答 → AI 取答案继续跑 ----

def answer_question(mid, text):
    """App 作答入口：给队列里那条 question 写 answer / answered_ts，原地落盘。

    铁律（与 mark_done 一致的写法）：走本文件既有的 _load/_save + _lock（RLock 可重入）。
    条目**不删除**——答案要留在队列里等 AI 侧 GET /api/inbox/answer 取走，
    由 AI 侧（ask_user.py）拿到答案后自行调 done 收尾；App 侧不 markDone question。
    返回 True=写入成功；False=条目不存在或参数不合法。
    """
    mid = (mid or "").strip()
    text = (text or "").strip()
    if not mid or not text:
        return False
    with _lock:
        items = _load()
        for it in items:
            if it.get("id") == mid:
                it["answer"] = text
                it["answered_ts"] = time.time()
                _save(items)
                return True
    return False


def read_answer(mid):
    """读某条的答案 → (found, answer)。

    found=False 表示队列里已没有这条（被 done / 被 STALE_TTL 清理 / 从未存在）；
    found=True, answer=None 表示问题还在但用户还没答。
    长轮询用这个区分「等答案」与「问题已消失」。
    """
    mid = (mid or "").strip()
    if not mid:
        return False, None
    for it in _load():
        if it.get("id") == mid:
            ans = it.get("answer")
            return True, (ans if ans else None)
    return False, None


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
                "session_id": it.get("session_id"),
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
        # v3.9.57：CORS 预检必须不经鉴权（返回 401 会让浏览器/客户端预检直接失败，
        # 表现为"接口完全打不通"）。原 do_GET/do_POST 的鉴权逻辑保持不变。
        self.send_response(204)
        self._cors()
        self.end_headers()

    def _answer_wait(self):
        """GET /api/inbox/answer?id=..&wait=N —— 长轮询等答案（给 AI 侧脚本，省得它每秒疯狂打接口）。

        语义：
          - 有答案 → 立刻 {"ok":true,"answered":true,"id":..,"text":..}
          - 没答案 → 最多轮询 wait 秒（上限 30s、1 秒一跳）→ {"ok":true,"answered":false,"id":..}
          - wait 缺省 0（只查一次就返回）；缺 id → {"ok":false,"error":"缺少 id"}
        阻塞说明：本服务是 ThreadingHTTPServer（unified_router.run_server），
        一个请求阻塞的是它自己的线程，不会卡住其它请求。
        """
        try:
            args = parse_qs(urlparse(self.path).query)
        except Exception:
            args = {}
        mid = (args.get("id") or [""])[0].strip()
        if not mid:
            self._send(200, {"ok": False, "error": "缺少 id"})
            return
        try:
            wait = int(float((args.get("wait") or ["0"])[0]))
        except Exception:
            wait = 0
        wait = max(0, min(wait, 30))   # 上限 30s：防 AI 侧写成 wait=600 把连接挂死
        deadline = time.time() + wait
        while True:
            found, ans = read_answer(mid)
            if ans:
                print("[inbox] GET answer 命中 id=%s 字数=%d" % (mid, len(ans)), flush=True)
                self._send(200, {"ok": True, "answered": True, "id": mid, "text": ans})
                return
            if time.time() >= deadline:
                print("[inbox] GET answer 未答 id=%s found=%s wait=%ds" % (mid, found, wait), flush=True)
                self._send(200, {"ok": True, "answered": False, "id": mid})
                return
            time.sleep(1)

    def do_GET(self):
        # GET /api/inbox/answer?id=..&wait=N —— AI 侧长轮询取问题答案（v3.9.83）
        # ⚠️ 必须排在下面 /api/inbox 泛匹配**之前**：否则会被「App 拉待取消息」分支吞掉，
        # 表现为 AI 侧永远拿到 {"ok":true,"items":[...]} 而不是答案。
        if self.path.startswith("/api/inbox/answer"):
            if not self._auth_app():
                print("[inbox] GET answer 401 未授权 path=%s" % self.path[:60], flush=True)
                self._send(401, {"ok": False, "error": "unauthorized"})
                return
            self._answer_wait()
            return
        # GET /api/inbox —— App 轮询拉取待推送消息
        if self.path.startswith("/api/inbox"):
            if not self._auth_app():
                print("[inbox] GET 401 未授权 path=%s" % self.path[:60], flush=True)
                self._send(401, {"ok": False, "error": "unauthorized"})
                return
            items = pop_pending()
            if items:
                print("[inbox] GET 取走 %d 条: %s" % (
                    len(items), ",".join(str(x.get("source_task_id")) for x in items)), flush=True)
            self._send(200, {"ok": True, "items": [{
                "id": it["id"], "text": it["text"], "ts": it.get("ts", 0),
                "source_task_id": it.get("source_task_id"),
                "task_type": it.get("task_type", "reply"),
                "session_id": it.get("session_id")
            } for it in items]})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        # POST /api/inbox/{id}/done —— App 消费后标记已读
        m = re.match(r"^/api/inbox/([0-9a-f]+)/done$", self.path)
        if m:
            if not self._auth_app():
                print("[inbox] POST 401 未授权 done id=%s" % m.group(1), flush=True)
                self._send(401, {"ok": False, "error": "unauthorized"})
                return
            ok = mark_done(m.group(1))
            print("[inbox] POST done id=%s → %s（内容已归档）" % (m.group(1), ok), flush=True)
            self._send(200, {"ok": ok})
            return
        # POST /api/inbox/answer —— App 作答（v3.9.83）：写 answer/answered_ts，条目留在队列等 AI 取
        if self.path.startswith("/api/inbox/answer"):
            if not self._auth_app():
                print("[inbox] POST answer 401 未授权", flush=True)
                self._send(401, {"ok": False, "error": "unauthorized"})
                return
            try:
                n = int(self.headers.get("Content-Length") or 0)
                d = json.loads(self.rfile.read(n) or b"{}")
            except Exception as e:
                self._send(400, {"ok": False, "error": str(e)[:200]})
                return
            mid = str(d.get("id") or "").strip()
            text = str(d.get("text") or "").strip()
            if not mid or not text:
                self._send(200, {"ok": False, "error": "缺少 id 或 text"})
                return
            if not answer_question(mid, text):
                self._send(200, {"ok": False, "error": "未找到该问题"})
                return
            print("[inbox] POST answer id=%s 字数=%d" % (mid, len(text)), flush=True)
            self._send(200, {"ok": True, "id": mid, "answered": True})
            return
        # POST /api/inbox/push —— Hermes 主动推消息
        if self.path.startswith("/api/inbox/push"):
            if not self._auth_hermes():
                self._send(401, {"ok": False, "error": "unauthorized"})
                return
            try:
                n = int(self.headers.get("Content-Length") or 0)
                d = json.loads(self.rfile.read(n) or b"{}")
                want_id = bool(d.get("want_id"))   # v3.9.83：追问链路要 id；老调用方不传 → 响应体不变
                ok, msg = push(d.get("text", ""), d.get("source_task_id"),
                               d.get("task_type", "reply"), want_id=want_id,
                               session_id=d.get("session_id"))
                resp = {"ok": ok, "message": msg}
                if ok and want_id:
                    resp["id"] = msg
                self._send(200, resp)
            except Exception as e:
                self._send(400, {"ok": False, "error": str(e)[:200]})
            return
        self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, *a):
        pass
