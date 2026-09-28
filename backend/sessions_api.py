#!/usr/bin/env python3
"""会话同步 API：/api/sessions/list + /api/sessions/merge
把轻聊的聊天会话同步到 NAS，跨设备（Safari/PWA/多设备）共享同一份数据。

数据存储：{DATA_DIR}/sessions/sessions.json
鉴权：所有请求需携带 X-Sessions-Password header（与文件管理同 QL_PASSWORD 密码）
"""
import http.server
import json
import os
import threading
DATA_DIR = os.environ.get("QL_DATA_DIR", "/volume1/docker/hermes/微信文件/轻聊web/data")
import time
import hmac
import media_convert  # v2.0.130: 历史消息 MEDIA:路径→data URL 图片

# v3.9.71 delivery: 固定投递会话——cron/system 投递详情落这里（App 任何版本可见，不可删除）
DELIVERY_SESSION_ID = "qingliao_delivery"
DELIVERY_SESSION_TITLE = "轻聊投递"



# v2.0.116 review：并发保存锁（多设备 merge 写覆盖丢数据）
_save_lock = threading.RLock()   # v3.9.71: RLock——append_delivery_message 持锁调用 save_sessions(内部同锁)

# 访问密码（与 files_api.py 保持一致）
# BE4：默认改空串——"change-me" 是公开仓库里的常量（密码兜底本身默认关闭，不留弱口令）
SESSIONS_PASSWORD = os.environ.get("QL_PASSWORD", "")

# 数据目录（root 运行，可写）：默认路径，可用 POST /api/sessions/location 修改（持久化到 LOC_FILE）
LOC_FILE = os.path.join(DATA_DIR, "sessions_loc.json")

def _dir_writable(p):
    """真实写入探测：只读挂载（EROFS）在 root 下 os.access 也会骗人，必须试写一次。"""
    probe = os.path.join(p, '.sessions_write_probe')
    try:
        with open(probe, 'w') as _f:
            _f.write('')
        os.remove(probe)
        return True
    except OSError:
        return False


def _data_dir():
    fallback = os.environ.get('SESSIONS_DATA_DIR', os.path.join(DATA_DIR, 'sessions'))
    try:
        with open(LOC_FILE, 'r', encoding='utf-8') as f:
            p = (json.load(f) or {}).get('path', '')
        if p and os.path.isdir(p):
            if _dir_writable(p):
                return p
            # 位置覆盖指向不可写目录时不能硬用：否则每次 merge/save 都 500（客户端表现为
            # 「删除失败 服务器错误(500)」）。典型成因：容器把 /volume1 以 :ro 挂入，而覆盖
            # 指向该卷下的历史目录（如 微信文件/轻聊app/sessions）。
            print('[sessions] 位置覆盖不可写，回退默认目录: %s -> %s' % (p, fallback), flush=True)
    except Exception:
        pass
    return fallback

def _data_file():
    return os.path.join(_data_dir(), 'sessions.json')

def _tmp_file():
    return os.path.join(_data_dir(), 'sessions.json.tmp')


def load_sessions():
    """读取全量会话，文件不存在或损坏时返回 []"""
    try:
        with open(_data_file(), 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
    except (OSError, json.JSONDecodeError):
        pass
    return []


def save_sessions(sessions):
    """原子写入：先写 tmp 再 rename，防止写一半损坏"""
    # v2.0.116 review：加锁防并发合并写覆盖（多设备同时 merge 丢数据）
    with _save_lock:
        os.makedirs(_data_dir(), exist_ok=True)
        with open(_tmp_file(), 'w', encoding='utf-8') as f:
            json.dump(sessions, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(_tmp_file(), _data_file())


def ensure_delivery_session():
    """确保固定投递会话存在，返回 (全量列表, 会话dict)。调用方负责 save_sessions。"""
    sessions = load_sessions()
    for s in sessions:
        if isinstance(s, dict) and s.get("id") == DELIVERY_SESSION_ID:
            if s.get("title") != DELIVERY_SESSION_TITLE:
                s["title"] = DELIVERY_SESSION_TITLE
            return sessions, s
    s = {"id": DELIVERY_SESSION_ID, "title": DELIVERY_SESSION_TITLE,
         "messages": [], "createdAt": int(time.time() * 1000), "updatedAt": 0}
    sessions.insert(0, s)
    return sessions, s

def append_delivery_message(text, task_type="cron"):
    """向固定会话追加一条 assistant 消息并落盘。失败只打日志，绝不影响投递主流程。"""
    try:
        with _save_lock:
            sessions, sess = ensure_delivery_session()
            ts_ms = int(time.time() * 1000)
            sess.setdefault("messages", []).append({
                "role": "assistant", "content": text,
                "timestamp": ts_ms, "isPush": True})
            sess["updatedAt"] = ts_ms
            save_sessions(sessions)
        return True
    except Exception as e:
        print('[delivery] 写入固定会话失败: %s' % e, flush=True)
        return False


def merge_sessions(local, incoming, deleted):
    """合并策略：
    - incoming 中 NAS 没有的 -> 新增
    - 同 id 的 -> 取 updatedAt 较新的（incoming 较新则覆盖）
    - deleted 中的 id -> 删除
    例外（固定投递会话 qingliao_delivery）：不可删 + 标题锁定，但**消息内容以客户端为准**
    （客户端能删单条/清空，否则投递详情越积越多）——详见下方 v3.9.72 注释。
    返回合并后的完整列表
    """
    merged = []
    by_id = {}
    for s in local:
        if isinstance(s, dict) and s.get('id'):
            by_id[s['id']] = s
    # v3.9.71 delivery: 固定会话保护——「轻聊投递」(qingliao_delivery) 不可被删除，
    # deleted 列表里的该 id 直接忽略（App/PWA/任何客户端的删除请求都拦在后端这一层）
    _PROTECTED_IDS = {"qingliao_delivery"}
    _DELIVERY_INCOMING = {}   # v3.9.72: 客户端发来的固定会话（内容以客户端为准，见下）
    for s in incoming:
        if not isinstance(s, dict) or not s.get('id'):
            continue
        sid = s['id']
        if sid in _PROTECTED_IDS:
            _DELIVERY_INCOMING[sid] = s
        if sid in by_id:
            cur = by_id[sid]
            # 以 updatedAt 较新者为准
            if (s.get('updatedAt') or 0) >= (cur.get('updatedAt') or 0):
                by_id[sid] = s
        else:
            by_id[sid] = s
    for sid in (deleted or []):
        if sid in _PROTECTED_IDS:
            print('[sessions] 拒绝删除固定会话: %s' % sid, flush=True)
            continue
        by_id.pop(sid, None)
    # v3.9.72 delivery: 固定会话「会话不可删，但内容可删」——客户端发来的该会话消息一律以客户端为准。
    # 起因（用户反馈「轻聊投递里删不掉投递内容，日积月累太多」）：App 写会话只发 id/title/messages，
    # 不发 updatedAt（恒 0），而 NAS 侧 append_delivery_message 每次投递都把该会话 updatedAt 抬到
    # 当前时间（非 0）→ 上面「updatedAt 较新者为准」恒判 incoming 旧 → 删除被静默丢弃，客户端刷新后
    # 投递内容原样回来（表现就是「删了又复活」，且越积越多）。这里对该 id 特判：
    #   · incoming 带 messages（list，含空数组=清空本会话）→ 直接采用（删单条/清空都能落库）
    #   · incoming 不带 messages 键（纯改名等）→ 保留 NAS 消息，防误清
    #   · 仍锁 title；updatedAt 沿用 NAS 的（避免该会话在列表里掉到底部）
    for _sid in _PROTECTED_IDS:
        _inc = _DELIVERY_INCOMING.get(_sid)
        if not _inc:
            continue
        _cur = by_id.get(_sid) or {}
        _new = dict(_inc)
        _new["id"] = _sid
        _new["title"] = DELIVERY_SESSION_TITLE
        if not isinstance(_new.get("messages"), list):
            _new["messages"] = list(_cur.get("messages") or [])
        _new["updatedAt"] = _inc.get("updatedAt") or _cur.get("updatedAt") or int(time.time() * 1000)
        if _cur.get("createdAt") and not _new.get("createdAt"):
            _new["createdAt"] = _cur["createdAt"]
        by_id[_sid] = _new
        print('[sessions] 固定会话内容以客户端为准: %s msgs=%d' % (_sid, len(_new["messages"])), flush=True)
    # v3.9.71: 固定会话标题锁定（改名也被还原）
    for _sid in _PROTECTED_IDS:
        if _sid in by_id:
            by_id[_sid]["title"] = DELIVERY_SESSION_TITLE
    # 按 updatedAt 倒序
    merged = list(by_id.values())
    merged.sort(key=lambda s: s.get('updatedAt') or 0, reverse=True)
    return merged


class SessionsHandler(http.server.BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Sessions-Password, X-Auth-Token")

    def _check_auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, 'X-Sessions-Password', SESSIONS_PASSWORD)

    def _send_json(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        try:
            length = int(self.headers.get('Content-Length', 0))
        except (TypeError, ValueError):
            length = 0
        if length <= 0:
            return None
        try:
            return json.loads(self.rfile.read(length).decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._check_auth():
            self._send_json(401, {"error": "需要密码"})
            return
        if self.path.startswith('/api/sessions/location'):
            self._send_json(200, {"ok": True, "path": _data_dir()})
            return
        if self.path.startswith('/api/sessions/list'):
            sessions = load_sessions()
            for _s in sessions:
                for _m in (_s.get("messages") or []):
                    if _m.get("role") == "assistant" and isinstance(_m.get("content"), str) and "MEDIA:" in _m.get("content", ""):
                        _m["content"] = media_convert.convert_media_marks(_m["content"])

            self._send_json(200, {"ok": True, "sessions": sessions, "total": len(sessions)})
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if not self._check_auth():
            self._send_json(401, {"error": "需要密码"})
            return
        # 会话存储位置设置（持久化到 LOC_FILE，重启不丢）
        if self.path.startswith('/api/sessions/location'):
            body = self._read_body()
            p = ((body or {}).get('path') or '').strip()
            if not p:
                self._send_json(400, {"error": "path 必填"})
                return
            if not os.path.isdir(p):
                self._send_json(400, {"error": "目录不存在: " + p})
                return
            try:
                with open(LOC_FILE, 'w', encoding='utf-8') as f:
                    json.dump({"path": p}, f, ensure_ascii=False)
            except OSError as e:
                self._send_json(500, {"error": "写入配置失败: " + str(e)[:100]})
                return
            self._send_json(200, {"ok": True, "path": p, "note": "会话将存储到新位置（下次写入生效）"})
            return
        if self.path.startswith('/api/sessions/merge'):
            body = self._read_body()
            if body is None or not isinstance(body, dict):
                self._send_json(400, {"error": "无效的请求体，需要 {sessions, deleted}"})
                return
            incoming = body.get('sessions') or []
            deleted = body.get('deleted') or []
            current = load_sessions()
            merged = merge_sessions(current, incoming, deleted)
            save_sessions(merged)
            self._send_json(200, {"ok": True, "saved": len(incoming), "deleted": len(deleted), "total": len(merged)})
            return
        if self.path.startswith('/api/sessions/search'):
            body = self._read_body() or {}
            q = ((body.get('q') or '').strip())
            if not q:
                self._send_json(400, {"error": "q 必填"})
                return
            ql = q.lower()
            sessions = load_sessions()
            results = []
            for s in sessions:
                title = (s.get('title') or '')
                msgs = s.get('messages') or []
                hits = []
                for m in msgs:
                    c = m.get('content')
                    if isinstance(c, str) and ql in c.lower():
                        idx = c.lower().find(ql)
                        start = max(0, idx - 30)
                        end = min(len(c), idx + len(q) + 60)
                        snippet = ('…' if start > 0 else '') + c[start:end] + ('…' if end < len(c) else '')
                        hits.append({'role': m.get('role'), 'snippet': snippet,
                                     'content': c[:2000]})
                        if len(hits) >= 3:
                            break
                if ql in title.lower() or hits:
                    results.append({
                        'id': s.get('id'),
                        'title': title,
                        'lastTime': s.get('lastTime'),
                        'hits': hits,
                        'hitCount': len(hits),
                    })
            self._send_json(200, {"ok": True, "results": results, "total": len(results)})
            return
        # 兼容旧格式：POST /api/sessions 直接传数组（老 exportToNas 用）
        if self.path.startswith('/api/sessions'):
            body = self._read_body()
            if body is None or not isinstance(body, list):
                self._send_json(400, {"error": "无效的请求体，需要会话数组"})
                return
            current = load_sessions()
            merged = merge_sessions(current, body, [])
            save_sessions(merged)
            self._send_json(200, {"ok": True, "saved": len(body), "total": len(merged)})
            return
        self._send_json(404, {"error": "not found"})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    os.makedirs(_data_dir(), exist_ok=True)
    server = http.server.ThreadingHTTPServer(("0.0.0.0", 9131), SessionsHandler)
    print("Sessions API listening on :9131")
    server.serve_forever()
