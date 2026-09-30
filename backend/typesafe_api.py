#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""TypeSafe（System One / Jev）判定 API —— 轻聊最小接入（2026-09-21）

TypeSafe 把「自然语言 → 带类型的判断 + 概率」做成可编程原语：一次请求给一个 state
加一组 typed questions（noul 是/否、choice 单选、score 打分），返回结构化 answers，
代码再拿判断去路由/排序/拦截，不用自己 prompt+解析。

接口（都走 9127，鉴权同其它模块：X-Auth-Token）
  POST /api/typesafe/judge         内网直连
  POST /api/agent/typesafe/judge   App 主链路别名（借 /api/agent 前缀：lucky 白名单 +
                                   relay ALLOWED_RELAY 均已放行 → 零 nginx/relay 改动）
  GET  /api/typesafe/health        配置状态（不回显 key；含 routing + breaker）
  GET  /api/(agent/)typesafe/routing   读路由开关与熔断状态
  POST /api/(agent/)typesafe/routing   写路由开关：{"enabled":false} / {"mode":"smart"} /
                                       {"threshold":0.7} / {"reset_breaker":true}
                                       （改配置免重启，App 设置里的开关走这个接口）

请求体
  {"state": <string|object|array>,  必填：要判定的内容（用户原话/会话片段/任意状态）
   "preset": "chat_router",         可选：内置判定模板（与 questions 二选一，preset 优先）
   "questions": {name: {...}},      可选：TypeSafe 原生 typed questions
   "model": "jev-latest"}           可选

响应
  {"ok":true,"model":"jev-1.13.0","answers":{...},"usage":{...},"ms":420,"preset":"chat_router"}
  上游失败：HTTP 502 + {"ok":false,"error":...,"upstream_status":N}——失败不降级成假判断。

配置：<data>/typesafe_config.json = {"api_key","base_url","model","routing":{...}}（600 权限）
会话路由（v3.9.55）：stream_api 在「关键词规则未命中」时调 route()，判定 needs_action
  → False=纯聊天契约 / True=Agent 工具契约；判定失败/超时一律回退现状（fail-open）。
  熔断（v3.9.56）：连续 breaker_fails（默认 3）次失败/超时 → 停判定 breaker_cooldown_s
  （默认 300s），期间 route() 立即返回 ok=False 不碰上游；到点自动半开重试。
维护：python3 typesafe_api.py show | keyfile <json路径> | judge <preset> "<文本>" | route "<文本>"
      | routing [enabled=true|false mode=smart threshold=0.7 reset_breaker=true ...]
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler

BASE_URL_DEFAULT = "https://api.typesafe.ai/v1"
MODEL_DEFAULT = "jev-latest"
TIMEOUT = 30
MAX_STATE_CHARS = 20000
MAX_QUESTIONS = 20
VALID_TYPES = ("noul", "choice", "score")
CONFIG_NAME = "typesafe_config.json"

# 轻聊惯例：QL_DATA_DIR 为空时落 轻聊web/data（容器内 root 可写，持久化）
_CANDIDATE_DIRS = [
    os.environ.get("QL_DATA_DIR") or "",
    "/volume1/docker/hermes/微信文件/轻聊web/data",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, "data"),
]

# 内置判定模板（只放真用得上的：加太杂不如让调用方直接传 questions）
PRESETS = {
    "chat_router": {
        "desc": "轻聊会话路由：要不要干活 / 急不急 / 该怎么回",
        "questions": {
            "needs_action": {
                "type": "noul",
                "instructions": "用户这条消息是否要求「实际做事」——查数据、操作设备或文件、跑命令、生成文件、"
                                "调用工具取外部信息——而不是纯聊天、闲聊或问常识？",
                "criteria": {
                    "true": "必须执行操作或取外部数据才能正确回答",
                    "false": "纯聊天/常识问答，不需要动手",
                },
            },
            "urgency": {
                "type": "noul",
                "instructions": "用户是否表达了紧急、催促、被卡住或不满（等待已久、反复失败、影响正在做的事）？",
                "criteria": {"true": "明确紧急或带情绪", "false": "平和陈述"},
            },
            "reply_mode": {
                "type": "choice",
                "instructions": "这条消息最合适的回复方式是什么？",
                "criteria": {
                    "direct": "已经能直接答，简短给出结论",
                    "clarify": "信息不足或指代不明，应先提一个澄清问题",
                    "execute": "需要实际执行操作或取数据，进入 Agent 执行",
                },
            },
        },
    },
}


# ─────────────────────────── 配置 ───────────────────────────

def data_dir():
    for d in _CANDIDATE_DIRS:
        if d and os.path.isdir(d):
            return os.path.abspath(d)
    return _CANDIDATE_DIRS[1]


def config_path():
    return os.path.join(data_dir(), CONFIG_NAME)


def load_config():
    """每次读盘（文件极小）——改 key 不用重启容器。"""
    try:
        with open(config_path(), encoding="utf-8") as f:
            cfg = json.load(f)
        return cfg if isinstance(cfg, dict) else {}
    except Exception:
        return {}


def save_config(api_key, base_url=None, model=None):
    cfg = load_config()
    cfg["api_key"] = (api_key or "").strip()
    if base_url:
        cfg["base_url"] = base_url.strip()
    if model:
        cfg["model"] = model.strip()
    cfg.setdefault("base_url", BASE_URL_DEFAULT)
    cfg.setdefault("model", MODEL_DEFAULT)
    p = config_path()
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=1)
    os.chmod(tmp, 0o600)
    os.replace(tmp, p)
    return cfg


def _mask(key):
    k = key or ""
    return (k[:10] + "…" + k[-4:]) if len(k) > 18 else ("已配置" if k else "未配置")


def config_status():
    cfg = load_config()
    key = cfg.get("api_key") or ""
    return {
        "configured": bool(key),
        "api_key": _mask(key),
        "base_url": cfg.get("base_url") or BASE_URL_DEFAULT,
        "model": cfg.get("model") or MODEL_DEFAULT,
        "config_path": config_path(),
        "presets": sorted(PRESETS),
        "routing": routing_cfg(),
        "breaker": breaker_status(),
    }


# ─────────────────────────── 会话路由（轻聊 stream_api 调用） ───────────────────────────
# v3.9.55：把「这条消息要不要干活」做成判定原语给会话流调用。
# 只判一件事（needs_action），其余字段（urgency / reply_mode）留作诊断与后续扩展。
ROUTING_DEFAULT = {
    "enabled": True,          # False = 完全不判定（等价回退现状）
    "mode": "smart",          # smart=按判定分流 / off=判定关 / force_agent=恒走 Agent
    "timeout_ms": 1200,       # 判定超时（超时=回退现状，不让用户等判定）
    "max_chars": 120,         # 文本超长跳过判定（长正文/附件多为真任务）
    "threshold": 0.6,         # needs_action 概率 ≥ 阈值 → 判「要干活」
    "breaker_fails": 3,       # 连续失败 N 次 → 熔断（0 = 关掉熔断）
    "breaker_cooldown_s": 300,  # 熔断持续秒数，到点自动半开重试（失败则再次熔断）
}

# ─── 熔断状态（进程内，key 失效/上游挂掉时不再每条消息白等一次判定）───
_BREAKER = {"fails": 0, "openedAt": 0.0, "until": 0.0, "lastError": "", "trips": 0}
_BREAKER_LOCK = threading.Lock()


def routing_cfg():
    """路由配置（typesafe_config.json 的 "routing" 段，缺省用 ROUTING_DEFAULT）。"""
    raw = load_config().get("routing")
    out = dict(ROUTING_DEFAULT)
    if isinstance(raw, dict):
        for k in ROUTING_DEFAULT:
            if k in raw and raw[k] is not None:
                out[k] = raw[k]
    return out


# ─── 熔断（v3.9.56）───

def _breaker_limits(cfg):
    """从配置取 (连续失败阈值, 熔断秒数)。阈值 0 = 不熔断。"""
    try:
        fails = int(cfg.get("breaker_fails", ROUTING_DEFAULT["breaker_fails"]))
    except Exception:
        fails = ROUTING_DEFAULT["breaker_fails"]
    try:
        cd = float(cfg.get("breaker_cooldown_s", ROUTING_DEFAULT["breaker_cooldown_s"]))
    except Exception:
        cd = float(ROUTING_DEFAULT["breaker_cooldown_s"])
    return max(0, fails), max(0.0, cd)


def _breaker_open():
    """熔断中（到点自动半开：下一次调用会真打上游，成功即恢复、失败再熔断）。"""
    now = time.time()
    with _BREAKER_LOCK:
        return now < _BREAKER["until"]


def _breaker_record(ok, error="", fails_limit=3, cooldown_s=300.0):
    """记录一次判定结果。返回 (本次是否刚触发熔断, 当前连续失败数)。"""
    now = time.time()
    tripped = False
    with _BREAKER_LOCK:
        if ok:
            _BREAKER["fails"] = 0
            _BREAKER["until"] = 0.0
            _BREAKER["openedAt"] = 0.0
            _BREAKER["lastError"] = ""
        else:
            _BREAKER["fails"] = int(_BREAKER["fails"]) + 1
            _BREAKER["lastError"] = (error or "")[:160]
            if fails_limit > 0 and _BREAKER["fails"] >= fails_limit:
                _BREAKER["until"] = now + cooldown_s
                _BREAKER["openedAt"] = now
                _BREAKER["trips"] = int(_BREAKER["trips"]) + 1
                _BREAKER["fails"] = 0
                tripped = True
        fails_now = int(_BREAKER["fails"])
    return tripped, fails_now


def reset_breaker():
    with _BREAKER_LOCK:
        _BREAKER["fails"] = 0
        _BREAKER["until"] = 0.0
        _BREAKER["openedAt"] = 0.0
        _BREAKER["lastError"] = ""
    return breaker_status()


def breaker_status():
    """熔断状态（诊断用，进 health / routing 接口）。"""
    now = time.time()
    with _BREAKER_LOCK:
        b = dict(_BREAKER)
    remain = max(0, int(round(float(b["until"]) - now)))
    return {
        "open": remain > 0,
        "remain_s": remain,
        "fails": int(b["fails"]),
        "trips": int(b["trips"]),
        "since_s": int(round(now - float(b["openedAt"]))) if b["openedAt"] else 0,
        "last_error": b["lastError"],
    }


# ─── 路由配置写入（App 开关 / 运维改配置都走这里）───

ROUTING_HINTS = {
    "timeout_ms": (100, 10000),
    "max_chars": (10, 5000),
    "breaker_fails": (0, 100),
    "breaker_cooldown_s": (0, 86400),
}


def _coerce_routing(name, v):
    if name == "enabled":
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        if s in ("true", "1", "on", "yes"):
            return True
        if s in ("false", "0", "off", "no"):
            return False
        raise ValueError("enabled 只能是 true / false")
    if name == "mode":
        m = str(v or "").strip().lower()
        if m not in ("smart", "off", "force_agent"):
            raise ValueError("mode 只能是 smart / off / force_agent")
        return m
    if name in ROUTING_HINTS:
        try:
            n = int(v)
        except Exception:
            raise ValueError("%s 必须是整数" % name)
        lo, hi = ROUTING_HINTS[name]
        if not lo <= n <= hi:
            raise ValueError("%s 需在 %d~%d 之间" % (name, lo, hi))
        return n
    if name == "threshold":
        try:
            f = float(v)
        except Exception:
            raise ValueError("threshold 必须是 0~1 之间的小数")
        if not 0.0 <= f <= 1.0:
            raise ValueError("threshold 需在 0~1 之间")
        return f
    raise ValueError("未知字段：%s" % name)


def _atomic_write_json(path, obj, mode=0o600):
    """原子写 JSON，保留原文件属主与权限（mkstemp/replace 不可只沿 mode）。"""
    st = None
    try:
        st = os.stat(path)
    except OSError:
        pass
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)
        f.flush()
        os.fsync(f.fileno())
    os.chmod(tmp, (st.st_mode & 0o777) if st else mode)
    os.replace(tmp, path)
    if st:
        try:
            os.chown(path, st.st_uid, st.st_gid)
        except OSError:
            pass


def save_routing(patch):
    """更新 routing 段（保留 api_key 等其它字段 + 文件属主/权限）。校验失败抛 ValueError。"""
    if not isinstance(patch, dict) or not patch:
        raise ValueError("需至少一个 routing 字段：%s" % ", ".join(sorted(ROUTING_DEFAULT)))
    unknown = [k for k in patch if k not in ROUTING_DEFAULT]
    if unknown:
        raise ValueError("未知字段：%s（可用：%s）" % (", ".join(sorted(unknown)),
                                                   ", ".join(sorted(ROUTING_DEFAULT))))
    cur = routing_cfg()
    for k, v in patch.items():
        cur[k] = _coerce_routing(k, v)
    cfg = load_config()
    cfg["routing"] = cur
    _atomic_write_json(config_path(), cfg)
    print("[typesafe-routing] 已更新 %s → %s" % (sorted(patch), cur), flush=True)
    return cur


def route(text, timeout_ms=None, cfg=None):
    """会话路由判定：{"ok", "needs_action", "needs_action_prob", "urgency_prob",
    "reply_mode", "model", "ms"}。任何异常/超时 → ok=False，调用方按现状处理（fail-open）。
    熔断中直接返回 ok=False + breaker=True（不碰上游、不白等）。"""
    c = cfg if isinstance(cfg, dict) else routing_cfg()
    t = (text or "").strip()
    if not t:
        return {"ok": False, "error": "空文本"}
    fails_limit, cooldown_s = _breaker_limits(c)
    if fails_limit > 0 and _breaker_open():
        b = breaker_status()
        return {"ok": False, "breaker": True, "remain_s": b["remain_s"],
                "error": "判定熔断中（连续 %d 次失败，%ds 后自动恢复）" % (fails_limit, b["remain_s"])}
    timeout = float(timeout_ms or c.get("timeout_ms") or ROUTING_DEFAULT["timeout_ms"]) / 1000.0
    try:
        t0 = time.time()
        data = _call(t[:MAX_STATE_CHARS], PRESETS["chat_router"]["questions"], timeout=timeout)
        ms = int((time.time() - t0) * 1000)
    except Exception as e:
        err = "%s: %s" % (type(e).__name__, str(e)[:160])
        tripped, fails_now = _breaker_record(False, err, fails_limit, cooldown_s)
        if tripped:
            print("[typesafe-route] 熔断开启：连续 %d 次判定失败，暂停判定 %d 秒（原因：%s）"
                  % (fails_limit, int(cooldown_s), err), flush=True)
        return {"ok": False, "error": err, "breaker": bool(tripped or _breaker_open()),
                "fails": fails_now}
    ans = data.get("answers") or {}
    na = ans.get("needs_action") or {}
    prob = na.get("noul")
    try:
        threshold = float(c.get("threshold") or ROUTING_DEFAULT["threshold"])
    except Exception:
        threshold = ROUTING_DEFAULT["threshold"]
    needs = None if prob is None else (float(prob) >= threshold)
    out = {
        "ok": True,
        "needs_action": bool(needs),
        "needs_action_prob": prob,
        "threshold": threshold,
        "urgency_prob": (ans.get("urgency") or {}).get("noul"),
        "reply_mode": (ans.get("reply_mode") or {}).get("choice"),
        "reply_mode_prob": (ans.get("reply_mode") or {}).get("confidence"),
        "model": data.get("model"),
        "ms": ms,
    }
    _breaker_record(True, "", fails_limit, cooldown_s)
    print("[typesafe-route] needs_action=%s p=%s mode=%s ms=%d" %
          (out["needs_action"], prob, out["reply_mode"], ms), flush=True)
    return out


# ─────────────────────────── 判定 ───────────────────────────

def pick_questions(body, preset_override=None):
    """返回 (preset_name_or_None, questions)。校验失败抛 ValueError。"""
    preset = (preset_override or body.get("preset") or "").strip()
    if preset:
        p = PRESETS.get(preset)
        if not p:
            raise ValueError("未知 preset：%s（可用：%s）" % (preset, ", ".join(sorted(PRESETS))))
        return preset, json.loads(json.dumps(p["questions"]))
    qs = body.get("questions")
    if not isinstance(qs, dict) or not qs:
        raise ValueError("缺少判定内容：传 preset 或 questions 之一")
    if len(qs) > MAX_QUESTIONS:
        raise ValueError("questions 最多 %d 条" % MAX_QUESTIONS)
    for name, q in qs.items():
        if not isinstance(q, dict):
            raise ValueError("question %s 必须是对象" % name)
        if q.get("type") not in VALID_TYPES:
            raise ValueError("question %s 的 type 必须是 %s" % (name, "/".join(VALID_TYPES)))
        if "instructions" not in q:
            raise ValueError("question %s 缺 instructions" % name)
        if q["type"] == "choice" and not isinstance(q.get("criteria"), dict):
            raise ValueError("choice 的 criteria 必须是 {选项: 说明} 映射")
        if q["type"] == "score" and not (isinstance(q.get("criteria"), list) and len(q["criteria"]) >= 2):
            raise ValueError("score 的 criteria 至少 2 档（数组）")
    return None, qs


def _call(state, questions, model=None, timeout=TIMEOUT):
    cfg = load_config()
    key = (cfg.get("api_key") or os.environ.get("QL_TYPESAFE_API_KEY") or "").strip()
    if not key:
        raise RuntimeError("未配置 TypeSafe key：%s" % config_path())
    url = (cfg.get("base_url") or BASE_URL_DEFAULT).rstrip("/") + "/systemone"
    payload = json.dumps({
        "state": state,
        "model": model or cfg.get("model") or MODEL_DEFAULT,
        "questions": questions,
    }, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Authorization", "Bearer " + key)
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", "replace")
    data = json.loads(body)
    if not isinstance(data, dict):
        raise RuntimeError("上游返回非对象：%s" % body[:200])
    return data


def judge(body, preset_override=None):
    """核心流程：校验 → 调 TypeSafe → 组装响应。异常按类型抛给调用方。"""
    state = body.get("state")
    if isinstance(state, str):
        if not state.strip():
            raise ValueError("state 不能为空字符串")
        size = len(state)
    elif isinstance(state, (dict, list)):
        if not state:
            raise ValueError("state 不能为空")
        size = len(json.dumps(state, ensure_ascii=False))
    else:
        raise ValueError("state 必须是字符串/对象/数组")
    if size > MAX_STATE_CHARS:
        raise ValueError("state 过长（%d 字符 > %d）" % (size, MAX_STATE_CHARS))

    preset, questions = pick_questions(body, preset_override)
    t0 = time.time()
    data = _call(state, questions, body.get("model"))
    ms = int((time.time() - t0) * 1000)
    usage = data.get("usage") or {}
    print("[typesafe] preset=%s questions=%d ms=%d in=%s out=%s"
          % (preset or "-", len(questions), ms, usage.get("input_tokens"), usage.get("output_tokens")),
          flush=True)
    out = {
        "ok": True,
        "model": data.get("model"),
        "answers": data.get("answers") or {},
        "usage": usage,
        "ms": ms,
    }
    if preset:
        out["preset"] = preset
    return out


# ─────────────────────────── HTTP ───────────────────────────

JUDGE_PATHS = ("/api/typesafe/judge", "/api/agent/typesafe/judge")
HEALTH_PATHS = ("/api/typesafe/health", "/api/agent/typesafe/health")
ROUTING_PATHS = ("/api/typesafe/routing", "/api/agent/typesafe/routing")


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token")

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _path(self):
        return self.path.split("?", 1)[0].rstrip("/") or "/"

    def _check_auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, "X-Typesafe-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        p = self._path()
        if p in HEALTH_PATHS:
            self._send(200, {"ok": True, **config_status()})
            return
        if p in ROUTING_PATHS:
            self._send(200, {"ok": True, "routing": routing_cfg(), "breaker": breaker_status(),
                             "restart_needed": False})
            return
        self._send(404, {"error": "Not Found"})

    def do_POST(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        p = self._path()
        if p not in JUDGE_PATHS and p not in ROUTING_PATHS:
            self._send(404, {"error": "Not Found"})
            return
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            n = 0
        raw = self.rfile.read(n) if n > 0 else b""
        try:
            body = json.loads(raw.decode("utf-8", "replace") or "{}")
        except Exception as e:
            self._send(400, {"ok": False, "error": "请求体不是合法 JSON：%s" % str(e)[:120]})
            return
        if not isinstance(body, dict):
            self._send(400, {"ok": False, "error": "请求体必须是对象"})
            return
        if p in ROUTING_PATHS:
            # 写开关/改配置：{"enabled":false} / {"mode":"smart"} / {"reset_breaker":true}
            try:
                patch = dict(body)
                reset = bool(patch.pop("reset_breaker", False))
                if reset:
                    reset_breaker()
                if not patch and not reset:
                    raise ValueError("需至少一个 routing 字段：%s（或 reset_breaker=true）"
                                     % ", ".join(sorted(ROUTING_DEFAULT)))
                cfg = save_routing(patch) if patch else routing_cfg()
            except ValueError as e:
                self._send(400, {"ok": False, "error": str(e)})
                return
            except Exception as e:
                self._send(500, {"ok": False, "error": "%s: %s" % (type(e).__name__, str(e)[:200])})
                return
            self._send(200, {"ok": True, "routing": cfg, "breaker": breaker_status(),
                             "restart_needed": False})
            return
        try:
            self._send(200, judge(body))
        except ValueError as e:
            self._send(400, {"ok": False, "error": str(e)})
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8", "replace")[:400]
            except Exception:
                pass
            self._send(502, {"ok": False, "error": "TypeSafe 上游 HTTP %s" % e.code,
                             "upstream_status": e.code, "upstream": detail})
        except Exception as e:
            self._send(502, {"ok": False, "error": "%s: %s" % (type(e).__name__, str(e)[:200])})

    def log_message(self, fmt, *args):
        pass


# ─────────────────────────── CLI（运维用，不参与服务） ───────────────────────────

def _main(argv):
    if not argv or argv[0] == "show":
        print(json.dumps(config_status(), ensure_ascii=False, indent=2))
        return 0
    if argv[0] == "presets":
        for name, p in PRESETS.items():
            print("%s：%s" % (name, p.get("desc", "")))
            for qn, q in p["questions"].items():
                print("   - %s (%s)" % (qn, q["type"]))
        return 0
    if argv[0] == "keyfile" and len(argv) >= 2:
        with open(argv[1], encoding="utf-8") as f:
            src = json.load(f)
        cfg = save_config(src.get("api_key") or src.get("key") or "",
                          src.get("base_url"), src.get("model"))
        print(json.dumps({"ok": True, **config_status()}, ensure_ascii=False))
        return 0
    if argv[0] == "set" and len(argv) >= 2:
        save_config(argv[1], argv[2] if len(argv) > 2 else None, argv[3] if len(argv) > 3 else None)
        print(json.dumps(config_status(), ensure_ascii=False))
        return 0
    if argv[0] == "routing":
        if len(argv) == 1:
            print(json.dumps({"routing": routing_cfg(), "breaker": breaker_status()},
                             ensure_ascii=False, indent=2))
            return 0
        patch = {}
        for a in argv[1:]:
            if "=" not in a:
                print("用法：routing [k=v ...]，k ∈ %s，另有 reset_breaker=true"
                      % ", ".join(sorted(ROUTING_DEFAULT)))
                return 2
            k, v = a.split("=", 1)
            patch[k.strip()] = v.strip()
        try:
            if str(patch.pop("reset_breaker", "")).lower() in ("1", "true", "yes", "on"):
                reset_breaker()
            if patch:
                save_routing(patch)
        except ValueError as e:
            print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
            return 1
        print(json.dumps({"ok": True, "routing": routing_cfg(), "breaker": breaker_status()},
                         ensure_ascii=False, indent=2))
        return 0
    if argv[0] == "judge" and len(argv) >= 3:
        body = {"preset": argv[1], "state": argv[2]}
        try:
            print(json.dumps(judge(body), ensure_ascii=False, indent=2))
        except Exception as e:
            print(json.dumps({"ok": False, "error": "%s: %s" % (type(e).__name__, e)},
                             ensure_ascii=False))
            return 1
        return 0
    if argv[0] == "route" and len(argv) >= 2:
        print(json.dumps(route(argv[1]), ensure_ascii=False, indent=2))
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
