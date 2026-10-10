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
                                       {"threshold":0.7} / {"reset_breaker":true} / {"backend":"custom"}
                                       （改配置免重启，App 设置里的开关走这个接口）
  GET  /api/(agent/)typesafe/model     读判定模型配置（自定义模型；api_key 只回掩码，永不回明文）
  POST /api/(agent/)typesafe/model     写判定模型：{"base_url":..,"model":..,"api_key":..}
                                       （api_key 省略=不改、传空串=清空）/ {"test":true} 真调用两条样例

请求体
  {"state": <string|object|array>,  必填：要判定的内容（用户原话/会话片段/任意状态）
   "preset": "chat_router",         可选：内置判定模板（与 questions 二选一，preset 优先）
   "questions": {name: {...}},      可选：TypeSafe 原生 typed questions
   "model": "jev-latest"}           可选

响应
  {"ok":true,"model":"jev-1.13.0","answers":{...},"usage":{...},"ms":420,"preset":"chat_router"}
  上游失败：HTTP 502 + {"ok":false,"error":...,"upstream_status":N}——失败不降级成假判断。

配置：<data>/typesafe_config.json = {"api_key","base_url","model","routing":{...},"custom":{...}}（600 权限）
判定后端（routing.backend，v4.0.87）：
  typesafe = 云端 Jev（默认）；custom = 用户自带 key 的 OpenAI 兼容模型（智谱/OpenRouter/硅基流动…，
  接口地址 + 模型名 + API Key 在 App「设置 → 智能路由 → 判定模型」里填，key 存后端、接口只回掩码）；
  local = 本机 ollama 小模型（CLI 可设，App 不暴露：0.6B 实测判不准）。
  三者同契约：判 needs_action 二分类 → 异常/超时一律 fail-open 回退现状；换后端只改配置、免重启。
会话路由（v3.9.55）：stream_api 在「关键词规则未命中」时调 route()，判定 needs_action
  → False=纯聊天契约 / True=Agent 工具契约；判定失败/超时一律回退现状（fail-open）。
  熔断（v3.9.56）：连续 breaker_fails（默认 3）次失败/超时 → 停判定 breaker_cooldown_s
  （默认 300s），期间 route() 立即返回 ok=False 不碰上游；到点自动半开重试。
维护：python3 typesafe_api.py show | keyfile <json路径> | judge <preset> "<文本>" | route "<文本>"
      | routing [enabled=true|false mode=smart threshold=0.7 reset_breaker=true backend=custom ...]
      | model [base_url=https://…/v1 model=xxx api_key=xxx test=true]
"""
import json
import os
import sys
import tempfile
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

# 配置读改写串行锁（可重入）：服务跑在 ThreadingHTTPServer 上，App 可能并发写 /routing 与 /model。
# 无锁会丢更新，并让两个线程共用同一 tmp 名互相 replace 失败（HTTP 500）。
_CFG_LOCK = threading.RLock()

# 轻聊惯例：QL_DATA_DIR 为空时落 轻聊web/data（容器内 root 可写，持久化）
_CANDIDATE_DIRS = [
    os.environ.get("QL_DATA_DIR") or "",
    os.environ.get("QL_DATA_DIR", "/data"),
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
    with _CFG_LOCK:                     # 读改写整体串行：并发写 routing/custom 不互相覆盖
        cfg = load_config()
        cfg["api_key"] = (api_key or "").strip()
        if base_url:
            cfg["base_url"] = base_url.strip()
        if model:
            cfg["model"] = model.strip()
        cfg.setdefault("base_url", BASE_URL_DEFAULT)
        cfg.setdefault("model", MODEL_DEFAULT)
        _atomic_write_json(config_path(), cfg)
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
        "custom": custom_cfg(),
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
    "backend": "typesafe",    # 判定后端：typesafe=云端 Jev / custom=自填 OpenAI 兼容模型 / local=本机 ollama
}

# ─── 本地判定后端（routing.backend = "local"）───
# 用途：上游 TypeSafe 被地区封锁（451 not available in your region）时的替代。
# 语义与云端 chat_router 对齐，但只判 needs_action 一件事 —— 小模型只够做二分类，
# 不产出校准概率，故 prob 恒为 1.0/0.0，threshold 实际不起区分作用。
# NAS 实测（Celeron N5105，N=10 条固定样例，见下方对照）——**默认用 1.7B**：
#   模型/提示词/num_predict      得分    单次耗时（常驻后）
#   Qwen3-0.6B + 现有提示词      4/10    1.9s   ← 判不准（倾向全判「纯聊天」，真任务会被丢掉）
#   Qwen3-0.6B + few-shot        6/10    2.6s
#   Qwen3-1.7B + 现有提示词       10/10   7.7s
#   Qwen3-1.7B + 现有提示词 + np=1 10/10  4.1s   ← 取这档（准 + 快）
#   Qwen3-1.7B + 精简提示词       7/10    4.8s   ← 提示词里的类别枚举不能省
LOCAL_URL_DEFAULT = "http://192.168.1.10:11434"   # 容器→宿主必须用 LAN IP（无 host.docker.internal）
LOCAL_MODEL_DEFAULT = "qwen3:1.7b"
LOCAL_KEEP_ALIVE = "60m"        # 常驻内存，避免每条消息都撞冷加载（冷启 ~5s）
LOCAL_TIMEOUT_MS = 15000        # 本地比云端慢得多，单独给足超时（云端仍走 routing.timeout_ms）
LOCAL_PROMPT = (
    "你是轻聊的会话路由器。判断用户这条消息是「要助手实际做事」还是「纯聊天」。\n"
    "要做事：查数据、看/写文件、控制设备、发邮件、建提醒、生成文件、取外部信息。\n"
    "纯聊天：打招呼、寒暄、闲聊、感慨、问常识、对上一轮做简短回应。\n"
    "只输出 1（要做事）或 0（纯聊天），不要输出任何解释或其他文字。\n"
    "用户消息：{msg}\n/no_think"
)


def local_cfg():
    """本地判定配置（typesafe_config.json 顶层 "local" 段，缺省用 LOCAL_* 常量）。"""
    raw = load_config().get("local")
    out = {"url": LOCAL_URL_DEFAULT, "model": LOCAL_MODEL_DEFAULT,
           "timeout_ms": LOCAL_TIMEOUT_MS, "keep_alive": LOCAL_KEEP_ALIVE}
    if isinstance(raw, dict):
        for k in out:
            if raw.get(k):
                out[k] = raw[k]
    return out


# ─── 自定义判定后端（routing.backend = "custom"）───
# 给「用户自带 key 的 OpenAI 兼容模型」用（智谱 GLM-4.7-Flash / OpenRouter / 硅基流动 …），
# 与 TypeSafe 只差协议：走标准 POST {base_url}/chat/completions + Authorization: Bearer。
# key 存在后端配置文件（600 权限、接口只回掩码），App 设置页只做「读写 + 回显掩码」。
# 默认留空：用户 2026-10-10 拍板「不预填厂商，自己填地址/模型/key」
CUSTOM_BASE_URL_DEFAULT = ""
CUSTOM_MODEL_DEFAULT = ""
CUSTOM_TIMEOUT_MS = 4000
CUSTOM_PROMPT = (
    "你是会话路由器，只输出 JSON，不要任何解释。\n"
    "判断用户这条消息是否需要助手实际执行操作：\n"
    "查数据 / 读写文件 / 控制设备 / 发邮件 / 建提醒 / 生成文件 / 取外部信息 / 翻译润色 → true；\n"
    "打招呼 / 寒暄 / 闲聊 / 感慨 / 问常识 / 对上一轮做简短回应 → false。\n"
    '格式：{"needs_action": true 或 false}\n'
    "用户消息：{msg}"
)


def custom_cfg(with_key=False):
    """自定义判定模型配置（typesafe_config.json 顶层 "custom" 段）。默认不回显明文 key。"""
    raw = load_config().get("custom")
    out = {"base_url": CUSTOM_BASE_URL_DEFAULT, "model": CUSTOM_MODEL_DEFAULT,
           "timeout_ms": CUSTOM_TIMEOUT_MS, "api_key": ""}
    if isinstance(raw, dict):
        for k in out:
            if raw.get(k):
                out[k] = raw[k]
    out["configured"] = bool(out.get("api_key"))
    if not with_key:
        out["api_key"] = _mask(out.get("api_key"))
    return out


def save_custom(base_url=None, model=None, api_key=None):
    """写自定义判定模型配置（保留其它字段）。api_key 省略/传掩码 = 不改，传空串 = 清空。"""
    with _CFG_LOCK:                     # 读改写整体串行（同上）
        return _save_custom_locked(base_url, model, api_key)


def _save_custom_locked(base_url=None, model=None, api_key=None):
    cfg = load_config()
    cur = cfg.get("custom") if isinstance(cfg.get("custom"), dict) else {}
    nxt = dict(cur)                     # 保留 timeout_ms 等自定义键（旧写法只列三键会悄悄丢掉它们）
    nxt["base_url"] = nxt.get("base_url") or CUSTOM_BASE_URL_DEFAULT
    nxt["model"] = nxt.get("model") or CUSTOM_MODEL_DEFAULT
    nxt["api_key"] = nxt.get("api_key") or ""
    if base_url is not None:
        b = str(base_url).strip()
        if b and not (b.startswith("http://") or b.startswith("https://")):
            raise ValueError("接口地址需以 http:// 或 https:// 开头")
        nxt["base_url"] = b.rstrip("/") or CUSTOM_BASE_URL_DEFAULT
    if model is not None:
        nxt["model"] = str(model).strip() or CUSTOM_MODEL_DEFAULT
    if api_key is not None:
        k = str(api_key).strip()
        if "…" in k or k in ("未配置", "已配置", "已设置") or (k and set(k) <= set("*•.")):
            pass                                   # 回传的是掩码 → 视为未改动
        elif k and len(k) < 8:
            raise ValueError("API Key 太短（至少 8 位）")
        else:
            nxt["api_key"] = k                     # 空串 = 清空
    cfg["custom"] = nxt
    _atomic_write_json(config_path(), cfg)
    return custom_cfg()


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
    if name == "backend":
        b = str(v or "").strip().lower()
        if b not in ("typesafe", "custom", "local"):
            raise ValueError("backend 只能是 typesafe / custom / local")
        return b
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
    """原子写 JSON，保留原文件属主与权限。

    ⚠️ 旧写法踩过两个真坑（2026-10-10 审查）：
      ① 固定 tmp 名 + 「先 open 后 chmod」：并发的两个写请求会互相 replace 走 tmp（后者抛
         FileNotFoundError → HTTP 500），且 open 按 umask(022) 建出 0644 —— 明文 api_key
         有短暂可读窗口 → 改 mkstemp（天生 0600）+ 全程持 _CFG_LOCK。
      ② 读改写不串行会丢另一段修改 → 所有写入口统一走本函数（锁在内部；调用方已持锁也
         不会死锁，_CFG_LOCK 是 RLock）。
    """
    st = None
    try:
        st = os.stat(path)
    except OSError:
        pass
    with _CFG_LOCK:
        fd, tmp = tempfile.mkstemp(prefix=os.path.basename(path) + ".", suffix=".tmp",
                                   dir=os.path.dirname(path) or ".")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False, indent=1)
                f.flush()
                os.fsync(f.fileno())
            os.chmod(tmp, (st.st_mode & 0o777) if st else mode)
            os.replace(tmp, path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
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


def _parse_yesno(text):
    """把本地小模型的极短输出解析成 True/False；判断不了返回 None（调用方据此回退 Agent）。"""
    s = (text or "").strip()
    if not s:
        return None
    low = s.lower().replace('"', "")
    i = low.find("needs_action")
    if i >= 0:                      # 优先认 JSON 形态 {"needs_action": true}
        seg = low[i:i + 24]
        if "true" in seg:
            return True
        if "false" in seg:
            return False
    if "true" in low:
        return True
    if "false" in low:
        return False
    for ch in s:
        if ch in ("1", "是", "有", "y", "对"):
            return True
        if ch in ("0", "否", "无", "n", "不"):
            return False
        if ch in " \t\r\n\"'`。，,.：:;-—…":
            continue
        break
    return None


def _call_local(state, timeout, meta=None):
    """本地 ollama 判定（routing.backend=local）：返回 (prob, model)，prob ∈ {1.0, 0.0}。

    任何异常 / 输出无法解析 → 抛异常，由 route() 的熔断与 fail-open 兜底（回退走 Agent）。
    故意不返回 None 概率：None 会被上层当成「纯聊天」，把真任务判掉才是危险方向。
    """
    lc = meta or local_cfg()
    base = {
        "model": lc.get("model") or LOCAL_MODEL_DEFAULT,
        # 必须显式关思考：Qwen3 默认先出思考段，num_predict 小时 content 会是空串、
        # done_reason=length（实测踩过 → 判定直接失败）。think=False 后 2 个 token 就出结果。
        "think": False,
        "messages": [{"role": "user", "content": LOCAL_PROMPT.replace("{msg}", state)}],
        "stream": False,
        "keep_alive": lc.get("keep_alive") or LOCAL_KEEP_ALIVE,
        # num_predict=1：只要那一个数字。实测 8 → 1 把 1.7B 的单次判定从 7.7s 压到 4.1s
        # （准确率不变，仍是 10/10），判定在用户每条消息的关键路径上，这里省下的都是体感。
        "options": {"temperature": 0, "num_predict": 1},
    }
    url = (lc.get("url") or LOCAL_URL_DEFAULT).rstrip("/") + "/api/chat"
    data = None
    for payload in (base, {k: v for k, v in base.items() if k != "think"}):
        req = urllib.request.Request(
            url, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"), method="POST")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8", "replace"))
            break
        except urllib.error.HTTPError as e:
            if e.code == 400 and "think" in payload:   # 老版 ollama 不认 think 字段
                continue
            raise
    if data is None:
        raise RuntimeError("本地判定请求失败")
    txt = ((data.get("message") or {}).get("content") or "").strip()
    yes = _parse_yesno(txt)
    if yes is None:
        raise RuntimeError("本地判定输出无法解析：%r" % txt[:60])
    return (1.0 if yes else 0.0), (data.get("model") or lc.get("model"))


def _call_custom(state, timeout, meta=None):
    """自定义 OpenAI 兼容判定（routing.backend=custom）：返回 (prob, model)，prob ∈ {1.0, 0.0}。

    与 _call_local 同契约：任何异常/解析不出 → 抛异常，由 route() 熔断 + fail-open 兜底。
    """
    cc = meta or custom_cfg(with_key=True)
    key = (cc.get("api_key") or "").strip()
    if not key:
        raise RuntimeError("自定义判定模型未配置 API Key")
    base = (cc.get("base_url") or CUSTOM_BASE_URL_DEFAULT).strip()
    if not base:
        raise RuntimeError("自定义判定模型未配置接口地址")
    if not (cc.get("model") or CUSTOM_MODEL_DEFAULT).strip():
        raise RuntimeError("自定义判定模型未配置模型名")
    url = base.rstrip("/") + "/chat/completions"
    # 2026-10-10：思考型模型（GLM-4.5+/GLM-5.x 等）会把 max_tokens=16 全花在 reasoning_content 上，
    # content 恒为空 → 判定永远解析失败（实测 glm-4.5-air/4.6/4.7-flash/5.x 全部如此）。
    # 带上「关思考」参数；上游不认识该参数时（400/422）自动退回不含它的原始形态，保证第三方
    # OpenAI 兼容服务（OpenRouter / 硅基流动…）仍然可用。
    body = {
        "model": cc.get("model") or CUSTOM_MODEL_DEFAULT,
        "messages": [{"role": "user", "content": CUSTOM_PROMPT.replace("{msg}", state)}],
        "temperature": 0,
        "max_tokens": 16,
        "stream": False,
    }
    data = None
    for _extra in ({"thinking": {"type": "disabled"}}, None):
        payload = dict(body)
        if _extra:
            payload.update(_extra)
        req = urllib.request.Request(url, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"), method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", "Bearer " + key)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8", "replace"))
            break
        except urllib.error.HTTPError as e:
            if _extra is None or e.code not in (400, 422):
                raise
    try:
        txt = data["choices"][0]["message"]["content"]
    except Exception:
        raise RuntimeError("自定义判定响应结构异常：%s"
                           % json.dumps(data, ensure_ascii=False)[:120])
    yes = _parse_yesno(txt)
    if yes is None:
        raise RuntimeError("自定义判定输出无法解析：%r" % (txt or "")[:60])
    return (1.0 if yes else 0.0), (data.get("model") or cc.get("model"))


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
    backend = str(c.get("backend") or ROUTING_DEFAULT["backend"]).strip().lower()
    urgency = reply_mode = reply_mode_prob = None
    try:
        # 超时换算也放进 try：配置被手改成非数值时同样走 fail-open，而不是抛穿 route()
        if backend == "local":
            timeout = float(local_cfg().get("timeout_ms") or LOCAL_TIMEOUT_MS) / 1000.0
        elif backend == "custom":
            timeout = float(custom_cfg().get("timeout_ms") or CUSTOM_TIMEOUT_MS) / 1000.0
        else:
            timeout = float(timeout_ms or c.get("timeout_ms") or ROUTING_DEFAULT["timeout_ms"]) / 1000.0
        t0 = time.time()
        if backend == "custom":
            prob, model = _call_custom(t[:MAX_STATE_CHARS], timeout)
        elif backend == "local":
            prob, model = _call_local(t[:MAX_STATE_CHARS], timeout)
        else:
            data = _call(t[:MAX_STATE_CHARS], PRESETS["chat_router"]["questions"], timeout=timeout)
            ans = data.get("answers") or {}
            prob = (ans.get("needs_action") or {}).get("noul")
            if prob is None:
                # 上游没给 needs_action 时**绝不能静默判成纯聊天** —— 真任务会被当闲聊丢掉。
                # 抛错走 fail-open + 熔断计数（与 _call_local 的注释同一口径）。
                raise RuntimeError("上游未返回 needs_action")
            urgency = (ans.get("urgency") or {}).get("noul")
            reply_mode = (ans.get("reply_mode") or {}).get("choice")
            reply_mode_prob = (ans.get("reply_mode") or {}).get("confidence")
            model = data.get("model")
        ms = int((time.time() - t0) * 1000)
        try:
            threshold = float(c.get("threshold") or ROUTING_DEFAULT["threshold"])
        except Exception:
            threshold = ROUTING_DEFAULT["threshold"]
        needs = float(prob) >= threshold      # prob 非数值 → 抛错走 fail-open，不静默判闲聊
    except Exception as e:
        err = "%s: %s" % (type(e).__name__, str(e)[:160])
        tripped, fails_now = _breaker_record(False, err, fails_limit, cooldown_s)
        if tripped:
            print("[typesafe-route] 熔断开启：连续 %d 次判定失败，暂停判定 %d 秒（原因：%s）"
                  % (fails_limit, int(cooldown_s), err), flush=True)
        return {"ok": False, "error": err, "breaker": bool(tripped or _breaker_open()),
                "fails": fails_now}
    out = {
        "ok": True,
        "backend": backend,
        "needs_action": bool(needs),
        "needs_action_prob": prob,
        "threshold": threshold,
        "urgency_prob": urgency,
        "reply_mode": reply_mode,
        "reply_mode_prob": reply_mode_prob,
        "model": model,
        "ms": ms,
    }
    _breaker_record(True, "", fails_limit, cooldown_s)
    print("[typesafe-route] backend=%s needs_action=%s p=%s mode=%s ms=%d" %
          (backend, out["needs_action"], prob, reply_mode, ms), flush=True)
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
MODEL_PATHS = ("/api/typesafe/model", "/api/agent/typesafe/model")


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
        if p in MODEL_PATHS:
            # custom = 用户自带 key 的模型；local = 本机模型（设置页第三档只读展示）
            self._send(200, {"ok": True, "custom": custom_cfg(), "local": local_cfg()})
            return
        self._send(404, {"error": "Not Found"})

    def do_POST(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        p = self._path()
        if p not in JUDGE_PATHS and p not in ROUTING_PATHS and p not in MODEL_PATHS:
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
                # 与 CLI 同口径解析：字符串 "false" 不能被 bool() 当成 True（会误复位熔断）
                reset = str(patch.pop("reset_breaker", "")).lower() in ("1", "true", "yes", "on")
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
        if p in MODEL_PATHS:
            # 写「自定义判定模型」：{"base_url":..,"model":..,"api_key":..}
            # api_key 省略 = 不改；传空串 = 清空（UI 回显的是掩码，没动过就别传）
            try:
                if any(k in body for k in ("base_url", "model", "api_key")):
                    save_custom(base_url=body.get("base_url") if "base_url" in body else None,
                                model=body.get("model") if "model" in body else None,
                                api_key=body.get("api_key") if "api_key" in body else None)
            except ValueError as e:
                self._send(400, {"ok": False, "error": str(e)})
                return
            except Exception as e:
                self._send(500, {"ok": False, "error": "%s: %s" % (type(e).__name__, str(e)[:200])})
                return
            if body.get("test"):
                # 测「当前判定后端」（云端 / 自定义 / 本机三档都覆盖）：直接走 route()，
                # 与生产同一份链路，看到的就是真实判定结果与耗时。
                # 先复位熔断：用户显式点的测试，不该被历史熔断状态挡成「熔断中」。
                reset_breaker()
                rc = routing_cfg()
                be = str(rc.get("backend") or ROUTING_DEFAULT["backend"]).lower()
                out = []
                for sample in ("帮我把这个文件转成 PDF", "你好呀"):
                    t0 = time.time()
                    r = route(sample, cfg=rc)
                    out.append({"text": sample, "ok": bool(r.get("ok")),
                                "needs_action": bool(r.get("needs_action")),
                                "ms": r.get("ms") or int((time.time() - t0) * 1000),
                                "error": r.get("error") or ""})
                self._send(200, {"ok": all(x.get("ok") for x in out), "backend": be,
                                 "test": out, "custom": custom_cfg(), "local": local_cfg()})
                return
            self._send(200, {"ok": True, "custom": custom_cfg(), "local": local_cfg(),
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
    if argv[0] == "model":
        # 读/写自定义判定模型（v4.0.87）：model [base_url=.. model=.. api_key=.. test=true]
        if len(argv) == 1:
            print(json.dumps({"custom": custom_cfg()}, ensure_ascii=False, indent=2))
            return 0
        patch = {}
        do_test = False
        for a in argv[1:]:
            if "=" not in a:
                print("用法：model [base_url=.. model=.. api_key=.. test=true]")
                return 2
            k, v = a.split("=", 1)
            k = k.strip()
            if k == "test":
                do_test = v.strip().lower() in ("1", "true", "yes", "on")
                continue
            patch[k] = v.strip()
        try:
            if patch:
                save_custom(base_url=patch.get("base_url"), model=patch.get("model"),
                            api_key=patch.get("api_key"))
        except ValueError as e:
            print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
            return 1
        print(json.dumps({"ok": True, "custom": custom_cfg()}, ensure_ascii=False, indent=2))
        if do_test:
            tmo = float(custom_cfg().get("timeout_ms") or CUSTOM_TIMEOUT_MS) / 1000.0
            for sample in ("帮我把这个文件转成 PDF", "你好呀"):
                t0 = time.time()
                try:
                    prob, _m = _call_custom(sample, tmo)
                    print("  %s → %s · %dms" % (sample, "要干活" if prob >= 0.5 else "纯聊天",
                                                int((time.time() - t0) * 1000)))
                except Exception as e:
                    print("  %s → 失败：%s: %s" % (sample, type(e).__name__, str(e)[:120]))
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

