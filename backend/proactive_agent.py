#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v4.0.11 · 轻聊主动型 Agent 中枢（设计稿 1+2+3+4 合并实现）

四件事在这一层收口：
  1. 决策层：事件/巡检信号 → LLM 判「值不值得打扰 + 说什么」（替代固定阈值直推）
  2. 事件入站：HA 状态差分（真 inbound 源）+ 外部事件注入口 POST /api/agent/proactive/event
  3. 目标追踪：goals.json 里没建 cron 的目标自动补建 + 停滞目标温和 nudge
  4. 打扰预算 + 复盘闭环：每日上限 / 静默时段 / 置信度阈值 / 采纳忽略回流调阈值

投递形态：inbox_api.push(task_type="agent") → App 当普通会话气泡注入（可回复、可追问），
不是收件箱卡片、不是投递壳会话。

HTTP 面（借 /api/agent 前缀：lucky 白名单 + relay + nginx 三份 conf 零改动）：
  GET  /api/agent/proactive/config      读配置
  POST /api/agent/proactive/config      改配置（部分字段）
  POST /api/agent/proactive/event       外部事件入站 {"text","kind","meta"}
  POST /api/agent/proactive/feedback    复盘 {"id","verdict":adopted|ignored}
  POST /api/agent/proactive/run         立刻跑一轮（dry_run=1 只判定不投递）
  GET  /api/agent/proactive/state       预算/最近/事件队列（看板用）
"""

import json
import os
import re
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler

# 🚨 时区坑（2026-09-30 实测修）：容器 TZ 为空 = UTC，而全链路（App/投递/用户口径）按北京时间。
# 原 `_now()` 用 naive datetime.now() → 北京 09:18 被读成 01:18，in_quiet() 误判落在静默段，
# 白天 07:00–15:00 全被静默 = 主动 Agent 白天永远不说话。必须显式 +08:00。
TZ_CN = timezone(timedelta(hours=8))

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("QL_DATA_DIR") or os.path.join(os.path.dirname(BASE), "data")
CFG_FILE = os.path.join(DATA_DIR, "proactive_config.json")
EVT_FILE = os.path.join(DATA_DIR, "proactive_events.json")
LOG_FILE = os.path.join(DATA_DIR, "proactive_log.json")
FB_FILE = os.path.join(DATA_DIR, "proactive_feedback.json")

LOOP_INTERVAL = 300          # 5 分钟一轮判定
HA_POLL_INTERVAL = 60        # HA 差分轮询
EVENT_TTL = 6 * 3600         # 事件 6 小时不消费就作废
HA_URL_FALLBACK = os.environ.get("QL_HA_URL", "http://127.0.0.1:8123")

DEFAULT_CFG = {
    "enabled": True,
    "dailyMax": 6,            # 每日主动条数上限
    "quietStart": 23,         # 静默时段（23:00-07:00 不主动）
    "quietEnd": 7,
    "minScore": 0.55,         # LLM 置信度阈值，低于它不打扰
    "judgeProvider": "",      # 空 = 按 JUDGE_FALLBACKS 依次试（默认 stepfun/step-3.7-flash）
    "judgeModel": "",         # 同上；两个都空才回退全局 AGENT_*
    "goalNudge": True,        # 目标停滞 nudge
    "haWatch": True,          # HA 差分事件源
    "llmJudge": True,         # 关掉则退回固定阈值模板（排查用）
}

_lock = threading.RLock()
_log = []                    # 最近投递/判定留痕（内存，滚动 200 条）
_ha_prev = {}                 # 上一次 HA 状态快照


# ───────────────────────── 基础读写 ─────────────────────────
def _now():
    # 固定 +08:00：静默时段/每日预算/goal 天数全部依赖本地钟
    return datetime.now(TZ_CN).replace(tzinfo=None)


def _day():
    return _now().strftime("%Y-%m-%d")


def _load(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, type(default)):
            return data
    except Exception:
        pass
    return json.loads(json.dumps(default))


def _save(path, obj):
    try:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=1)
        os.replace(tmp, path)
    except Exception as e:
        print("[proactive] 写 %s 失败: %s" % (os.path.basename(path), str(e)[:120]), flush=True)


def get_config():
    cfg = _load(CFG_FILE, DEFAULT_CFG)
    for k, v in DEFAULT_CFG.items():
        cfg.setdefault(k, v)
    return cfg


def save_config(patch):
    with _lock:
        cfg = get_config()
        for k, v in (patch or {}).items():
            if k in DEFAULT_CFG:
                cfg[k] = v
        if not (0 < int(cfg["dailyMax"]) <= 50):
            return {"ok": False, "error": "dailyMax 取值 1~50"}
        if not (0 <= float(cfg["minScore"]) <= 1):
            return {"ok": False, "error": "minScore 取值 0~1"}
        cfg["quietStart"] = min(max(int(cfg["quietStart"]), 0), 23)
        cfg["quietEnd"] = min(max(int(cfg["quietEnd"]), 0), 23)
        _save(CFG_FILE, cfg)
    return {"ok": True, "config": cfg}


def _log_add(entry):
    with _lock:
        _log.append(entry)
        del _log[:-200]


# ───────────────────────── 事件队列 ─────────────────────────
def _events():
    return _load(EVT_FILE, [])


def add_event(text, kind="external", meta=None):
    """事件入站：去重（10 分钟内同 kind+同文本）+ 6h TTL。返回事件 id 或 ''。"""
    text = (text or "").strip()[:300]
    if not text:
        return ""
    with _lock:
        items = _events()
        now = time.time()
        items = [e for e in items if now - (e.get("ts") or 0) < EVENT_TTL]
        for e in items:
            if e.get("kind") == kind and e.get("text") == text and now - (e.get("ts") or 0) < 600:
                return e.get("id", "")
        eid = "%s%04x" % (int(now), int(now * 1000) % 0x10000)
        items.append({"id": eid, "ts": now, "kind": kind, "text": text,
                      "meta": meta or {}, "tried": 0})
        _save(EVT_FILE, items[-200:])
    return eid


def pop_events(max_n=5):
    with _lock:
        items = _events()
        now = time.time()
        keep, out = [], []
        for e in items:
            if now - (e.get("ts") or 0) >= EVENT_TTL:
                continue
            if len(out) < max_n:
                out.append(e)
            else:
                keep.append(e)
        _save(EVT_FILE, keep)
    return out


# ───────────────────────── HA 事件源（差分） ─────────────────────────
LONG_RUN_HOURS = 6
BATTERY_LOW = 20
WATCH_PREFIX = ("climate.", "light.", "switch.", "cover.", "lock.", "binary_sensor.")


def _ha_creds():
    try:
        import rules_engine
        return rules_engine.ha_creds()
    except Exception:
        return (HA_URL_FALLBACK, os.environ.get("QL_HA_TOKEN", ""))


def _ha_states():
    try:
        url, token = _ha_creds()
        req = urllib.request.Request(url.rstrip("/") + "/api/states",
                                     headers={"Authorization": "Bearer %s" % token})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        return data if isinstance(data, list) else []
    except Exception as e:
        print("[proactive] HA 状态读取失败: %s" % str(e)[:100], flush=True)
        return []


def ha_diff_events():
    """把 HA 状态差分翻译成事件。首次只建基线不产事件（避免重启即轰炸）。"""
    global _ha_prev
    states = _ha_states()
    if not states:
        return 0
    cur = {}
    for st in states:
        eid = st.get("entity_id", "")
        if not eid.startswith(WATCH_PREFIX):
            continue
        attrs = st.get("attributes") or {}
        cur[eid] = {
            "state": st.get("state", ""),
            "name": attrs.get("friendly_name") or eid.split(".")[-1],
            "battery": attrs.get("battery"),
            "changed": st.get("last_changed") or "",
        }
    with _lock:
        prev = _ha_prev
        _ha_prev = cur
    if not prev:
        return 0
    n = 0
    now = time.time()
    for eid, c in cur.items():
        p = prev.get(eid)
        if p and p["state"] != c["state"]:
            n += 1 if add_event("「%s」从 %s 变成 %s" % (c["name"], p["state"], c["state"]),
                                kind="ha_state", meta={"entity_id": eid}) else 0
        if p and c["changed"] == p["changed"]:
            continue
        if c["state"] in ("on", "heat", "cool", "open", "locked") and c["changed"]:
            try:
                t = time.mktime(time.strptime(c["changed"][:19], "%Y-%m-%dT%H:%M:%S"))
            except Exception:
                t = now
            if (now - t) / 3600 >= LONG_RUN_HOURS and not p:
                n += 1 if add_event("「%s」已持续开启 %d 小时以上" %
                                    (c["name"], int((now - t) / 3600)),
                                    kind="ha_long", meta={"entity_id": eid}) else 0
        bat = c.get("battery")
        if isinstance(bat, (int, float)) and bat < BATTERY_LOW:
            n += 1 if add_event("「%s」电量只剩 %d%%" % (c["name"], int(bat)),
                                kind="ha_battery", meta={"entity_id": eid}) else 0
    return n


# ───────────────────────── 天气源 ─────────────────────────
def weather_event():
    try:
        req = urllib.request.Request("http://127.0.0.1:9127/api/weather/now")
        with urllib.request.urlopen(req, timeout=8) as r:
            d = json.loads(r.read())
        t, code = d.get("temp"), d.get("code")
        if t is None:
            return 0, None
        st = _load(os.path.join(DATA_DIR, "proactive_weather.json"), {})
        prev = st.get("v") or {}
        st["v"] = {"temp": t, "code": code}
        _save(os.path.join(DATA_DIR, "proactive_weather.json"), st)
        if not prev:
            return 0, None
        RAIN = {51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82, 85, 86}
        try:
            rain = int(code) in RAIN
        except (TypeError, ValueError):
            rain = False
        if prev.get("code") != code and rain:
            return 1 if add_event("天气转为降雨，现在 %s°C" % t, kind="weather") else 0, {"temp": t}
        if prev.get("temp") is not None and float(prev["temp"]) - float(t) >= 5:
            return 1 if add_event("气温从 %s°C 降到 %s°C，降温明显" % (prev["temp"], t),
                                  kind="weather") else 0, {"temp": t}
        return 0, None
    except Exception:
        return 0, None


# ───────────────────────── 目标追踪（设计稿 3） ─────────────────────────
def _goals():
    """读 goals.json 快照（goal_module._goals_read 自带锁与容错）。"""
    try:
        import goal_module
        g = goal_module._goals_read()
        return list(g.values()) if isinstance(g, dict) else []
    except Exception:
        return []


def goal_nudge_event():
    """goals.json 里有目标但没建 cron / 停滞超 3 天 → 产一条 nudge 事件。"""
    items = _goals()
    n = 0
    for g in items:
        if not isinstance(g, dict) or g.get("paused"):
            continue
        if not (g.get("cronJobIDs") or g.get("cronJobID")):
            n += 1 if add_event("目标「%s」还没建上每日自动推进" % g.get("title", ""),
                                kind="goal_nojob", meta={"goalId": g.get("id")}) else 0
            continue
        upd = str(g.get("updatedAt") or "")
        steps = g.get("steps") or []
        undone = [s for s in steps if not s.get("done")]
        if not undone:
            continue
        try:
            last = datetime.fromisoformat(upd) if upd else None
        except Exception:
            last = None
        if last and (_now() - last).days >= 3:
            n += 1 if add_event(
                "目标「%s」已经 %d 天没动静，下一步是「%s」" %
                (g.get("title", ""), (_now() - last).days, undone[0].get("title", "")[:40]),
                kind="goal_stall", meta={"goalId": g.get("id")}) else 0
    return n


def _job_id_of(result):
    """hermes 建 job 的返回体在不同版本里包在 job / 顶层，两种都认。"""
    if isinstance(result, dict):
        j = result.get("job") if isinstance(result.get("job"), dict) else result
        return j.get("id") or ""
    return ""


def _goals():
    try:
        import goal_module
        g = goal_module._goals_read()
        return g if isinstance(g, dict) else {}
    except Exception:
        return {}


def ensure_goal_jobs():
    """给没有 cron 的目标补建每日推进 job（幂等：只在无 job 时建，避免重复建）。"""
    try:
        import goal_module
    except Exception:
        return 0, []
    goals = _goals()
    made = []
    for gid, g in goals.items():
        if not isinstance(g, dict) or g.get("paused"):
            continue
        if g.get("cronJobIDs") or g.get("cronJobID"):
            continue
        title = (g.get("title") or "")[:50]
        try:
            jid = _job_id_of(goal_module._job_create(
                "目标·早推进·%s" % title, "%d 9 * * *" % int(g.get("morningHour", 9)),
                goal_module._goal_morning_prompt(g), deliver="weixin"))
        except Exception as e:
            print("[proactive] 补建目标 job 失败 %s: %s" % (title, str(e)[:100]), flush=True)
            continue
        if not jid:
            continue
        try:
            with goal_module._goals_lock:
                cur = _goals()
                t = cur.get(gid)
                if t and not (t.get("cronJobIDs") or t.get("cronJobID")):
                    t["cronJobID"] = jid
                    t["cronJobIDs"] = [jid]
                    t["updatedAt"] = _now().isoformat()
                    goal_module._goals_write(cur)
        except Exception as e:
            print("[proactive] 回写目标 job id 失败: %s" % str(e)[:100], flush=True)
        made.append(jid)
    if made:
        print("[proactive] 补建目标 cron job %d 个: %s" % (len(made), ",".join(made)), flush=True)
    return len(made), made


# ───────────────────────── 决策层（设计稿 1） ─────────────────────────
JUDGE_PROMPT = """你是「轻聊」的主动决策中枢。判断这条信号值不值得主动打扰用户，并写出要说的话。

规则：
- 家里已经正常、无需用户做任何事 → speak=false
- 只是例行数据（例如此时温度、某设备正常开着且刚开不久）→ speak=false
- 用户不做就会难受/亏钱/错过/有风险 → speak=true
- 说的话要具体、可执行、带一个明确动作；不要寒暄、不要复述信号原文、别列清单
- 中文，60 字以内，最多 1 个 emoji 开头

用户偏好（来自长期记忆，供参考）：
%s

本次信号：
%s

只输出 JSON：{"speak":true|false,"score":0~1,"text":"...","action":""}
action 填 suggestion/none。"""


def _memory_block():
    try:
        import memory_store
        rows = memory_store.list_entries() or []
        txt = "、".join([str(r.get("text", ""))[:40] for r in rows[:12]])
        return txt or "（暂无）"
    except Exception:
        return "（暂无）"


JUDGE_FALLBACKS = [("stepfun", "step-3.7-flash"), ("", "")]   # (provider, model)


def _parse_judge(txt):
    m = re.search(r"\{.*\}", txt, re.S)
    if not m:
        return None
    obj = json.loads(m.group(0))
    return obj if isinstance(obj, dict) else None


def _judge_llm(signal):
    """按 JUDGE_FALLBACKS 逐个端点试，第一个成功即用。

    🚨 踩坑：`_agent_endpoint(None, None)` 回退全局 AGENT_*（本部署 = DeepSeek，
    余额 0 → 402），判定层会**静默永久退化**成模板兜底（实测日志
    「LLM 判定失败: HTTP Error 402」而服务照跑，看似在岗实则失能）。
    故默认走 stepfun step-3.7-flash，只在它也挂时才回退全局端点。
    """
    cfg = get_config()
    want = (cfg.get("judgeProvider") or "", cfg.get("judgeModel") or "")
    order = ([want] if any(want) else []) + JUDGE_FALLBACKS
    seen = set()
    for prov, mdl in order:
        key0 = (prov, mdl)
        if key0 in seen:
            continue
        seen.add(key0)
        try:
            import stream_api
            url, api_key, eff_model = stream_api._agent_endpoint(
                mdl or None, prov or None)
            body = {
                "model": eff_model,
                "messages": [
                    {"role": "system", "content":
                     "你只输出 JSON，不要任何解释。不要输出思考过程或推理步骤。"},
                    {"role": "user", "content": JUDGE_PROMPT % (_memory_block(), signal)},
                ],
                # step-3.7-flash 思考关不掉（单次 ~2400 字）→ 必须留足，
                # 否则撞 finish_reason=length 返空 content
                "max_tokens": 4000,
            }
            resp = stream_api._chat_once(body, url=url, key=api_key)
            msg = (resp.get("choices") or [{}])[0].get("message", {}) or {}
            obj = _parse_judge(msg.get("content") or "")
            if obj is None:
                obj = _parse_judge(msg.get("reasoning_content") or "")
            if obj is None:
                raise ValueError("判定输出无 JSON content=%r" % (msg.get("content") or "")[:60])
            print("[proactive] 判定端点 %s/%s ok" % (prov or "global", eff_model), flush=True)
            return obj
        except Exception as e:
            print("[proactive] 判定端点 %s/%s 失败: %s"
                  % (prov or "global", mdl or "-", str(e)[:120]), flush=True)
    return None


def _judge_template(signal):
    """LLM 不可用时的兜底：关键词命中才说话。"""
    hit = any(k in signal for k in ("降雨", "降温", "电量只剩", "没动静", "从", "未开启"))
    return {"speak": bool(hit), "score": 0.6 if hit else 0.2,
            "text": ("🌦 " + signal) if hit else "", "action": "suggestion" if hit else "none"}


def judge(signal, use_llm=True):
    obj = _judge_llm(signal) if use_llm else None
    if not isinstance(obj, dict) or "speak" not in obj:
        obj = _judge_template(signal)
    try:
        obj["score"] = max(0.0, min(1.0, float(obj.get("score", 0))))
    except (TypeError, ValueError):
        obj["score"] = 0.0
    obj["text"] = (obj.get("text") or "").strip()[:200]
    return obj


# ───────────────────────── 打扰预算 + 复盘闭环（设计稿 4） ─────────────────────────
def _feedback():
    return _load(FB_FILE, {"adopted": 0, "ignored": 0, "by_kind": {}})


def _adaptive_threshold(cfg):
    """采纳/忽略比低 → 抬高阈值（少打扰）；比高 → 略降（更主动）。"""
    fb = _feedback()
    a, i = int(fb.get("adopted", 0)), int(fb.get("ignored", 0))
    if a + i < 4:
        return float(cfg.get("minScore", 0.55))
    ratio = a / float(a + i)
    return round(max(0.35, min(0.85, 0.55 + (0.5 - ratio) * 0.4)), 3)


def budget_left():
    cfg = get_config()
    log = _load(LOG_FILE, [])
    day = _day()
    used = len([e for e in log if e.get("day") == day and e.get("spoke")])
    return max(0, int(cfg["dailyMax"]) - used), used


def in_quiet(cfg=None):
    cfg = cfg or get_config()
    h = _now().hour
    a, b = int(cfg["quietStart"]), int(cfg["quietEnd"])
    return h >= a or h < b if a > b else (a <= h < b)


def gate(cfg, obj):
    if not cfg.get("enabled"):
        return False, "已关闭"
    if in_quiet(cfg):
        return False, "静默时段"
    left, _used = budget_left()
    if left <= 0:
        return False, "今日条数用完"
    if not obj.get("speak") or not obj.get("text"):
        return False, "判定不打扰"
    if float(obj.get("score", 0)) < _adaptive_threshold(cfg):
        return False, "低于置信度阈值"
    return True, "通过"


def record(entries):
    if not entries:
        return
    with _lock:
        log = _load(LOG_FILE, [])
        log.extend(entries)
        _save(LOG_FILE, log[-500:])


def feedback(pid, verdict):
    with _lock:
        fb = _feedback()
        if verdict not in ("adopted", "ignored"):
            return {"ok": False, "error": "verdict 只收 adopted / ignored"}
        fb[verdict] = int(fb.get(verdict, 0)) + 1
        log = _load(LOG_FILE, [])
        for e in log:
            if e.get("id") == pid and e.get("kind"):
                bk = fb.setdefault("by_kind", {})
                k = e["kind"]
                bk[k] = bk.get(k, {"adopted": 0, "ignored": 0})
                bk[k][verdict] = int(bk[k].get(verdict, 0)) + 1
                break
        _save(FB_FILE, fb)
    return {"ok": True, "feedback": fb, "threshold": _adaptive_threshold(get_config())}


# ───────────────────────── 投递 ─────────────────────────
def deliver(text, kind="agent", event_id=""):
    """把主动消息投成**可回复的会话气泡**（task_type=agent）。

    ⚠️ inbox_api.push 的白名单会把未知 task_type 静默改写成 reply —— 那个
    结果「看起来能用」但 App 走的是回复去重链路，会误判重复被丢。故必须
    先在 inbox_api 白名单里放行 agent。
    """
    pid = "%s%04x" % (int(time.time()), int(time.time() * 1000) % 0x10000)
    entry = {"id": pid, "day": _day(), "ts": time.time(), "kind": kind,
             "eventId": event_id, "text": text[:200]}
    try:
        import inbox_api
        ok, msg = inbox_api.push(text, task_id=pid, task_type="agent")
    except Exception as e:
        print("[proactive] 投递失败: %s" % str(e)[:120], flush=True)
        ok, msg = False, str(e)
    entry["ok"] = bool(ok)
    entry["err"] = "" if ok else str(msg)[:120]
    record([entry])
    _log_add(entry)
    if ok:
        print("[proactive] 已主动投递 kind=%s 字数=%d" % (kind, len(text)), flush=True)
    return entry


# ───────────────────────── 一轮判定 ─────────────────────────
def run_once(dry_run=False, limit=3):
    cfg = get_config()
    out = []
    evs = pop_events(max_n=limit)
    thr = _adaptive_threshold(cfg)
    for e in evs:
        signal = e.get("text", "")
        obj = judge(signal, use_llm=bool(cfg.get("llmJudge", True)))
        passed, why = gate(cfg, obj)
        _log_add({"id": e.get("id"), "ts": time.time(), "day": _day(),
                  "kind": e.get("kind"), "signal": signal[:120],
                  "speak": bool(obj.get("speak")), "score": obj.get("score"),
                  "gate": why, "threshold": thr, "spoke": False})
        item = {"event": e, "judge": obj, "gate": why, "threshold": thr}
        if passed and not dry_run:
            r = deliver(obj["text"], kind=e.get("kind", "agent"), event_id=e.get("id", ""))
            item["delivered"] = r.get("ok")
            for lg in _log:
                if lg.get("id") == e.get("id"):
                    lg["spoke"] = True
        out.append(item)
    return out


def _loop():
    while True:
        try:
            cfg = get_config()
            if cfg.get("haWatch"):
                ha_diff_events()
            weather_event()
            if cfg.get("goalNudge"):
                ensure_goal_jobs()
                goal_nudge_event()
            run_once()
        except Exception as e:
            print("[proactive] 轮次异常: %s" % str(e)[:140], flush=True)
        time.sleep(LOOP_INTERVAL)


def start_engine():
    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    print("[proactive] 主动 Agent 引擎已启动（每 %ds 一轮）" % LOOP_INTERVAL, flush=True)
    return t


# ───────────────────────── HTTP 面 ─────────────────────────
def _state():
    cfg = get_config()
    left, used = budget_left()
    return {"ok": True, "config": cfg, "budget": {"left": left, "used": used,
            "dailyMax": cfg["dailyMax"]},
            "quiet": in_quiet(), "threshold": _adaptive_threshold(cfg),
            "feedback": _feedback(), "events": len(_events()),
            "recent": _log[-15:]}


class Handler(BaseHTTPRequestHandler):
    """挂到 unified_router 的 /api/agent/proactive。

    router 用 `cls.__new__(cls)` 造子 Handler 再调 do_GET/do_POST，所以
    这里必须实现标准 BaseHTTPRequestHandler 方法，不能只写 handle_one。
    """

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, X-Auth-Token")

    def _send(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            pass

    def _auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, "X-Proactive-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def _body(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(n) or b"{}") if n else {}
        except Exception:
            return {}

    def _tail(self):
        import urllib.parse
        p = urllib.parse.urlparse(self.path).path
        return p[len("/api/agent/proactive"):] or "/"

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._auth():
            return self._send({"ok": False, "error": "unauthorized"}, 401)
        tail = self._tail()
        if tail in ("/", "/config"):
            return self._send({"ok": True, "config": get_config()})
        if tail == "/state":
            return self._send(_state())
        return self._send({"ok": False, "error": "not found"}, 404)

    def do_POST(self):
        if not self._auth():
            return self._send({"ok": False, "error": "unauthorized"}, 401)
        tail = self._tail()
        body = self._body()
        if tail == "/config":
            return self._send(save_config(body))
        if tail == "/event":
            eid = add_event(body.get("text"), body.get("kind") or "external", body.get("meta"))
            return self._send({"ok": bool(eid), "id": eid})
        if tail == "/feedback":
            return self._send(feedback(str(body.get("id") or ""), str(body.get("verdict") or "")))
        if tail == "/run":
            dry = bool(body.get("dry_run"))
            return self._send({"ok": True, "dry_run": dry, "results": run_once(dry_run=dry)})
        return self._send({"ok": False, "error": "not found"}, 404)

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "once":
        print(json.dumps(run_once(dry_run="--send" not in sys.argv),
                         ensure_ascii=False, indent=1)[:3000])
