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
# v4.0.x 第 5 项：追问已问过几遍的留痕 {text: {"asked": n, "lastTs": ts}}。
# 🚨 为什么必须落盘而不是靠日志：proactive_log 只留最近 500 条且滚动，
# 隔几天日志早滚没了 → 同一条 pending 记忆会被**反复追问**（每天一句「那件事做了吗」）。
# 这个文件就是「问过几次」的唯一真源，也是勾销时的清理依据。
FU_FILE = os.path.join(DATA_DIR, "proactive_followup.json")
# v4.0.x 第 6 项：反思日记的「今天问过没 / 答过没 / 这周回顾发过没」留痕。
# 🚨 为什么必须落盘：_loop 每 5 分钟一轮，若只在内存里记，当天这一问会被重复入队
# N 次（事件队列靠 10 分钟去重挡一部分，10 分钟后又来一次）→ 用户一天被问十几次。
# 跨天/跨周的清零也只认落盘的 day / weekKey，内存态随重启清零不算。
JOURNAL_FILE = os.path.join(DATA_DIR, "proactive_journal.json")
CLUSTER_FILE = os.path.join(DATA_DIR, "proactive_todo_cluster.json")
# v4.0.20（#⑧）周报打通：App 记账快照路径。
# 真源 = <QueryDataDir>/records.json（App RecordStore 经 /api/files/pin_write 写入），
# **不是** life_config.expense 段（App 从不写那个段 → 老周报的记账永远是 ¥0）。
RECORDS_FILE = os.path.join(DATA_DIR, "records.json")

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
    # v4.0.20（#2）：长期目标自动判定 —— 关掉后 AI 不再收到 goal.create 说明，
    # 闲聊不会被误判成「长期目标」；**已存在的目标照常推进**（那是 cron 的事）。
    "goalAutoDetect": True,
    "haWatch": True,          # HA 差分事件源
    "llmJudge": True,         # 关掉则退回固定阈值模板（排查用）
    # v4.0.x 第 5 项「主动跟进闭环」：到期追问 + App 内勾销。
    # 判定池 = 记忆里 status=pending 的条目（第 4 项备好的落点）：
    # 用户自己把某条记忆标成「待确认」= 「我打算照这条做，但还没落实」。
    # followupEnable 关掉后整条链路不产事件（勾销端点仍可用，只是不再追问）。
    "followupEnable": True,
    # 到期小时数：条目标 pending 后多久还没勾销就追问一次（默认 20h ≈ 隔夜）。
    # 不用天：同一天标 pending 当天就追问等于「刚点完就催」，是噪音。
    "followupAfterHours": 20,
    # v4.0.x 第 6 项「反思日记」：到点主动问一句「今天有什么值得记下的」+ 每周一条回顾。
    "journalEnable": True,      # 关掉后每日一问与周回顾都不再产生
    "journalHour": 22,          # 每天几点开始问（本地钟 +08:00）；22 点 = 睡前
}

_lock = threading.RLock()
_log = []                    # 最近投递/判定留痕（内存，滚动 200 条）
_ha_prev = {}                 # 上一次 HA 状态快照


# ───────────────────────── 基础读写 ─────────────────────────
def _now():
    # 固定 +08:00：静默时段/每日预算/goal 天数全部依赖本地钟
    return datetime.now(TZ_CN).replace(tzinfo=None)


def _iso_now():
    """写进 SyncedStore 文件（goals.json 的 reports[].at / updatedAt）的时间戳：
    UTC + 'Z' + 无小数秒。原写法 _now().isoformat() 是 CST naive + 微秒，iOS 端
    JSONDecoder .iso8601 解不开 → 整个 goals.json 解码失败被 try? 吞掉 → 界面停在
    本地旧快照。详见 goal_module._iso_now 注释。"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _dt_cn(s):
    """goals.json 里的时间戳 → CST naive（与 _now() 同口径，可直接相减）。

    兼容三种历史形态：App 写的 '…Z'、后端新写的 '…Z'、旧数据的 naive
    （容器 TZ=UTC，故 naive 按 UTC 解释）。解析失败返回 None。"""
    try:
        dt = datetime.fromisoformat(str(s or "").strip().replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ_CN).replace(tzinfo=None)


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
        # v4.0.x 第 5 项：followupAfterHours 必须 ≥1。低于 1h 时 followup_event 里的
        # `max(1, …)` 会静默兜成 1 —— 用户设 0 想「立刻问」却得到 1h，界面显示还写 0，
        # 表现为「我设了 0 它还是隔一小时才问」。宁可在这里拒掉。
        try:
            cfg["followupAfterHours"] = int(cfg["followupAfterHours"])
        except (TypeError, ValueError, KeyError):
            return {"ok": False, "error": "followupAfterHours 必须是整数"}
        if not (1 <= int(cfg["followupAfterHours"]) <= 720):
            return {"ok": False, "error": "followupAfterHours 取值 1~720 小时"}
        try:
            cfg["journalHour"] = int(cfg["journalHour"])
        except (TypeError, ValueError, KeyError):
            return {"ok": False, "error": "journalHour 必须是整数"}
        if not (0 <= int(cfg["journalHour"]) <= 23):
            return {"ok": False, "error": "journalHour 取值 0~23"}
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
    # v4.0.20 bugfix：_goals() 返回的是 {id: goal} **dict**，原来直接迭代拿到的是
    # id 字符串 → isinstance(g, dict) 恒 False → 整个 nudge 函数从来没生效过。
    items = list(_goals().values())
    n = 0
    for g in items:
        if not isinstance(g, dict) or g.get("paused"):
            continue
        if not (g.get("cronJobIDs") or g.get("cronJobID")):
            n += 1 if add_event("目标「%s」还没建上每日自动推进" % g.get("title", ""),
                                kind="goal_nojob", meta={"goalId": g.get("id")}) else 0
            _note_goal(g.get("id"), "AI 注意到这个目标还没有后台推进任务")
            continue
        upd = str(g.get("updatedAt") or "")
        steps = g.get("steps") or []
        undone = [s for s in steps if not s.get("done")]
        if not undone:
            continue
        last = _dt_cn(upd)
        if last and (_now() - last).days >= 3:
            n += 1 if add_event(
                "目标「%s」已经 %d 天没动静，下一步是「%s」" %
                (g.get("title", ""), (_now() - last).days, undone[0].get("title", "")[:40]),
                kind="goal_stall", meta={"goalId": g.get("id")}) else 0
            _note_goal(g.get("id"), "AI 注意到这个目标 %d 天没动静，已提醒一次"
                       % (_now() - last).days)
    return n


# ─────── 待办聚类建议（v4.0.25 补强第 8 项） ───────
# 语义：同一类事在待办里反复出现（≥3 条未完成、共享关键词）→ 它其实是「要持续做的事」，
# 建议升级成长期目标（AI 每天推一步），比在待办里一条条攒着强。
# 数据源：App 生活页待办 todos.json（与 iOS TodoStore 同目录同文件，只读，取不到就当没有）。
# 防骚扰三重：①已有目标标题命中该词就跳过 ②同一个词只建议一次（落盘记名）③每轮最多 1 条。
CLUSTER_MIN = 3
_CLUSTER_STOP = set("""一个 一下 一起 今天 明天 后天 本周 下周 这个 那个 然后 另外 尽快
记得 别忘 有空 时间 时候 需要 应该 可以 已经 还是 或者 因为 所以 但是 而且 如果
我的 你的 我们 你们 他们 什么 怎么 为什么 有点 一些 准备 打算 计划 开始 完成
第一 第二 第三 每天 每周 每月 坚持 一直""".split())


def _ngrams2(text):
    """2 字滑窗词（粗聚类用）——要分多步推进的事通常有稳定名词反复出现。"""
    out = set()
    for blk in re.findall(r"[一-龥A-Za-z0-9]+", str(text or "")):
        for i in range(len(blk) - 1):
            out.add(blk[i:i + 2])
    return out


def todo_cluster_event():
    """同类未完成待办 >= 3 条 → 建议升级为长期目标。返回新增事件数。"""
    try:
        with open(os.path.join(DATA_DIR, "todos.json"), encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return 0
    rows = data if isinstance(data, list) else (data.get("todos") if isinstance(data, dict) else None)
    if not isinstance(rows, list):
        return 0
    items = [r for r in rows
             if isinstance(r, dict) and not r.get("done") and str(r.get("content") or "").strip()]
    if len(items) < CLUSTER_MIN:
        return 0

    hits = {}
    for it in items:
        for w in _ngrams2(it.get("content")):
            if w in _CLUSTER_STOP:
                continue
            hits.setdefault(w, set()).add(str(it.get("id") or ""))
    cands = [(w, ids) for w, ids in hits.items() if len(ids) >= CLUSTER_MIN]
    if not cands:
        return 0
    cands.sort(key=lambda kv: (-len(kv[1]), -len(kv[0])))

    seen = _load(CLUSTER_FILE, {})
    goals = _goals()
    gvals = list(goals.values()) if isinstance(goals, dict) else []
    for w, ids in cands:
        if w in seen:
            continue
        if any(w in str((g or {}).get("title") or "") for g in gvals):
            continue
        if not add_event("待办里有 %d 条都在说「%s」，看着像要持续做的事 —— 要不要我建个长期目标、每天推你一步？"
                         % (len(ids), w), kind="todoCluster"):
            return 0
        seen[w] = time.time()
        _save(CLUSTER_FILE, seen)
        return 1
    return 0


def _job_id_of(result):
    """hermes 建 job 的返回体在不同版本里包在 job / 顶层，两种都认。"""
    if isinstance(result, dict):
        j = result.get("job") if isinstance(result.get("job"), dict) else result
        return j.get("id") or ""
    return ""


def _append_agent_note(g, text):
    """v4.0.20（#7）：往目标里追加一条「AI 自动动作」留痕（滚动 50 条）。

    存在的理由：目标被 Agent 自动补建 cron / 被判定停滞提醒时，用户只看到
    「它怎么突然跑起来了」，不知道是 AI 自己做的 —— 这类动作必须可追溯。
    """
    try:
        reps = g.setdefault("reports", [])
        reps.insert(0, {"at": _iso_now(), "text": str(text)[:400], "kind": "agent_action"})
        del reps[50:]
    except Exception:
        pass


def _note_goal(goal_id, text):
    """给指定目标追加 Agent 留痕（重新读盘写回，避免覆盖并发修改）。"""
    if not goal_id:
        return
    try:
        import goal_module
        with goal_module._goals_lock:
            goals = goal_module._goals_read()
            g = goals.get(goal_id)
            if isinstance(g, dict):
                _append_agent_note(g, text)
                goal_module._goals_write(goals)
    except Exception:
        pass


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
                    t["updatedAt"] = _iso_now()
                    # v4.0.20（#7）：Agent 自己做的事要留痕，用户才知道「它怎么突然跑起来了」
                    _append_agent_note(t, "AI 自动补上了每日推进（此前这个目标没有后台任务）")
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
        # 🚨 修一个静默空块（代码审查发现，第 4 项顺带修）：原来调 `list_entries()` 拿到的是
        # **纯字符串**列表，却写 `r.get("text","")` 当 dict 用 → 第一行就抛 AttributeError，
        # 被下面这个裸 `except Exception` 吞掉 → 每次都返回「（暂无）」。
        # 后果：主动 Agent 的「用户偏好」上下文**一直是空的**，proactive 少给的一句废话都查不出来，
        # 而日志里一行错都没有。现在改用第 4 项新增的 list_meta()（真 dict 列表）。
        try:
            rows = memory_store.list_meta() or []
        except AttributeError:
            # 老版 memory_store 没这个函数时退回旧读法（滚动部署期保险）
            rows = [{"text": t} for t in (memory_store.list_entries() or [])]
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


# ───────────────────────── 到期追问（第 5 项） ─────────────────────────
# 判定池 = memory_store 里 status=pending 的条目（第 4 项备好的落点）。
# 语义：用户自己把某条记忆标成「待确认」= 「我打算照这条做，但还没落实」。
# 到期 = updated 距今 >= followupAfterHours。
# 最多问 MAX_FOLLOWUP_ROUNDS 遍就收手（用户没勾销可能只是不想被这条打扰，
# 一直问下去就是把「主动」变成「骚扰」，与本模块整个设计目标相反）。
FOLLOWUP_MAX_ROUNDS = 3


def _followups():
    return _load(FU_FILE, {})


def _pending_rows():
    """取 memory_store 里 status=pending 的条目。读不到返回空列表（不是「没有待办」，
    是「不知道」）—— 调用方必须照样发信号，区别在于拿不到正文时用兜底话术。"""
    try:
        import memory_store
        try:
            rows = memory_store.list_meta() or []
        except AttributeError:
            rows = [{"text": t, "status": "active", "updated": 0}
                    for t in (memory_store.list_entries() or [])]
    except Exception:
        return []
    return [r for r in rows if r.get("status") == "pending"]


def _due_at(r, after):
    """一条 pending 条目的到期时刻（**绝对时间戳**，秒）。

    返回 0 = 判不出到期（没有可用时间戳），调用方必须显示「还没到时间」而不是谎报到点。
    口径抽成共用函数是硬要求：followup_event（真投递）和 _pending_with_due（界面显示）
    各自算一遍，阈值改一处就会两处不一致 → 界面说「已到点」而真跑不投递。
    """
    # 🚨 用 updated 而不是 created：用户标 pending 之后又编辑正文，那次编辑是重新确认。
    ts = r.get("updated") or r.get("created") or 0
    try:
        ts = float(ts)
    except (TypeError, ValueError):
        ts = 0
    if ts <= 0:
        return 0
    return int(ts + max(1, int(after)) * 3600)


def _pending_with_due(cfg):
    """pending 判定池 + 每条 dueAt/due，供 App「待跟进」页直接显示。

    只下发正文/计数/到期时刻，不下发任何会进模型上下文的字段。
    """
    try:
        after = max(1, int(cfg.get("followupAfterHours", 20)))
    except (TypeError, ValueError):
        after = 20
    now = time.time()
    fu = _followups()
    out = []
    for r in _pending_rows():
        text = str(r.get("text") or "").strip()
        if not text:
            continue
        at = _due_at(r, after)
        due = bool(at) and now >= at
        asked = int((fu.get(text) or {}).get("asked") or 0)
        # 闭嘴的那几条照样列出来（用户要看得见「为什么它不问了」），但 due 只在还有额度时为真。
        # 🚨 v4.0.17：不再截断。App 拿这个 text **原样回传**去改状态（settle），
        # 而后端按**完整正文**匹配条目；截到 60 字后，超 60 字的条目（手动加的可能很长）
        # 勾销/忽略必然匹配不上 → 恒显示「操作失败，已还原」且无限重试。
        # 界面本来就单行省略显示，不需要后端省字节。
        out.append({"text": text, "asked": asked,
                    "dueAt": at, "due": due and asked < FOLLOWUP_MAX_ROUNDS})
    return out


def followup_event(dry_run=False):
    """pending 记忆到期 → 产一条追问事件。返回 (产了几条, 详情)。

    dry_run=True 时**只判定不入队**（不 add_event、不加 asked 计数、不落盘）：
    App「立刻检查」按钮用它给用户看「现在有哪几条到期了」，点下去不该就真的推消息，
    更不该把「问过几遍」的计数提前消耗掉（那会让自动轮询少问一次，用户莫名少收一条）。
    """
    cfg = get_config()
    if not cfg.get("followupEnable"):
        return 0, None
    try:
        after = max(1, int(cfg.get("followupAfterHours", 20)))
    except (TypeError, ValueError):
        after = 20
    now = time.time()
    fu = _followups()
    n = 0
    detail = []
    # 本轮扫到的 pending 正文：不在这里的旧留痕就是孤儿（条目已改状态/已删）→ 顺手清掉。
    # 不清的后果：用户把条目删了又加回同名一条，它会**继承上一世的 asked 计数**而被直接闭嘴。
    live = set()
    for r in _pending_rows():
        text = str(r.get("text") or "").strip()
        if not text:
            continue
        live.add(text)
        rec = fu.get(text) or {}
        asked = int(rec.get("asked") or 0)
        if asked >= FOLLOWUP_MAX_ROUNDS:
            continue
        # 与界面显示共用 _due_at（口径唯一），否则界面说「已到点」而这里不投递。
        at = _due_at(r, after)
        if not at or now < at:
            continue
        ts = at - after * 3600
        if dry_run:
            # 到期但本次不消耗任何状态：asked 原样回报（让 App 显示「已问 1 遍，还剩 2 遍」）
            # detail 的 text 仅供 App 展示（不作为状态键），保留截断省流量；
            # 全量正文走 full 字段，需要按正文回传的场景用它（见 _pending_with_due）。
            detail.append({"text": text[:60], "full": text,
                           "asked": asked + 1, "dueTs": ts})
            n += 1
            continue
        if not add_event("【待跟进】%s" % text, kind="followup"):
            continue
        rec = {"asked": asked + 1, "lastTs": now}
        fu[text] = rec
        n += 1
        detail.append({"text": text[:60], "full": text,
                       "asked": rec["asked"], "dueTs": ts})
    if not dry_run:
        with _lock:
            if detail or (set(fu.keys()) - live):
                # 真剪枝：不在 live 里的留痕一律丢掉。
                # （写成 `if k in live or k in fu` 是恒真的空操作 —— 看着像剪了，其实一行没删。）
                _save(FU_FILE, {k: v for k, v in fu.items() if k in live})
    return n, detail


def clear_followup(text):
    """用户勾销一条 → 清掉它的追问留痕（下次再标 pending 重新计时）。"""
    t = (text or "").strip()
    if not t:
        return False
    with _lock:
        fu = _followups()
        existed = t in fu
        if existed:
            fu.pop(t, None)
            _save(FU_FILE, fu)
    return existed


# ───────────────────────── 反思日记（第 6 项） ─────────────────────────
# 每日一问：到点（journalHour 之后）投一次「今天有什么值得记下的」。
# 🚨 模板池而不是每晚现调 LLM：①夜里再打一次模型 = 延迟与成本，且同一天重试会换出
# 不同问法，用户看到「今天第二次被问」；②模板按「距 1970-01-01 的天数 % 7」取，
# 每天换一句、一周一轮回，同一天再问永远是同一句 —— 界面显示与真投递口径天然一致。
JOURNAL_DAILY_QS = [
    "今天有什么值得记下来的？（一句话就行，说完我帮你存进记忆）",
    "今天哪件事让你觉得「还好我做了」？",
    "今天有什么让你有点烦？说出来会好受点。",
    "今天学到了什么新东西？",
    "今天有什么想对明天的自己说的？",
    "今天哪一刻你觉得最放松？",
    "今天如果重来一次，你会改哪一步？",
]
# 每周一条回顾（周一到点发）：统计上一周的真实投递/采纳数据 + 记忆池状态。
# v4.0.17 文案修正：第二个 %d 是**累计**采纳/忽略（全期），不是上周的。原来文案把
# 累计数字紧挨「上周」二字 → 数字与措辞不符，用户以为在讲上周。
# 改成明确说「累计」，宁可啰嗦也不误导。
WEEKLY_REVIEW_TEXT = ("【上周回顾】上周我主动开口 %d 条；你到今天为止累计标了 %d 条有用、"
                      "%d 条没用；记忆里还挂着 %d 件待确认的事。%s"
                      "要不要现在一起过一遍？")


def _journal():
    return _load(JOURNAL_FILE, {})


def _week_key(dt=None):
    d = dt or _now()
    y, w, _ = d.isocalendar()
    return "%d-W%02d" % (y, w)


def _daily_q(dt=None):
    d = dt or _now()
    return JOURNAL_DAILY_QS[d.toordinal() % len(JOURNAL_DAILY_QS)]


def _week_stats(week_key):
    """指定那一周（week_key）的投递条数 + 累计采纳/忽略 + 当前待确认数。

    ⚠️ 不能拿「本周 weekKey」去算上周：周一发回顾时 _week_key() 就是本周，
    过滤条件里若带 `!= 当前周` 会把整周数据全滤掉 → 回顾永远报「上周我主动开口 0 条」。
    所以调用方显式传**上一周**的 weekKey，这里只按事件自己的 ts 归周。
    """
    log = _load(LOG_FILE, [])
    fb = _feedback()
    rows = [e for e in log if _ts_week(e.get("ts")) == week_key]
    return len([e for e in rows if e.get("spoke")]), \
        int(fb.get("adopted", 0)), int(fb.get("ignored", 0)), len(_pending_rows())


def _ts_week(ts):
    # v4.0.17 修跨时区错位：原来 datetime.fromtimestamp 用**容器本地时区**（容器 TZ 未设
    # = UTC），而 _day()/_now() 显式 +08:00。周一 00:00–08:00 CST（＝周日 16:00–24:00 UTC）
    # 产出的事件会被归到**上一个 ISO 周** → 周回顾「上周 N 条」既不算本周也错过上周回顾。
    # 口径必须与 _day() 同源：显式 TZ_CN。
    try:
        return _week_key(datetime.fromtimestamp(float(ts), TZ_CN).replace(tzinfo=None))
    except Exception:
        return ""


def _prev_week_key():
    return _week_key(_now() - timedelta(days=7))


def _load_records():
    """读 App 记账快照。缺失/损坏一律当空表（周报给空态，绝不抛）。"""
    try:
        with open(RECORDS_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _rec_epoch(v):
    """RecordItem.createdAt（App 用 .iso8601 → '2026-10-02T01:23:45Z'）→ epoch。

    兼容 'Z' / '+00:00' / 小数秒 / 纯数字；解析失败返回 None（该条不进任何周）。
    """
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ_CN)
        return dt.timestamp()
    except Exception:
        return None


def _rec_week_key(v):
    """createdAt → ISO 周 key（与 _week_stats 同源，显式 TZ_CN，不吃容器 TZ=UTC 的亏）。"""
    ts = _rec_epoch(v)
    if ts is None:
        return ""
    return _week_key(datetime.fromtimestamp(ts, TZ_CN).replace(tzinfo=None))


def _record_spend(week_key):
    """某 ISO 周的支出合计/笔数。只认 unit=='元' 且 kind!='income'（与 App 月度口径同源）；
    度/kWh 是表读数不是钱，收入也绝不并进支出。"""
    total, n = 0.0, 0
    for r in _load_records():
        if not isinstance(r, dict):
            continue
        if str(r.get("unit") or "").strip() != "元":
            continue
        if str(r.get("kind") or "").strip() == "income":
            continue
        try:
            amt = float(r.get("amount"))
        except Exception:
            continue
        if amt != amt or amt <= 0:          # NaN / 非正数
            continue
        if _rec_week_key(r.get("createdAt")) != week_key:
            continue
        total += amt
        n += 1
    return round(total, 2), n


def _rec_money(v):
    """金额文案：整数不带小数，非整留两位。"""
    v = float(v or 0)
    return str(int(round(v))) if abs(v - round(v)) < 0.005 else ("%.2f" % v)


def _record_week_clause():
    """周一「上周回顾」里的记账句。无任何记账记录时返回空串（不留「支出 ¥0」垃圾行）。

    口径参照 v4.0.17 的文案纪律：**「上周」与「本周」写清楚**，别把两个数挨在一起让用户猜。
    """
    cur_sum, cur_n = _record_spend(_week_key())
    prev_sum, prev_n = _record_spend(_prev_week_key())
    if cur_n == 0 and prev_n == 0:
        return ""
    return ("记账这边：上周支出 ¥%s（%d 笔），本周到目前 ¥%s（%d 笔）。"
            % (_rec_money(prev_sum), prev_n, _rec_money(cur_sum), cur_n))




def journal_state(cfg=None):
    """今日/本周的日记状态，供 App「反思日记」卡直接显示（不重算文案口径）。"""
    cfg = cfg or get_config()
    j = _journal()
    day, wk = _day(), _week_key()
    rec = j.get(day) or {}
    return {"enable": bool(cfg.get("journalEnable")),
            "hour": int(cfg.get("journalHour", 22)),
            "day": day, "weekKey": wk,
            "question": _daily_q(),
            "asked": bool(rec.get("asked")),
            "answered": bool(rec.get("answered")),
            "answer": str(rec.get("answer") or ""),
            "weekAsked": bool(rec.get("weekAsked"))}


def journal_event(dry_run=False):
    """每日一问 + 周回顾。返回 (产了几条, 详情)。

    dry_run=True 时**只判定不消耗**：App「看看今天问了没」用它预览，
    不能因为用户看了一眼就把当天的提问机会用掉（那明天就没人问了）。
    """
    cfg = get_config()
    if not cfg.get("journalEnable"):
        return 0, None
    try:
        hour = int(cfg.get("journalHour", 22))
    except (TypeError, ValueError):
        hour = 22
    now = _now()
    day, wk = _day(), _week_key()
    if now.hour < hour:
        # 到点之前不问。日报型提醒在早上问「今天有什么值得记的」是自相矛盾的。
        return 0, None
    # v4.0.17 修「静默时段把当天机会作废」：journalHour 允许 0...23，用户设到 0~6/23 时
    # 这里照样产事件并把 asked=True 落盘，随后 run_once 的 gate 判静默时段不投递、
    # pop_events 把事件丢掉 → 留痕显示「已问过」，用户一条也没收到，当天机会作废。
    # 判定口径必须与 gate 同源：这里就早退，不产出、不消耗留痕。
    if in_quiet(cfg):
        return 0, None
    j = _journal()
    rec = j.get(day) or {}
    detail = []
    if not rec.get("asked"):
        detail.append({"kind": "journal", "text": _daily_q(now)})
    # 周回顾：只在周一发出，且一周一次（weekKey 变 → 自动是新的一周）。
    if now.weekday() == 0 and not rec.get("weekAsked"):
        spoke, ad, ig, pend = _week_stats(_prev_week_key())
        detail.append({"kind": "week_review",
                       "text": WEEKLY_REVIEW_TEXT % (spoke, ad, ig, pend,
                                                     _record_week_clause())})
    if not detail or dry_run:
        return len(detail), detail
    n = 0
    for d in detail:
        if add_event(d["text"], kind=d["kind"]):
            rec["asked" if d["kind"] == "journal" else "weekAsked"] = True
            n += 1
    if n:
        rec["ts"] = time.time()
        # v4.0.17 修并发丢答案：原来 j 是**锁外**读的，随后整体覆盖写；
        # 与 journal_answer（锁内读-改-写）并发时，晚写的一方会把对方的 answer/answered
        # 整段盖掉。改成锁内重读 + 只并入本次写的那几个键。
        with _lock:
            j = _journal()
            cur = j.get(day) or {}
            cur["asked"] = bool(cur.get("asked")) or (rec.get("asked") is True)
            cur["weekAsked"] = bool(cur.get("weekAsked")) or (rec.get("weekAsked") is True)
            cur["ts"] = rec["ts"]
            j[day] = cur
            _save(JOURNAL_FILE, j)
    # 只留最近 90 天：日记是「每天一句」，攒一年既没人翻也占盘。
    with _lock:
        stale = sorted(j.keys())[:-90]
        if stale:
            _save(JOURNAL_FILE, {k: v for k, v in j.items() if k not in stale})
    return n, detail


def journal_answer(text):
    """用户答了今天这一问 → 记进留痕（下次问同一句时 App 能看到「已答过」）。"""
    t = (text or "").strip()[:300]
    day = _day()
    if not t:
        return {"ok": False, "error": "内容为空"}
    with _lock:
        j = _journal()
        rec = j.get(day) or {}
        rec["answered"] = True
        rec["answer"] = t
        rec["answerTs"] = time.time()
        j[day] = rec
        _save(JOURNAL_FILE, j)
    # 答了就记进记忆。
    # 🚨 v4.0.17 修「谎报已存进记忆」：原来调 check_and_save（聊天**记忆意图抽取器**，
    # 只认「记住/我是/我喜欢…」这类句式）却丢弃返回值、无条件 saved:True。
    # 日记答案绝大多数是自由文本（「今天把专利交初稿了」），抽不出东西 → 一条都没存，
    # 界面却 flash「已存进记忆」。实测：答该句 → {ok:True, saved:True} 而 entries=[]。
    # 现在走**真实写入链路** add_entry（去重命中返回 False = 本来就有，不谎报新增），
    # saved 取 add_entry 的返回值，不再猜。
    try:
        import memory_store
        saved = bool(memory_store.add_entry(t, source="journal"))
    except Exception as e:
        print("[proactive] 日记写入记忆失败: %s" % str(e)[:120], flush=True)
        return {"ok": True, "day": day, "saved": False}
    return {"ok": True, "day": day, "saved": saved}


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
            if cfg.get("todoCluster", True):
                todo_cluster_event()
            if cfg.get("followupEnable"):
                followup_event()
            if cfg.get("journalEnable"):
                journal_event()
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
            # v4.0.x 第 5 项：App「待跟进」页要显示「已问过几遍 / 到期时间」，
            # 所以 pending 判定池也一起下发（正文 + 时间戳，不含任何注入内容）。
            "followup": {"pending": _pending_with_due(cfg)[:20],
                         "asked": _followups(),
                         "afterHours": cfg.get("followupAfterHours"),
                         "maxRounds": FOLLOWUP_MAX_ROUNDS},
            # v4.0.x 第 6 项：App「反思日记」卡读这个段直接显示，
            # 问句与后端真投递同一份（_daily_q）→ 界面不会显示另一句话。
            "journal": journal_state(cfg),
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
        # v4.0.x 第 5 项：手动立刻扫一次待跟进（App「待跟进」页的「立刻检查」按钮）。
        # dry_run 默认 True：按下去先看「哪几条到期了」，确认后才真发（真发传 dry_run=false）。
        # 追问**不能**复用 /run —— run_once 会连排队里的其它事件（HA/天气/目标）一起过判定，
        # 用户只想看自己的待办，却顺带触发一批主动消息，那不是他按的按钮。
        # v4.0.x 第 6 项：反思日记（预览默认 dry_run；答问才写盘）
        if tail == "/journal":
            dry = bool(body.get("dry_run", True))
            n, detail = journal_event(dry_run=dry)
            return self._send({"ok": True, "dry_run": dry, "produced": n,
                               "detail": detail or [],
                               "journal": journal_state()})
        if tail == "/journal/answer":
            return self._send(journal_answer(str(body.get("text") or "")))
        if tail == "/followup":
            dry = bool(body.get("dry_run", True))
            n, detail = followup_event(dry_run=dry)
            return self._send({"ok": True, "dry_run": dry, "produced": n,
                               "detail": detail or [],
                               "pending": _pending_rows()[:20],
                               "asked": _followups()})
        return self._send({"ok": False, "error": "not found"}, 404)

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "once":
        print(json.dumps(run_once(dry_run="--send" not in sys.argv),
                         ensure_ascii=False, indent=1)[:3000])



