# -*- coding: utf-8 -*-
"""条件自动化规则引擎（轻聊 v3.9.21）

背景：automation_api.py 原来只支持「X 分钟后执行 Y」（run_at、一次性、无重复）。
本模块补上「条件触发」：时间窗 / HA 实体状态 / 上报事件（地理围栏、快捷指令、充电等）
→ 条件全部命中、且过冷却与小时限额后才执行动作。

关键设计（都是踩过的坑，别简化掉）
- **边沿触发**：只在条件由「假」变「真」的那一刻执行。`last_matched` 落盘，重启不丢。
  否则「在家 + 22 点后」这类条件在窗口内每 3 秒都会命中 → 反复开灯。
- **冷却 + 小时限额**：默认 cooldown 900s、每小时最多 6 次，防抖动风暴。
- **干跑（simulate）**：只求值不执行，逐条返回真假与依据 —— 规则上线前先跑一遍。
- **动作类型**：ha（HA 服务调用，兼容旧 actions 格式）/ push（微信推送队列）。

存储（QL_DATA_DIR）
- rules.json   规则列表（含运行时字段 last_matched / last_run / hour_start / hour_count）
- events.json  最近上报事件（push_event 写入，默认 1 小时 TTL、保留 200 条）

条件 DSL
    trigger = {"all": [...], "any": [...], "cooldown": 900, "max_per_hour": 6}
    条件项：
      {"type": "always"}
      {"type": "time",   "after": "22:00", "before": "06:00", "weekdays": [0,1,2,3,4]}  # 支持跨午夜；0=周一
      {"type": "state",  "entity": "lock.front_door", "equals": "locked"}               # 或用 "in": [...]
      {"type": "state",  "entity": "sensor.xxx_battery", "below": 25}                   # 数值比较：below/above/between
      {"type": "event",  "event": "geofence.enter", "match": {"place": "home"}, "within": 120}
      {"type": "device", "prop": "charging", "equals": True, "within": 900}
"""
import json
import os
import threading
import time
import urllib.request

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("QL_DATA_DIR", os.path.join(os.path.dirname(BASE), "data"))
RULES_FILE = os.path.join(DATA_DIR, "rules.json")
EVENTS_FILE = os.path.join(DATA_DIR, "events.json")

HA_URL = os.environ.get("QL_HA_URL", "http://localhost:8123")
HA_TOKEN = os.environ.get("QL_HA_TOKEN", "")
HA_CACHE_TTL = 5          # HA 状态缓存，避免条件求值把 HA 打爆


def ha_creds():
    """凭据与 ha_proxy 同源：优先 data/ha_config.json 的 address/token，其次环境变量。

    ⚠️ 实测：容器里的 QL_HA_TOKEN 调 HA 会 **401**（旧 token），而 ha_config.json 里的有效
    —— 必须跟 ha_proxy 走同一份，否则条件永远读不到状态、规则永不触发。
    """
    try:
        with open(os.path.join(DATA_DIR, "ha_config.json"), encoding="utf-8") as f:
            c = json.load(f)
        return (c.get("address") or HA_URL), (c.get("token") or HA_TOKEN)
    except Exception:
        return HA_URL, HA_TOKEN
DEFAULT_COOLDOWN = 900
DEFAULT_MAX_PER_HOUR = 6
EVENT_KEEP = 200
EVENT_TTL = 3600

_lock = threading.Lock()
_ha_cache = {}            # entity -> (ts, state)


def _read(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write(path, data):
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass


# ---------------- 规则 CRUD ----------------

def load_rules():
    return _read(RULES_FILE, [])


def save_rules(rules):
    _write(RULES_FILE, rules)


def list_rules():
    import time as _t
    now = _t.time()
    out = []
    for r in load_rules():
        item = dict(r)
        item["last_run_ago"] = int(now - r["last_run"]) if r.get("last_run") else None
        out.append(item)
    out.sort(key=lambda x: (not x.get("enabled", True), x.get("name") or ""))
    return out


def create_rule(name, trigger, actions, enabled=True):
    if not name or not trigger or not actions:
        return False, "规则需要名称、触发条件、动作"
    if not ((trigger.get("all") or trigger.get("any"))):
        return False, "触发条件需要至少一条 all 或 any 条件"
    if not trigger.get("all") and trigger.get("any") and len(trigger["any"]) > 1:
        return False, "只给 any、不给 all 时，any 只能有一条（否则语义含糊）"
    item = {
        "id": __import__("uuid").uuid4().hex[:12],
        "name": name,
        "enabled": bool(enabled),
        "trigger": trigger,
        "actions": actions,
        "created": time.strftime("%Y-%m-%d %H:%M"),
        "last_matched": False,
        "last_run": 0,
        "hour_start": 0,
        "hour_count": 0,
        "run_count": 0,
    }
    with _lock:
        rules = load_rules()
        rules.append(item)
        save_rules(rules)
    return True, f"规则「{name}」已创建（冷却 {trigger.get('cooldown') or DEFAULT_COOLDOWN}s）"


def delete_rule(rid):
    with _lock:
        rules = load_rules()
        new = [r for r in rules if r.get("id") != rid]
        if len(new) == len(rules):
            return False
        save_rules(new)
        return True


def toggle_rule(rid, enabled):
    with _lock:
        rules = load_rules()
        hit = False
        for r in rules:
            if r.get("id") == rid:
                r["enabled"] = bool(enabled)
                r["last_matched"] = False      # 重新启用时清状态，避免"启用瞬间补一刀"
                hit = True
        if hit:
            save_rules(rules)
        return hit


# ---------------- 事件 ----------------

def load_events():
    now = time.time()
    return [e for e in _read(EVENTS_FILE, []) if now - (e.get("ts") or 0) <= EVENT_TTL]


def push_event(ev):
    """App / 快捷指令 / 后端上报事件。
    例：{"event": "geofence.enter", "place": "home"}
        {"event": "device.state", "charging": True, "battery": 78}
    """
    item = dict(ev or {})
    if not item.get("event"):
        return None
    item["ts"] = item.get("ts") or time.time()
    with _lock:
        evs = load_events()
        evs.append(item)
        _write(EVENTS_FILE, evs[-EVENT_KEEP:])
    return item


def _find_event(events, name, match, within, now):
    for e in reversed(events):
        if now - (e.get("ts") or 0) > within:
            break
        if e.get("event") != name:
            continue
        if all(e.get(k) == v for k, v in (match or {}).items()):
            return e
    return None


# ---------------- HA 状态 ----------------

def ha_state(entity, ttl=HA_CACHE_TTL):
    now = time.time()
    c = _ha_cache.get(entity)
    if c and now - c[0] <= ttl:
        return c[1], None
    base, token = ha_creds()
    try:
        req = urllib.request.Request(f"{base}/api/states/{entity}",
                                     headers={"Authorization": "Bearer " + token})
        with urllib.request.urlopen(req, timeout=8) as r:
            st = (json.loads(r.read().decode()) or {}).get("state")
        _ha_cache[entity] = (now, st)
        return st, None
    except Exception as e:
        return None, str(e)[:80]


# ---------------- 条件求值 ----------------

def _hm(t):
    try:
        h, m = str(t).split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return None


def _in_window(now_min, after, before):
    a, b = _hm(after), _hm(before)
    if a is None or b is None:
        return False
    if a <= b:
        return a <= now_min <= b
    return now_min >= a or now_min <= b      # 跨午夜，如 22:00~06:00


def eval_cond(cond, now=None, events=None):
    """返回 (是否命中, 依据)——依据要能直接给人看，干跑时就是它。"""
    now = now or time.time()
    events = load_events() if events is None else events
    t = (cond or {}).get("type")
    if t == "always":
        return True, "恒真"
    if t == "time":
        lt = time.localtime(now)
        now_min = lt.tm_hour * 60 + lt.tm_min
        wd = cond.get("weekdays")
        if wd and lt.tm_wday not in wd:
            return False, "今天不在限定星期内（今天=周%d）" % (lt.tm_wday + 1)
        ok = _in_window(now_min, cond.get("after"), cond.get("before"))
        return ok, "当前 %02d:%02d，窗口 %s~%s → %s" % (
            lt.tm_hour, lt.tm_min, cond.get("after"), cond.get("before"),
            "在窗口内" if ok else "不在窗口内")
    if t == "state":
        ent = cond.get("entity")
        st, err = ha_state(ent)
        if err:
            return False, "%s 读取失败：%s" % (ent, err)
        # 数值比较：电量/温度这类 sensor 的 state 是数字（below / above / between）
        # —— 等值比较对它们没用（60.0 永远 != 用户心里的“低电量”）
        if any(k in cond for k in ("below", "above", "between")):
            try:
                val = float(st)
            except Exception:
                return False, "%s = %r 不是数值（不可用或非数字），无法比较" % (ent, st)
            if "below" in cond:
                ok = val < float(cond["below"])
                return ok, "%s = %s，期望 < %s → %s" % (ent, val, cond["below"], "满足" if ok else "不满足")
            if "above" in cond:
                ok = val > float(cond["above"])
                return ok, "%s = %s，期望 > %s → %s" % (ent, val, cond["above"], "满足" if ok else "不满足")
            lo, hi = (cond.get("between") or [0, 0])[:2]
            ok = float(lo) <= val <= float(hi)
            return ok, "%s = %s，期望落在 [%s, %s] → %s" % (ent, val, lo, hi, "满足" if ok else "不满足")
        if "in" in cond:
            ok = st in (cond.get("in") or [])
            return ok, "%s = %s，期望属于 %s" % (ent, st, cond.get("in"))
        ok = (st == cond.get("equals"))
        return ok, "%s = %s，期望 %s" % (ent, st, cond.get("equals"))
    if t == "event":
        within = int(cond.get("within") or 120)
        e = _find_event(events, cond.get("event"), cond.get("match"), within, now)
        tag = "%s %s" % (cond.get("event"), cond.get("match") or "")
        if e:
            return True, "%ds 内收到 %s（%.0fs 前）" % (within, tag, now - (e.get("ts") or now))
        return False, "%ds 内没有 %s" % (within, tag)
    if t == "device":
        within = int(cond.get("within") or 900)
        prop = cond.get("prop")
        for e in reversed(events):
            if now - (e.get("ts") or 0) > within:
                break
            if e.get("event") == "device.state" and prop in e:
                ok = (e.get(prop) == cond.get("equals"))
                return ok, "%ds 内设备 %s=%s，期望 %s" % (within, prop, e.get(prop), cond.get("equals"))
        return False, "%ds 内没有设备状态上报（%s）" % (within, prop)
    return False, "未知条件类型 %s" % t


def eval_rule(rule, now=None, events=None):
    """all 全真 且（无 any 或 any 至少一条真）→ 命中"""
    now = now or time.time()
    events = load_events() if events is None else events
    tr = rule.get("trigger") or {}
    alls = tr.get("all") or []
    anys = tr.get("any") or []
    details = []
    ok_all = True
    for c in alls:
        ok, why = eval_cond(c, now, events)
        details.append({"cond": c, "ok": ok, "why": why})
        ok_all = ok_all and ok
    if anys:
        ok_any = False
        for c in anys:
            ok, why = eval_cond(c, now, events)
            details.append({"cond": c, "ok": ok, "why": why})
            ok_any = ok_any or ok
        matched = ok_all and ok_any
    else:
        matched = ok_all and bool(alls)
    return matched, details


def simulate(rule, now=None, events=None):
    """干跑：只求值不执行。"""
    matched, details = eval_rule(rule, now, events)
    tr = rule.get("trigger") or {}
    return {
        "matched": matched,
        "conditions": details,
        "cooldown_seconds": int(tr.get("cooldown") or DEFAULT_COOLDOWN),
        "max_per_hour": int(tr.get("max_per_hour") or DEFAULT_MAX_PER_HOUR),
        "verdict": ("条件命中：真实事件发生时**会执行**动作" if matched
                    else "条件未命中：当前不会执行动作"),
    }


# ---------------- 调度（边沿触发） ----------------

def tick(executor, now=None):
    """调度线程每 N 秒调用一次。

    executor(rule, details) -> (ok, message)
    返回本次真正触发的列表。
    """
    now = now or time.time()
    events = load_events()
    rules = load_rules()
    dirty = False
    fired = []
    for r in rules:
        if not r.get("enabled", True):
            continue
        if (r.get("trigger") or {}).get("kind") == "delay":
            continue                       # 旧的延时型交给 automation_api 原逻辑
        try:
            matched, details = eval_rule(r, now, events)
        except Exception:
            continue
        prev = bool(r.get("last_matched"))
        if matched != prev:
            r["last_matched"] = matched
            dirty = True
        if not (matched and not prev):     # 只认上升沿
            continue
        tr = r.get("trigger") or {}
        cd = int(tr.get("cooldown") or r.get("cooldown_seconds") or DEFAULT_COOLDOWN)
        if now - (r.get("last_run") or 0) < cd:
            continue
        cap = int(tr.get("max_per_hour") or DEFAULT_MAX_PER_HOUR)
        if now - (r.get("hour_start") or 0) > 3600:
            r["hour_start"], r["hour_count"] = now, 0
            dirty = True
        if (r.get("hour_count") or 0) >= cap:
            continue
        try:
            ok, msg = executor(r, details)
        except Exception as e:
            ok, msg = False, str(e)[:120]
        r["last_run"] = now
        r["hour_count"] = (r.get("hour_count") or 0) + 1
        r["run_count"] = (r.get("run_count") or 0) + 1
        dirty = True
        fired.append({"id": r.get("id"), "name": r.get("name"), "ok": ok, "message": msg})
    if dirty:
        save_rules(rules)
    return fired
