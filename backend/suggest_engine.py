# -*- coding: utf-8 -*-
"""主动建议引擎（v2.0.116）：后台学习/巡检家庭状态，主动生成建议 → 微信推送 + 看板展示。

巡检项（每 30 分钟）：
1. 天气突变：降温 ≥5°C 或转雨 → 建议（当天同类只推一次）
2. 设备长时间运行：空调/灯持续开启 > 6h → 建议关/定时
3. 低电量：门锁/传感器 battery < 20% → 提醒（每日一次）

存储：QL_DATA_DIR/active_suggestions.json
"""
import json
import os
import threading
import time
import urllib.request

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("QL_DATA_DIR", os.path.join(os.path.dirname(BASE), "data"))
STATE_FILE = os.path.join(DATA_DIR, "active_suggestions.json")

HA_URL = os.environ.get("QL_HA_URL", "http://localhost:8123")
HA_TOKEN = os.environ.get("QL_HA_TOKEN", "")
WEATHER_URL = os.environ.get("QL_WEATHER_URL", "http://127.0.0.1:9141")

CHECK_INTERVAL = 1800   # 30 分钟
LONG_RUN_HOURS = 6      # 长开阈值
BATTERY_LOW = 20        # 低电量阈值


def _load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"pushed": {}, "last_weather": None}


def _save_state(s):
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(s, f, ensure_ascii=False, indent=1)
        os.replace(tmp, STATE_FILE)
    except Exception:
        pass


def _push(text):
    """推送到微信队列 + 记录为最近建议（看板可读）"""
    try:
        import push_api
        push_api.enqueue(text)
    except Exception:
        pass
    try:
        s = _load_state()
        s["last_suggestion"] = {"ts": time.strftime("%m-%d %H:%M"), "text": text}
        _save_state(s)
    except Exception:
        pass


def _weather():
    try:
        req = urllib.request.Request(f"{WEATHER_URL}/api/weather/now")
        with urllib.request.urlopen(req, timeout=8) as r:
            d = json.loads(r.read())
        return d.get("temp"), d.get("code")
    except Exception:
        return None, None


def _ha_states():
    try:
        req = urllib.request.Request(f"{HA_URL}/api/states",
                                     headers={"Authorization": f"Bearer {HA_TOKEN}"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception:
        return []


def _check_weather():
    """天气突变：降温 ≥5°C 或转雨"""
    today = time.strftime("%Y-%m-%d")
    s = _load_state()
    temp, code = _weather()
    if temp is None:
        return
    prev = s.get("last_weather") or {}
    s["last_weather"] = {"temp": temp, "code": code, "day": today}
    _save_state(s)
    if not prev:
        return   # 首次无基线
    # 转雨
    if prev.get("code") != code and str(code) in ("3", "5", "60", "61", "63", "65", "80", "81", "82"):
        key = f"rain_{today}"
        if not s.get("pushed", {}).get(key):
            _push(f"🌧 天气提醒：当前天气转为降雨，记得收衣服、出门带伞（{temp}°C）")
            s["pushed"][key] = True
            _save_state(s)
    # 降温
    if prev.get("temp") is not None and prev.get("temp") - temp >= 5:
        key = f"cold_{today}"
        if not s.get("pushed", {}).get(key):
            _push(f"🥶 降温提醒：气温从 {int(prev['temp'])}°C 降至 {int(temp)}°C，注意添衣，睡前可考虑关空调")
            s["pushed"][key] = True
            _save_state(s)


def _check_long_running():
    """设备长时间运行：空调/灯 on 且 last_changed > 6h
    v2.0.116 fix：合并成一条推送（原按设备逐个推，用户收到多条类似提醒）"""
    today = time.strftime("%Y-%m-%d")
    s = _load_state()
    now = time.time()
    long_running = []   # (friendly, hours)
    for st in _ha_states():
        eid = st.get("entity_id", "")
        state = st.get("state", "")
        if not (eid.startswith("climate.") or eid.startswith("light.")):
            continue
        if state not in ("on", "heat", "cool", "auto"):
            continue
        try:
            lc = st.get("last_changed") or ""
            t = time.mktime(time.strptime(lc[:19], "%Y-%m-%dT%H:%M:%S"))
        except Exception:
            continue
        hours = (now - t) / 3600
        if hours >= LONG_RUN_HOURS:
            name = (st.get("attributes") or {}).get("friendly_name") or eid.split(".")[-1]
            long_running.append((name, hours))
    if not long_running:
        return
    # 同一天只推一条合并提醒（防打扰）
    key = f"long_all_{today}"
    if s.get("pushed", {}).get(key):
        return
    parts = [f"{n}（{int(h)}h）" for n, h in long_running[:4]]
    if len(long_running) > 4:
        parts.append(f"等 {len(long_running)} 个设备")
    _push(f"⏱ 设备提醒：以下设备已持续开启 {int(LONG_RUN_HOURS)}+ 小时：{'、'.join(parts)}，如不需要建议关闭或设置定时")
    s["pushed"][key] = True
    _save_state(s)


def _check_battery():
    """低电量：battery < 20%（每日一次）"""
    today = time.strftime("%Y-%m-%d")
    s = _load_state()
    for st in _ha_states():
        attrs = st.get("attributes") or {}
        bat = attrs.get("battery")
        if not isinstance(bat, (int, float)):
            continue
        if bat < BATTERY_LOW:
            eid = st.get("entity_id", "")
            name = attrs.get("friendly_name") or eid
            key = f"battery_{eid}_{today}"
            if not s.get("pushed", {}).get(key):
                _push(f"🔋 电量提醒：「{name}」电量仅 {int(bat)}%，建议及时充电/换电池")
                s["pushed"][key] = True
                _save_state(s)


def check_once():
    """手动/定时触发一次巡检"""
    _check_weather()
    _check_long_running()
    _check_battery()


def last_suggestion():
    """看板读取最近建议"""
    s = _load_state()
    return s.get("last_suggestion")


def _loop():
    while True:
        try:
            check_once()
        except Exception:
            pass
        time.sleep(CHECK_INTERVAL)


def start_engine():
    t = threading.Thread(target=_loop, daemon=True)
    t.start()
