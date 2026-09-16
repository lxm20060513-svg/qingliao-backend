#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""天气 API：IP 定位（ip-api）→ Open-Meteo 天气（温度+天气码），缓存 30 分钟

GET /api/weather → {"ok": true, "temp": 28.1, "code": 2, "city": "上海"}
天气码映射（WMO）：0 晴 / 1-3 多云 / 45-48 雾 / 51-67 雨 / 71-77 雪 / 80-82 阵雨 / 95-99 雷暴

v3.9.25：新增 daily（今天 + 未来 5 天 = forecast_days=6），供 App 天气弹窗第 2 页使用。
仅**加字段**，temp/code/city 结构与语义不变（旧客户端忽略 daily 即可，向后兼容）。
"""
import json
import os
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler

CACHE_FILE = "/tmp/qingliao_weather_cache.json"
CACHE_TTL = 1800   # 30 分钟
# v3.9.19：坐标兜底——geocoding 与 ip-api 同时抖动时不要再让请求崩掉（用户报"天气失效"）
DEFAULT_LAT, DEFAULT_LON = 31.2304, 121.4737   # 上海


def _fetch(url, timeout=8):
    req = urllib.request.Request(url, headers={"User-Agent": "qingliao/2.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode() or "{}")


def _daily(w):
    """Open-Meteo daily 列 → [{"date","code","max","min"}]（列长度不齐/缺列都安全降级）

    注意 timezone=Asia%2FShanghai 必须带上：不带的话 daily.time 是 UTC 日期，
    东八区傍晚起会整列偏一天。
    """
    d = w.get("daily") or {}
    dates = d.get("time") or []
    codes = d.get("weather_code") or []
    maxs = d.get("temperature_2m_max") or []
    mins = d.get("temperature_2m_min") or []
    out = []
    for i, t in enumerate(dates):
        out.append({
            "date": t,
            "code": codes[i] if i < len(codes) else None,
            "max": maxs[i] if i < len(maxs) else None,
            "min": mins[i] if i < len(mins) else None,
        })
    return out


def _get_weather(lat=None, lon=None, city=None):
    # 缓存（按坐标/城市区分）
    cache_key = ("%.3f,%.3f" % (lat, lon)) if lat is not None else (city or "ip")
    # 城市 → 坐标（Open-Meteo geocoding）
    if city:
        try:
            g = _fetch("https://geocoding-api.open-meteo.com/v1/search?name=%s&count=1&language=zh"
                       % urllib.parse.quote(city), timeout=8)
            rs = (g.get("results") or [])
            if rs:
                lat, lon = rs[0]["latitude"], rs[0]["longitude"]
                # v2.0.101：保留用户输入的城市名（Open-Meteo 搜"南宁"首个结果可能是区级"兴宁区"，覆盖会显示错乱）
        except Exception:
            pass
    try:
        if os.path.exists(CACHE_FILE) and time.time() - os.path.getmtime(CACHE_FILE) < CACHE_TTL:
            with open(CACHE_FILE) as f:
                cached = json.load(f)
                # v3.9.19：null 结果不参与命中——失败缓存会让徽章空半小时（用户报"天气失效"）
                # v3.9.25：旧缓存（无 daily）也不命中——否则升级后头 30 分钟弹窗第 2 页空白
                if cached.get("_key") == cache_key and cached.get("temp") is not None and cached.get("daily"):
                    return cached
    except Exception:
        pass
    # v2.0.87as：默认城市只在无 city 参数时（否则覆盖 geocoding 结果）
    if not city:
        city = "上海"   # 默认
    if lat is None or lon is None:
        # IP 定位
        try:
            loc = _fetch("http://ip-api.com/json/?fields=lat,lon,city", timeout=6)
            if loc.get("status") == "success":
                lat, lon = loc["lat"], loc["lon"]
                city = loc.get("city") or city
        except Exception:
            pass
    elif not city:
        # v2.0.87ag：坐标反查城市（Nominatim，显示具体地点）
        # v2.0.101：仅无手动城市时反查——否则 Nominatim zoom=10 会把"南宁"反查成区级"兴宁区"
        try:
            rev = _fetch("https://nominatim.openstreetmap.org/reverse?lat=%.4f&lon=%.4f"
                         "&format=json&zoom=10" % (lat, lon), timeout=6)
            addr = rev.get("address", {}) or {}
            city = (addr.get("city") or addr.get("town") or addr.get("county") or city)
        except Exception:
            pass
    # v3.9.19：坐标仍缺失（geocoding 与 ip-api 同时失败）→ 先用上次成功的坐标，再退到默认坐标；
    # 原实现会走到 "%.4f" % None 抛 TypeError → 直接返回 null（徽章空掉，旧代码还会把它缓存 30 分钟）
    if lat is None or lon is None:
        try:
            with open(CACHE_FILE) as f:
                old = json.load(f)
            if old.get("lat") is not None and old.get("lon") is not None:
                lat, lon = old["lat"], old["lon"]
                city = city or (old.get("city") or "")
        except Exception:
            pass
    if lat is None or lon is None:
        lat, lon = DEFAULT_LAT, DEFAULT_LON
    try:
        w = _fetch("https://api.open-meteo.com/v1/forecast?latitude=%.4f&longitude=%.4f"
                   "&current=temperature_2m,weather_code"
                   "&daily=weather_code,temperature_2m_max,temperature_2m_min"
                   "&timezone=Asia%%2FShanghai&forecast_days=6" % (lat, lon), timeout=8)
        cur = w.get("current", {})
        data = {"temp": cur.get("temperature_2m"), "code": cur.get("weather_code"),
                "daily": _daily(w),
                "city": city, "lat": lat, "lon": lon, "_key": cache_key}
    except Exception:
        data = {"temp": None, "code": None, "daily": [], "city": city, "_key": cache_key}
    # v3.9.19：上游失败（temp=None）不写缓存，并回落上一次有效值——
    # 原来失败结果也会写进 30 分钟 TTL 缓存，一次网络抖动就让天气"失效"半小时且不重试
    if data.get("temp") is not None:
        try:
            with open(CACHE_FILE, "w") as f:
                json.dump(data, f)
        except Exception:
            pass
    else:
        try:
            with open(CACHE_FILE) as f:
                old = json.load(f)
            if old.get("temp") is not None:
                return old
        except Exception:
            pass
    return data


class WeatherHandler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token")

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _check_auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, "X-Weather-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/api/weather"):
            params = urllib.parse.parse_qs(parsed.query)
            try:
                lat = float(params.get("lat", [""])[0]) if params.get("lat") else None
                lon = float(params.get("lon", [""])[0]) if params.get("lon") else None
            except ValueError:
                lat = lon = None
            city = params.get("city", [""])[0] or None
            d = _get_weather(lat, lon, city)
            d.pop("_key", None)
            self._send(200, {"ok": True, **d})
            return
        self._send(404, {"error": "Not Found"})

    def log_message(self, fmt, *args):
        pass

