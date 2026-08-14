#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""天气 API：IP 定位（ip-api）→ Open-Meteo 天气（温度+天气码），缓存 30 分钟

GET /api/weather → {"ok": true, "temp": 28.1, "code": 2, "city": "上海"}
天气码映射（WMO）：0 晴 / 1-3 多云 / 45-48 雾 / 51-67 雨 / 71-77 雪 / 80-82 阵雨 / 95-99 雷暴
"""
import json
import os
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler

CACHE_FILE = "/tmp/qingliao_weather_cache.json"
CACHE_TTL = 1800   # 30 分钟


def _fetch(url, timeout=8):
    req = urllib.request.Request(url, headers={"User-Agent": "qingliao/2.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode() or "{}")


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
                city = rs[0].get("name") or rs[0].get("admin1") or city
        except Exception:
            pass
    try:
        if os.path.exists(CACHE_FILE) and time.time() - os.path.getmtime(CACHE_FILE) < CACHE_TTL:
            with open(CACHE_FILE) as f:
                cached = json.load(f)
                if cached.get("_key") == cache_key:
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
    else:
        # v2.0.87ag：坐标反查城市（Nominatim，显示具体地点）
        try:
            rev = _fetch("https://nominatim.openstreetmap.org/reverse?lat=%.4f&lon=%.4f"
                         "&format=json&zoom=10" % (lat, lon), timeout=6)
            addr = rev.get("address", {}) or {}
            city = (addr.get("city") or addr.get("town") or addr.get("county") or city)
        except Exception:
            pass
    try:
        w = _fetch("https://api.open-meteo.com/v1/forecast?latitude=%.4f&longitude=%.4f"
                   "&current=temperature_2m,weather_code" % (lat, lon), timeout=8)
        cur = w.get("current", {})
        data = {"temp": cur.get("temperature_2m"), "code": cur.get("weather_code"),
                "city": city, "_key": cache_key}
    except Exception:
        data = {"temp": None, "code": None, "city": city, "_key": cache_key}
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(data, f)
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
