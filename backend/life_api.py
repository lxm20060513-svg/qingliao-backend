#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""生活数据卡片 API v2：股票行情 / 资讯 RSS / 快递查询 / 价格监控（全部可 App 内增删改）

端点（均需鉴权，与其它模块一致走 X-Auth-Token）：
  GET  /api/life/cards[?fresh=1]      → {"ok":bool,"ts":Int,"cards":[…]}   看板取数
  GET  /api/life/config               → {"ok":true,"config":{…},"presets":{…}}  设置页取配置
  POST /api/life/config {"config":{…}}→ 规范化 + 落盘，返回生效配置（App 保存）
  GET  /api/life/stock/search?q=关键词 → 东财 suggest 搜股票（加卡片时选标的）
  POST /api/life/article {"url","title"[,"fresh"]}
                                      → {"ok":bool,"title","content","source":"ai"|"raw",…}
                                        单条资讯正文（后端抓取 + 模型整理，按 URL 缓存 6h）
  POST /api/life/price/test {"url","pattern","group","extract","path","headers"}
                                      → 正则/JSON 路径试抓（保存规则前先验一次）

卡片结构（kind 区分类型，UI 按 kind 渲染）：
  {"kind":"stock","id":"1.601138","market":"1","code":"601138","name":"工业富联",
   "price":63.91,"prev_close":65.24,"change":-1.33,"change_pct":-2.04,
   "currency":"CNY","ok":true,"error":""}
  {"kind":"rss","id":"rss","title":"博客/资讯","ok":true,
   "entries":[{"source":"少数派","title":"…","link":"https://…","published":"…"}],
   "sources":[{"name":"少数派","ok":true,"error":"","count":4}]}
  {"kind":"express","id":"express","title":"快递","ok":true,
   "packages":[{"no":"YT…","carrier":"yuantong","carrierName":"圆通速递","name":"我的快递",
                "state":"3","stateText":"已签收","latest":{"time":"…","context":"…"},
                "ok":true,"error":""}],"error":""}
  {"kind":"price","id":"price","title":"价格监控","ok":true,
   "items":[{"name":"…","url":"…","price":129.0,"currency":"CNY","ok":true,"error":"",
             "target":100.0,"hit":false}],"error":""}

约定：单条目/单源失败不影响其它；全部失败才 ok:false + error。上游请求短超时，
整体收集有硬上限（不死等），失败降级为条目内 ok:false + error 文本。
仅标准库（urllib + xml.etree + hashlib），无第三方依赖。
落盘配置：与本文件同目录的 life_config.json（改完即生效，无需重启进程）。
"""
import hashlib
import json
import os
import re
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from html import unescape
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------------------------------------------------------------- 路径与参数
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "life_config.json")

HTTP_TIMEOUT = 5          # 单个上游请求超时（秒）
COLLECT_SLACK = 2         # 整体收集相对单个超时的宽限（秒）
STOCK_TTL = 60            # 行情缓存 60s
RSS_TTL = 900             # RSS 缓存 15 分钟
EXPRESS_TTL = 300         # 快递缓存 5 分钟（物流更新频率低）
PRICE_TTL = 900           # 价格缓存 15 分钟
MAX_ENTRIES_PER_FEED = 4  # 每个源取多少条
MAX_ENTRIES = 8           # 合并后最多返回多少条（源多时保证首页只显示最近的）

MAX_STOCKS = 20
MAX_FEEDS = 20

# v3.6.2：资讯正文（AI 后台拉取）——抓 HTML → 清洗正文 → 交给模型整理
ARTICLE_TTL = 6 * 3600          # 同一 URL 正文缓存 6 小时（少抓、少花钱）
ARTICLE_FAIL_TTL = 600          # 失败只缓存 10 分钟（允许稍后重试）
ARTICLE_FETCH_TIMEOUT = 10      # 抓网页超时（秒）
ARTICLE_MAX_CHARS = 3500        # 送模型的正文上限（控制耗时与响应体大小）
ARTICLE_MIN_CHARS = 120         # 正文短于此 = 抓取失败
MAX_PACKAGES = 20
MAX_ITEMS = 20

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126 Safari/537.36")

# 东方财富公开行情端点（延迟端点在家宽/容器网络下更稳）
EM_HOSTS = ("push2delay.eastmoney.com", "push2.eastmoney.com")
EM_UT = "fa5fd1943c7b386f172d6893dbfba10b"
EM_FIELDS = "f43,f44,f45,f46,f47,f48,f57,f58,f59,f60,f116,f169,f170"
EM_SUGGEST = ("https://searchapi.eastmoney.com/api/suggest/get"
              "?input={q}&type=14&token=D43BF722C8E33BDC906FB84D85E326E8&count=10")

# 快递100 免费网页接口（免 key，实测可用）；type=快递公司代码，postid=单号
KUAIDI100_FREE = ("https://www.kuaidi100.com/query"
                  "?type={carrier}&postid={no}&temp={ts}&resultv2=4&phone={phone}")

# ---------------------------------------------------------------- 市场 / 快递 代码表
MARKETS = (("1", "沪A"), ("0", "深A"), ("105", "纳斯达克"), ("106", "纽交所"),
           ("107", "美交所"), ("116", "港股"))
CURRENCY = {"0": "CNY", "1": "CNY", "116": "HKD"}

CARRIERS = (
    ("sf", "顺丰速运"), ("yuantong", "圆通速递"), ("yunda", "韵达速递"),
    ("zhongtong", "中通快递"), ("shentong", "申通快递"), ("jd", "京东物流"),
    ("jtexpress", "极兔速递"), ("ems", "EMS"), ("youzhengguonei", "邮政快递包裹"),
    ("debangwuliu", "德邦快递"), ("huitongkuaidi", "百世快递"), ("tiantian", "天天快递"),
    ("zhaijisong", "宅急送"), ("ane66", "安能物流"), ("youshuwuliu", "优速快递"),
)
CARRIER_NAME = dict(CARRIERS)

STATE_TEXT = {"0": "在途", "1": "已揽收", "2": "疑难件", "3": "已签收", "4": "已退签",
              "5": "派送中", "6": "已退回", "7": "转投中", "8": "清关中", "14": "已拒签"}

# ---------------------------------------------------------------- 内置默认源
DEFAULT_STOCK_PRESETS = (
    {"market": "1", "code": "000001", "name": "上证指数"},
    {"market": "1", "code": "601138", "name": "工业富联"},
    {"market": "0", "code": "300750", "name": "宁德时代"},
    {"market": "116", "code": "00700", "name": "腾讯控股"},
    {"market": "105", "code": "AAPL", "name": "苹果"},
    {"market": "105", "code": "NVDA", "name": "英伟达"},
    {"market": "105", "code": "TSLA", "name": "特斯拉"},
    {"market": "1", "code": "600519", "name": "贵州茅台"},
    {"market": "0", "code": "002594", "name": "比亚迪"},
)

# 资讯源预设库（App「添加资讯源」一键选）；带 builtin=True 的是默认启用的
RSS_CATALOG = (
    {"name": "少数派", "url": "https://sspai.com/feed", "builtin": True},
    {"name": "IT之家", "url": "https://www.ithome.com/rss/", "builtin": True},
    {"name": "爱范儿", "url": "https://www.ifanr.com/feed", "builtin": True},
    {"name": "机核", "url": "https://www.gcores.com/rss", "builtin": True},
    {"name": "Solidot", "url": "https://www.solidot.org/index.rss", "builtin": True},
    {"name": "Hacker News", "url": "https://hnrss.org/frontpage", "builtin": True},
    {"name": "华尔街见闻", "url": "https://dedicated.wallstreetcn.com/rss.xml", "builtin": True},
    {"name": "阮一峰", "url": "https://www.ruanyifeng.com/blog/atom.xml", "builtin": True},
    {"name": "TechCrunch", "url": "https://techcrunch.com/feed/"},
    {"name": "The Verge", "url": "https://www.theverge.com/rss/index.xml"},
    {"name": "Apple Newsroom", "url": "https://www.apple.com/newsroom/rss-feed.rss"},
)

DEFAULT_EXPRESS_SOURCE = {
    "type": "free",          # free=快递100 免费接口；custom=自定义 URL + JSON 字段路径
    "url_template": "",      # custom 用：支持 {no} {carrier} {key} {phone} 占位
    "headers": {},           # custom 用：附加请求头（如 Referer / Cookie）
    "key": "",               # custom 用：密钥（写进 {key} 占位）
    "list_path": "data",     # 轨迹数组在 JSON 里的路径（点号分隔）
    "time_key": "time",
    "context_key": "context",
    "state_path": "state",
}


def _default_config():
    return {
        "version": 2,
        "stocks": [{"market": m, "code": c} for m, c in
                   (("1", "601138"), ("0", "300750"), ("116", "00700"), ("105", "AAPL"))],
        "rss": [{"name": f["name"], "url": f["url"]} for f in RSS_CATALOG if f.get("builtin")],
        "express": {"source": dict(DEFAULT_EXPRESS_SOURCE), "packages": []},
        "price": {"source": {"headers": {}, "timeout": 8}, "items": []},
    }


# ---------------------------------------------------------------- 配置读写（规范化 + 原子落盘）
_CFG_LOCK = threading.Lock()
_CFG_CACHE = {"sig": None, "cfg": None}


def _s(v, limit=200):
    return str(v if v is not None else "").strip()[:limit]


def _norm_stocks(raw):
    out, seen = [], set()
    for it in (raw or []):
        if not isinstance(it, dict):
            continue
        market = _s(it.get("market"), 6)
        code = _s(it.get("code"), 16).upper()
        if market not in dict(MARKETS) or not code or not re.fullmatch(r"[0-9A-Z.]+", code):
            continue
        sid = "%s.%s" % (market, code)
        if sid in seen:
            continue
        seen.add(sid)
        out.append({"market": market, "code": code})
        if len(out) >= MAX_STOCKS:
            break
    return out


def _norm_rss(raw):
    out, seen = [], set()
    for it in (raw or []):
        if not isinstance(it, dict):
            continue
        url = _s(it.get("url"), 500)
        if not re.match(r"^https?://", url, re.I):
            continue
        if url in seen:
            continue
        seen.add(url)
        name = _s(it.get("name"), 40) or urllib.parse.urlparse(url).netloc
        out.append({"name": name, "url": url})
        if len(out) >= MAX_FEEDS:
            break
    return out


def _norm_express(raw):
    raw = raw if isinstance(raw, dict) else {}
    src = raw.get("source") if isinstance(raw.get("source"), dict) else {}
    source = {
        "type": "custom" if _s(src.get("type")) == "custom" else "free",
        "url_template": _s(src.get("url_template"), 800),
        "headers": {(_s(k, 60)): _s(v, 400) for k, v in
                    (src.get("headers") if isinstance(src.get("headers"), dict) else {}).items()
                    if _s(k)},
        "key": _s(src.get("key"), 200),
        "list_path": _s(src.get("list_path"), 120) or "data",
        "time_key": _s(src.get("time_key"), 60) or "time",
        "context_key": _s(src.get("context_key"), 60) or "context",
        "state_path": _s(src.get("state_path"), 120) or "state",
    }
    pkgs, seen = [], set()
    for it in (raw.get("packages") or []):
        if not isinstance(it, dict):
            continue
        no = _s(it.get("no"), 60)
        if not no or no in seen:
            continue
        seen.add(no)
        carrier = _s(it.get("carrier"), 40)
        pkgs.append({"no": no, "carrier": carrier,
                     "name": _s(it.get("name"), 40) or (CARRIER_NAME.get(carrier) or no)})
        if len(pkgs) >= MAX_PACKAGES:
            break
    return {"source": source, "packages": pkgs}


def _norm_price(raw):
    raw = raw if isinstance(raw, dict) else {}
    src = raw.get("source") if isinstance(raw.get("source"), dict) else {}
    try:
        to = int(src.get("timeout") or 8)
    except Exception:
        to = 8
    source = {
        "headers": {(_s(k, 60)): _s(v, 400) for k, v in
                    (src.get("headers") if isinstance(src.get("headers"), dict) else {}).items()
                    if _s(k)},
        "timeout": max(3, min(20, to)),
    }
    items, seen = [], set()
    for it in (raw.get("items") or []):
        if not isinstance(it, dict):
            continue
        url = _s(it.get("url"), 800)
        # v3.6.3 修复「点添加价格监控后卡片立刻回退」：URL 尚未填写的未完成项必须保留。
        # App 的添加是「先追加空卡片 → 落库」，原实现把非 http 开头的项直接丢弃，
        # POST 回读的配置里没有这条 → App 用回读值覆盖本地 → 刚加的卡片瞬间消失。
        # 填了 URL 才做合法性校验与去重；url 为空 = 用户还在填，原样保留。
        if url:
            if not re.match(r"^https?://", url, re.I) or url in seen:
                continue
            seen.add(url)
        extract = "json" if _s(it.get("extract")) == "json" else "regex"
        try:
            group = int(it.get("group") or 1)
        except Exception:
            group = 1
        try:
            target = float(it.get("target")) if it.get("target") not in (None, "") else None
        except Exception:
            target = None
        items.append({
            "name": _s(it.get("name"), 60) or urllib.parse.urlparse(url).netloc,
            "url": url,
            "extract": extract,
            "pattern": _s(it.get("pattern"), 400),
            "path": _s(it.get("path"), 200),
            "group": max(0, min(9, group)),
            "currency": _s(it.get("currency"), 6) or "CNY",
            "target": target,
        })
        if len(items) >= MAX_ITEMS:
            break
    return {"source": source, "items": items}


def normalize_config(raw):
    raw = raw if isinstance(raw, dict) else {}
    return {
        "version": 2,
        "stocks": _norm_stocks(raw.get("stocks")),
        "rss": _norm_rss(raw.get("rss")),
        "express": _norm_express(raw.get("express")),
        "price": _norm_price(raw.get("price")),
    }


def load_config():
    """读盘（按 mtime+size 缓存；文件缺失/损坏 → 内置默认）。"""
    try:
        st = os.stat(CONFIG_PATH)
        sig = "%d:%d" % (st.st_mtime_ns, st.st_size)
    except OSError:
        sig = "missing"
    with _CFG_LOCK:
        if _CFG_CACHE["sig"] == sig and _CFG_CACHE["cfg"] is not None:
            return _CFG_CACHE["cfg"]
    cfg = None
    if sig != "missing":
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg = normalize_config(json.load(f))
        except Exception as e:
            print("[life] 配置读取失败，改用默认配置: %s" % e)
    if cfg is None:
        cfg = normalize_config(_default_config())
    with _CFG_LOCK:
        _CFG_CACHE["sig"] = sig
        _CFG_CACHE["cfg"] = cfg
    return cfg


def save_config(raw):
    cfg = normalize_config(raw)
    tmp = CONFIG_PATH + ".tmp"
    with _CFG_LOCK:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        os.replace(tmp, CONFIG_PATH)
        _CFG_CACHE["sig"] = None
        _CFG_CACHE["cfg"] = None
    _CACHE.clear()
    return cfg


def _sig():
    """配置签名，参与缓存 key（改配置立即失效）。"""
    cfg = load_config()
    h = hashlib.md5(json.dumps(cfg, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:8]
    return h


# ---------------------------------------------------------------- 工具
def _get(url, timeout=HTTP_TIMEOUT, headers=None, data=None, method=None):
    h = {"User-Agent": UA, "Accept": "*/*"}
    if headers:
        h.update({str(k): str(v) for k, v in headers.items()})
    req = urllib.request.Request(url, headers=h, data=data, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _num(v):
    """数值容错：停牌时东财可能返回 '-' / None / 字符串。"""
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).strip())
    except Exception:
        return None


def _decimals(data, market):
    d = data.get("f59")
    if isinstance(d, int) and 0 <= d <= 6:
        return d
    return 2 if market in ("0", "1") else 3


def _scaled(v, decimals):
    f = _num(v)
    if f is None:
        return None
    return round(f / float(10 ** decimals), max(decimals, 2))


def _dig(obj, path):
    """点号路径取值： 'data.list' / 'a.0.b'；空路径返回原对象。"""
    if not path:
        return obj
    cur = obj
    for part in str(path).split("."):
        if not part:
            continue
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list):
            try:
                cur = cur[int(part)]
            except Exception:
                return None
        else:
            return None
        if cur is None:
            return None
    return cur


def _collect_one(fn, items, timeout, tag):
    """并发跑 items，整体硬上限 timeout；超时/异常的条目降级返回 None 占位。"""
    results = [None] * len(items)
    if not items:
        return results
    ex = ThreadPoolExecutor(max_workers=min(6, len(items)))
    try:
        futs = {ex.submit(fn, it, i): i for i, it in enumerate(items)}
        try:
            for fut in as_completed(futs, timeout=timeout):
                i = futs[fut]
                try:
                    results[i] = fut.result()
                except Exception as e:
                    results[i] = {"_error": "%s: %s" % (tag, e)}
        except FuturesTimeout:
            pass
    finally:
        ex.shutdown(wait=False)
    return results


# ---------------------------------------------------------------- 股票
def _mk_stock(market, code):
    return {"kind": "stock", "id": "%s.%s" % (market, code), "market": market, "code": code,
            "name": code, "currency": CURRENCY.get(market, "USD"), "ok": False,
            "error": "行情获取失败"}


def _fetch_stock(item, index=0):
    market, code = item["market"], item["code"]
    secid = "%s.%s" % (market, code)
    data, last_err = None, ""
    for host in EM_HOSTS:
        url = ("https://%s/api/qt/stock/get?secid=%s&fields=%s&ut=%s"
               % (host, secid, EM_FIELDS, EM_UT))
        try:
            j = json.loads(_get(url).decode("utf-8", "ignore") or "{}")
            data = j.get("data")
            if data:
                break
            last_err = "上游无数据(rc=%s)" % j.get("rc")
        except Exception as e:
            last_err = "%s 不可用: %s" % (host, e)
    if not data:
        return {"kind": "stock", "id": secid, "market": market, "code": code,
                "name": code, "currency": CURRENCY.get(market, "USD"),
                "ok": False, "error": last_err or "行情获取失败"}

    dec = _decimals(data, market)
    price = _scaled(data.get("f43"), dec)
    prev = _scaled(data.get("f60"), dec)
    change = _scaled(data.get("f169"), dec)
    pct = _num(data.get("f170"))
    ok = price is not None and price > 0
    return {
        "kind": "stock", "id": secid, "market": market, "code": code,
        "name": (data.get("f58") or code),
        "price": price,
        "prev_close": prev,
        "change": change,
        "change_pct": (round(pct / 100.0, 2) if pct is not None else None),
        "open": _scaled(data.get("f46"), dec),
        "high": _scaled(data.get("f44"), dec),
        "low": _scaled(data.get("f45"), dec),
        "volume": _num(data.get("f47")),
        "amount": _num(data.get("f48")),
        "market_cap": _num(data.get("f116")),
        "currency": CURRENCY.get(market, "USD"),
        "ok": ok,
        "error": "" if ok else "行情未就绪",
    }


def _collect_stocks(cfg):
    wl = cfg["stocks"]
    out = _collect_one(_fetch_stock, wl, HTTP_TIMEOUT + COLLECT_SLACK, "stock")
    for i, r in enumerate(out):
        if not isinstance(r, dict) or "kind" not in r:
            base = _mk_stock(wl[i]["market"], wl[i]["code"])
            base["error"] = (r or {}).get("_error") or "超时"
            out[i] = base
    return out


def search_stocks(q):
    """东财 suggest 搜股票（行情/指数/港美股都能搜到）。"""
    q = _s(q, 40)
    if not q:
        return []
    j = json.loads(_get(EM_SUGGEST.format(q=urllib.parse.quote(q)), timeout=8).decode("utf-8", "ignore") or "{}")
    rows = _dig(j, "QuotationCodeTable.Data") or []
    out = []
    for r in rows:
        code = _s(r.get("Code"), 16)
        market = _s(r.get("MktNum"), 6)
        if not code or market not in dict(MARKETS):
            continue
        out.append({"code": code, "market": market,
                    "marketName": dict(MARKETS).get(market, market),
                    "name": _s(r.get("Name"), 40),
                    "type": _s(r.get("SecurityTypeName"), 20)})
        if len(out) >= 10:
            break
    return out


# ---------------------------------------------------------------- RSS / Atom
def _local(tag):
    return str(tag).rsplit("}", 1)[-1].lower()


def _text(el):
    try:
        return "".join(el.itertext()).strip()
    except Exception:
        return ""


def _clean(s, limit=160):
    s = re.sub(r"<[^>]+>", " ", s or "")
    s = re.sub(r"\s+", " ", s).strip()
    s = (s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
          .replace("&quot;", '"').replace("&#39;", "'"))
    return s[:limit]


def _iso(s):
    """RSS pubDate(RFC822) / Atom published(ISO8601) → UTC ISO8601。"""
    s = (s or "").strip()
    if not s:
        return ""
    dt = None
    try:
        dt = parsedate_to_datetime(s)
    except Exception:
        dt = None
    if dt is None:
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_feed(raw):
    root = ET.fromstring(raw)          # 传 bytes：让 ET 自行按 XML 声明解码
    out = []
    for el in root.iter():
        if _local(el.tag) not in ("item", "entry"):
            continue
        title, link, date = "", "", ""
        for ch in list(el):
            n = _local(ch.tag)
            if n == "title" and not title:
                title = _text(ch)
            elif n == "link" and not link:
                href = ch.get("href")
                if href:                # Atom
                    if (ch.get("rel") or "alternate") == "alternate":
                        link = href
                elif (ch.text or "").strip():
                    link = ch.text.strip()
            elif n in ("pubdate", "published", "updated", "date") and not date:
                date = _text(ch)
        out.append({"title": _clean(title, 120), "link": link.strip(), "published": _iso(date)})
    return out


def _fetch_feed(feed, index=0):
    name, url = feed.get("name") or "RSS", feed.get("url") or ""
    try:
        raw = _get(url, timeout=HTTP_TIMEOUT + COLLECT_SLACK)
    except Exception as e:
        return {"name": name, "ok": False, "error": "抓取失败: %s" % e, "entries": []}
    items = []
    try:
        items = _parse_feed(raw)
    except Exception:
        # 声明编码非 UTF-8 且解析失败：按内容解码后再试一次（仅对「编码问题」有效）
        try:
            m = re.search(r'encoding=["\']([\w\-]+)["\']', raw[:200].decode("ascii", "ignore"))
            enc = (m.group(1) if m else "utf-8")
            items = _parse_feed(raw.decode(enc, "ignore").encode("utf-8"))
        except Exception as e:
            return {"name": name, "ok": False, "error": "解析失败(不符合 RSS/Atom): %s" % e,
                    "entries": []}
    if not items:
        return {"name": name, "ok": False, "error": "无条目", "entries": []}
    dated = sorted([i for i in items if i.get("published")],
                   key=lambda x: x["published"], reverse=True)
    undated = [i for i in items if not i.get("published")]
    picked = (dated + undated)[:MAX_ENTRIES_PER_FEED]
    for i in picked:
        i["source"] = name
    return {"name": name, "ok": True, "error": "", "entries": picked}


def _collect_feeds(cfg):
    feeds = cfg["rss"]
    if not feeds:
        return {"kind": "rss", "id": "rss", "title": "资讯", "ok": False,
                "entries": [], "sources": [], "error": "未添加资讯源"}
    out = _collect_one(_fetch_feed, feeds, HTTP_TIMEOUT + COLLECT_SLACK * 2, "rss")
    norm = []
    for i, r in enumerate(out):
        if not isinstance(r, dict) or "entries" not in r:
            norm.append({"name": feeds[i].get("name") or "RSS", "ok": False,
                         "error": (r or {}).get("_error") or "超时", "entries": []})
        else:
            norm.append(r)
    entries, sources = [], []
    for f in norm:
        sources.append({"name": f["name"], "ok": bool(f.get("ok")),
                        "error": f.get("error") or "", "count": len(f.get("entries") or [])})
        entries.extend(f.get("entries") or [])
    entries.sort(key=lambda e: e.get("published") or "", reverse=True)
    entries = entries[:MAX_ENTRIES]
    ok = any(f.get("ok") for f in norm)
    bad = [f["name"] for f in norm if not f.get("ok")]
    return {"kind": "rss", "id": "rss", "title": "资讯", "ok": ok,
            "entries": entries, "sources": sources,
            "error": "" if ok else "全部订阅源获取失败（%s）" % "、".join(bad[:3])}


# ---------------------------------------------------------------- 快递
def _fetch_package(item, index=0):
    cfg = load_config()
    src = cfg["express"]["source"]
    no = item.get("no") or ""
    carrier = item.get("carrier") or ""
    phone = item.get("phone") or ""
    base = {"no": no, "carrier": carrier,
            "carrierName": CARRIER_NAME.get(carrier, carrier or "快递"),
            "name": item.get("name") or no, "ok": False, "error": "", "state": "",
            "stateText": "", "latest": None}

    if src.get("type") == "custom":
        tpl = src.get("url_template") or ""
        if not tpl or "{no}" not in tpl:
            base["error"] = "自定义源未配置 URL 模板（需含 {no} 占位）"
            return base
        url = (tpl.replace("{no}", urllib.parse.quote(no))
                  .replace("{carrier}", urllib.parse.quote(carrier))
                  .replace("{key}", urllib.parse.quote(src.get("key") or ""))
                  .replace("{phone}", urllib.parse.quote(phone)))
        try:
            j = json.loads(_get(url, timeout=HTTP_TIMEOUT + COLLECT_SLACK,
                                headers=src.get("headers") or {}).decode("utf-8", "ignore") or "{}")
        except Exception as e:
            base["error"] = "查询失败: %s" % e
            return base
        rows = _dig(j, src.get("list_path") or "data") or []
        state = _dig(j, src.get("state_path") or "state")
        time_key = src.get("time_key") or "time"
        ctx_key = src.get("context_key") or "context"
    else:
        url = KUAIDI100_FREE.format(carrier=urllib.parse.quote(carrier), no=urllib.parse.quote(no),
                                    ts=round(time.time() % 1000, 3), phone=urllib.parse.quote(phone))
        try:
            j = json.loads(_get(url, timeout=HTTP_TIMEOUT + COLLECT_SLACK,
                                headers={"Referer": "https://www.kuaidi100.com/"}).decode("utf-8", "ignore") or "{}")
        except Exception as e:
            base["error"] = "查询失败: %s" % e
            return base
        if str(j.get("status")) not in ("200", "0") or j.get("message") not in ("ok", "", None):
            base["error"] = _s(j.get("message") or "查询失败", 60)
            return base
        rows = j.get("data") or []
        state = j.get("state")
        time_key, ctx_key = "time", "context"

    if not isinstance(rows, list):
        base["error"] = "返回结构无法识别（列表路径: %s）" % (src.get("list_path") or "data")
        return base
    if not rows:
        base["error"] = "暂无物流信息"
        return base
    latest = rows[0] if isinstance(rows[0], dict) else {}
    ctx = _clean(latest.get(ctx_key), 120)
    # 快递100 对不存在的单号也会返回一条「查无结果」轨迹 + state=3，不能当成已签收
    if "查无结果" in ctx or "无结果" in ctx or "no result" in ctx.lower():
        base["error"] = "查无此单号（核对单号与快递公司）"
        base["latest"] = {"time": _s(latest.get(time_key), 40), "context": ctx}
        return base
    base["ok"] = True
    base["state"] = _s(state, 6)
    base["stateText"] = STATE_TEXT.get(_s(state, 6), "已查询")
    base["latest"] = {"time": _s(latest.get(time_key), 40), "context": ctx}
    return base


def _collect_express(cfg):
    src = cfg["express"]
    pkgs = src["packages"]
    if not pkgs:
        return {"kind": "express", "id": "express", "title": "快递", "ok": False,
                "packages": [], "error": "未添加快递单号", "hint": "设置 → 生活卡片 → 快递"}
    out = _collect_one(_fetch_package, pkgs, HTTP_TIMEOUT + COLLECT_SLACK * 3, "express")
    norm = []
    for i, r in enumerate(out):
        if not isinstance(r, dict) or "no" not in r:
            p = pkgs[i]
            norm.append({"no": p.get("no", ""), "carrier": p.get("carrier", ""),
                         "carrierName": CARRIER_NAME.get(p.get("carrier", ""), "快递"),
                         "name": p.get("name") or p.get("no", ""), "ok": False,
                         "error": (r or {}).get("_error") or "超时", "state": "",
                         "stateText": "", "latest": None})
        else:
            norm.append(r)
    ok = any(p.get("ok") for p in norm)
    bad = [p["no"] for p in norm if not p.get("ok")]
    return {"kind": "express", "id": "express", "title": "快递", "ok": ok,
            "packages": norm, "error": "" if ok else ("查询失败: %s" % "、".join(bad[:3]))}


# ---------------------------------------------------------------- 价格监控
def _extract_price(text, item, src):
    if item.get("extract") == "json":
        try:
            j = json.loads(text)
        except Exception as e:
            return None, "返回不是 JSON: %s" % e
        v = _dig(j, item.get("path") or "")
        p = _num(v)
        return (p, "" if p is not None else "JSON 路径未取到数值: %s" % item.get("path"))
    pat = item.get("pattern") or ""
    if not pat:
        return None, "未配置提取规则"
    try:
        m = re.search(pat, text, re.S)
    except re.error as e:
        return None, "正则错误: %s" % e
    if not m:
        return None, "页面里没匹配到（规则需按该页面实际内容调整）"
    g = item.get("group") or 0
    try:
        raw = m.group(g)
    except Exception:
        return None, "分组号 %d 不存在" % g
    digits = re.findall(r"\d+(?:\.\d+)?", (raw or "").replace(",", ""))
    if not digits:
        return None, "匹配到「%s」但里面没有数字" % _clean(raw, 30)
    return float(digits[0]), ""


def _fetch_price(item, index=0):
    cfg = load_config()
    src = cfg["price"]["source"]
    base = {"name": item.get("name") or "", "url": item.get("url") or "",
            "price": None, "currency": item.get("currency") or "CNY",
            "target": item.get("target"), "hit": False, "ok": False, "error": ""}
    # v3.6.3：未完成项（URL 还没填）给友好提示，别去请求空地址
    if not (item.get("url") or "").strip():
        base["error"] = "未填写商品 URL"
        return base
    try:
        raw = _get(item["url"], timeout=src.get("timeout") or 8, headers=src.get("headers") or {})
    except Exception as e:
        base["error"] = "抓取失败: %s" % e
        return base
    text = raw.decode("utf-8", "ignore")
    price, err = _extract_price(text, item, src)
    if price is None:
        base["error"] = err
        return base
    base["price"] = round(price, 2)
    base["ok"] = True
    if item.get("target") is not None:
        base["hit"] = price <= float(item["target"])
    return base


def _collect_price(cfg):
    items = cfg["price"]["items"]
    if not items:
        return {"kind": "price", "id": "price", "title": "价格监控", "ok": False,
                "items": [], "error": "未添加监控商品", "hint": "设置 → 生活卡片 → 价格监控"}
    out = _collect_one(_fetch_price, items, HTTP_TIMEOUT + COLLECT_SLACK * 3, "price")
    norm = []
    for i, r in enumerate(out):
        if not isinstance(r, dict) or "url" not in r:
            it = items[i]
            norm.append({"name": it.get("name") or "", "url": it.get("url") or "", "price": None,
                         "currency": it.get("currency") or "CNY", "target": it.get("target"),
                         "hit": False, "ok": False, "error": (r or {}).get("_error") or "超时"})
        else:
            norm.append(r)
    ok = any(p.get("ok") for p in norm)
    return {"kind": "price", "id": "price", "title": "价格监控", "ok": ok,
            "items": norm, "error": "" if ok else "全部商品获取失败"}


# ---------------------------------------------------------------- v3.6.2 资讯正文
_ARTICLE_CACHE = {}   # url_md5 -> (ts, payload)

_SCRIPT_RE = re.compile(r"<(script|style|noscript|svg|iframe|template)\b.*?</\1>", re.S | re.I)
_COMMENT_RE = re.compile(r"<!--.*?-->", re.S)
_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"[ \t\u00a0]+")
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.S | re.I)
_ARTICLE_RE = re.compile(r"<article\b[^>]*>(.*?)</article>", re.S | re.I)
_PARA_RE = re.compile(r"<p\b[^>]*>(.*?)</p>", re.S | re.I)


def _extract_article(html):
    """极简正文提取（纯标准库）：优先 <article> 块，退化到全页 <p> 段落。
    返回 (页面标题, 正文文本)；只做去标签 + 丢弃导航短句，不做站点适配。"""
    raw = html or ""
    raw = _COMMENT_RE.sub(" ", raw)
    raw = _SCRIPT_RE.sub(" ", raw)
    m = _TITLE_RE.search(raw)
    page_title = unescape(_TAG_RE.sub("", m.group(1))).strip() if m else ""
    blocks = _ARTICLE_RE.findall(raw)
    scope = max(blocks, key=len) if blocks else raw
    paras = _PARA_RE.findall(scope)
    if not paras:
        paras = [scope]
    out = []
    for p in paras:
        t = unescape(_TAG_RE.sub(" ", p))
        t = _SPACE_RE.sub(" ", t).strip()
        if len(t) >= 20:              # 丢导航/按钮/版权等短句
            out.append(t)
    text = "\n".join(out).strip()
    return page_title, re.sub(r"\n{3,}", "\n\n", text)


def _ai_tidy(title, text):
    """把正文交给模型整理（去导航/广告、保留完整内容）。
    模型不可用/判定失败 → 返回空串，调用方降级用清洗后的原文。"""
    try:
        import stream_api               # 同进程模块（qingliao_all.py 一并 import）
        prompt = ("下面是从网页抓取并已去掉 HTML 标签的文章正文。请输出这篇文章的完整内容："
                  "保留原文段落结构与全部信息、数据、人名，只删除导航、广告、版权声明、"
                  "推荐阅读之类噪音。不要总结、不要评论、不要客套、不要加任何前缀说明。"
                  "如果这段文本明显不是正文（乱码、只有登录或验证提示、内容过短），"
                  "只回复四个字：无法提取。\n\n标题：" + (title or "(无)") + "\n正文：\n" + text)
        body = {"model": stream_api.AGENT_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "stream": False, "max_tokens": 1400}
        resp = stream_api._chat_once(body)
        out = ((resp.get("choices") or [{}])[0].get("message", {}) or {}).get("content", "") or ""
        out = out.strip()
        if len(out) < ARTICLE_MIN_CHARS or "无法提取" in out[:12]:
            return ""
        return out
    except Exception:
        return ""


def _fetch_article(url, title="", fresh=False):
    """单条资讯正文：抓 HTML → 提正文 → 模型整理；按 URL 缓存（成功 6h / 失败 10min）。"""
    url = _s(url, 1000)
    title = _s(title, 200)
    if not re.match(r"^https?://", url, re.I):
        return {"ok": False, "error": "仅支持 http/https 链接"}
    key = hashlib.md5(url.encode("utf-8")).hexdigest()[:12]
    ts, val = _ARTICLE_CACHE.get(key, (0.0, None))
    ttl = ARTICLE_TTL if (val or {}).get("ok") else ARTICLE_FAIL_TTL
    if not fresh and val is not None and (time.time() - ts) < ttl:
        return dict(val, cached=True)
    try:
        raw = _get(url, timeout=ARTICLE_FETCH_TIMEOUT,
                   headers={"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"})
    except Exception as e:
        payload = {"ok": False, "url": url, "error": "抓取失败: %s" % str(e)[:120]}
        _ARTICLE_CACHE[key] = (time.time(), payload)
        return payload
    html = raw.decode("utf-8", "ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
    page_title, text = _extract_article(html)
    if len(text) < ARTICLE_MIN_CHARS:
        payload = {"ok": False, "url": url, "title": page_title or title,
                   "error": "该页面抓不到正文（可能需 JS 渲染或反爬）"}
        _ARTICLE_CACHE[key] = (time.time(), payload)
        return payload
    feed = text[:ARTICLE_MAX_CHARS]
    ai = _ai_tidy(title or page_title, feed)
    if ai:
        payload = {"ok": True, "url": url, "title": title or page_title, "content": ai,
                   "source": "ai", "raw_chars": len(text), "chars": len(ai),
                   "truncated": len(text) > ARTICLE_MAX_CHARS, "ts": int(time.time())}
    else:
        payload = {"ok": True, "url": url, "title": title or page_title, "content": feed,
                   "source": "raw", "raw_chars": len(text), "chars": len(feed),
                   "truncated": len(text) > ARTICLE_MAX_CHARS, "ts": int(time.time())}
    _ARTICLE_CACHE[key] = (time.time(), payload)
    return payload


# ---------------------------------------------------------------- 缓存 + 汇总
_CACHE = {}


def _cached(key, ttl, builder, fresh=False):
    ts, val = _CACHE.get(key, (0.0, None))
    if not fresh and val is not None and (time.time() - ts) < ttl:
        return val
    try:
        val = builder()
    except Exception as e:
        val = {"_error": str(e)}
    _CACHE[key] = (time.time(), val)
    return val


def _placeholder(kind, title, err, hint=""):
    return {"kind": kind, "id": kind, "title": title, "ok": False,
            "error": err, "hint": hint, "configured": False}


def _collect(fresh=False):
    cfg = load_config()
    sig = _sig()
    cards = []
    stocks = _cached("stock:" + sig, STOCK_TTL, lambda: _collect_stocks(cfg), fresh)
    if isinstance(stocks, list) and stocks:
        cards.extend(stocks)
    elif isinstance(stocks, list):
        cards.append(_placeholder("stock", "股票行情", "未添加股票", "设置 → 生活卡片 → 股票"))
    else:
        cards.append(_placeholder("stock", "股票行情", "行情获取失败: %s" % stocks.get("_error", "")))

    rss = _cached("rss:" + sig, RSS_TTL, lambda: _collect_feeds(cfg), fresh)
    cards.append(rss if isinstance(rss, dict) and "entries" in rss
                 else _placeholder("rss", "资讯", "订阅源获取失败"))

    exp = _cached("express:" + sig, EXPRESS_TTL, lambda: _collect_express(cfg), fresh)
    cards.append(exp if isinstance(exp, dict) and "packages" in exp
                 else _placeholder("express", "快递", "快递查询失败"))

    prc = _cached("price:" + sig, PRICE_TTL, lambda: _collect_price(cfg), fresh)
    cards.append(prc if isinstance(prc, dict) and "items" in prc
                 else _placeholder("price", "价格监控", "价格获取失败"))

    ok = any(c.get("ok") for c in cards)
    return {"ok": ok, "ts": int(time.time()), "cards": cards,
            "error": "" if ok else "全部数据源获取失败"}


def _presets():
    return {
        "stocks": [dict(x) for x in DEFAULT_STOCK_PRESETS],
        "rss": [dict(x) for x in RSS_CATALOG],
        "carriers": [{"code": c, "name": n} for c, n in CARRIERS],
        "markets": [{"code": c, "name": n} for c, n in MARKETS],
    }


# ---------------------------------------------------------------- HTTP
class LifeHandler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
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
        """与其他模块一致：复用 auth_api.check_auth（主进程内存 token / X-Auth-Token）。

        auth_api 仅在部署容器内存在；本机自测（源码树直接跑）无该模块 →
        退化为「未设置 QL_PASSWORD 时放行」，设置了密码则拒绝（不放口子）。
        """
        try:
            import auth_api
        except Exception:
            return not os.environ.get("QL_PASSWORD")
        return auth_api.check_auth(self.headers, "X-Life-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def _body(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except Exception:
            n = 0
        if n <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(n).decode("utf-8", "ignore") or "{}")
        except Exception:
            return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._check_auth():
            self._send(401, {"ok": False, "error": "未授权"})
            return
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        try:
            if parsed.path.startswith("/api/life/cards"):
                fresh = params.get("fresh", ["0"])[0] in ("1", "true", "yes")
                self._send(200, _collect(fresh=fresh))
            elif parsed.path.startswith("/api/life/config"):
                self._send(200, {"ok": True, "config": load_config(), "presets": _presets(),
                                 "path": CONFIG_PATH})
            elif parsed.path.startswith("/api/life/stock/search"):
                q = (params.get("q") or [""])[0]
                self._send(200, {"ok": True, "items": search_stocks(q)})
            else:
                self._send(404, {"ok": False, "error": "Not Found"})
        except Exception as e:
            self._send(200, {"ok": False, "error": "内部错误: %s" % e})

    def do_POST(self):
        if not self._check_auth():
            self._send(401, {"ok": False, "error": "未授权"})
            return
        parsed = urllib.parse.urlparse(self.path)
        body = self._body()
        try:
            if parsed.path.startswith("/api/life/config"):
                cfg = save_config(body.get("config") if "config" in body else body)
                self._send(200, {"ok": True, "config": cfg, "presets": _presets()})
            elif parsed.path.startswith("/api/life/article"):
                # v3.6.2：单条资讯正文（后端抓取 + 模型整理，按 URL 缓存）
                self._send(200, _fetch_article(body.get("url"), body.get("title"),
                                               fresh=bool(body.get("fresh"))))
            elif parsed.path.startswith("/api/life/price/test"):
                src = load_config()["price"]["source"]
                item = {"url": _s(body.get("url"), 800),
                        "extract": "json" if _s(body.get("extract")) == "json" else "regex",
                        "pattern": _s(body.get("pattern"), 400),
                        "path": _s(body.get("path"), 200),
                        "group": body.get("group") or 1}
                ok, err = False, ""
                try:
                    raw = _get(item["url"], timeout=src.get("timeout") or 8,
                               headers=src.get("headers") or {})
                    price, err = _extract_price(raw.decode("utf-8", "ignore"), item, src)
                    ok = price is not None
                except Exception as e:
                    price, err = None, "抓取失败: %s" % e
                self._send(200, {"ok": ok, "price": price, "error": err})
            else:
                self._send(404, {"ok": False, "error": "Not Found"})
        except Exception as e:
            self._send(200, {"ok": False, "error": "内部错误: %s" % e})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    # 本地自测：python3 life_api.py [--fresh] [--config] [端口]
    import sys
    if "--fresh" in sys.argv:
        print(json.dumps(_collect(fresh=True), ensure_ascii=False, indent=2))
        raise SystemExit(0)
    if "--config" in sys.argv:
        print(json.dumps({"config": load_config(), "presets": _presets()},
                         ensure_ascii=False, indent=2))
        raise SystemExit(0)
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9136
    print("life_api v2 配置: %s" % CONFIG_PATH)
    ThreadingHTTPServer(("127.0.0.1", port), LifeHandler).serve_forever()
