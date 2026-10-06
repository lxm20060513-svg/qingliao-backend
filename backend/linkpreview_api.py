# -*- coding: utf-8 -*-
"""链接预览抓取（待做池第 8 项）。

给聊天里出现的一条 http(s) 链接抓 og:title / og:description / og:image。

设计口径（与台账一致）：
- **轻量抓取**：单次 GET、短超时、响应体截断、只用标准库（容器内没有 requests）。
- **任何失败一律 HTTP 200 + ok:false**：App 侧据此静默不渲染（「抓取失败不显示空卡」）。
- **借 /api/agent 前缀**（前缀大法）→ nginx 两处 / lucky 白名单 / ALLOWED_RELAY 三处零改动。
- **SSRF 防护**：目标解析到环回/私网/链路本地/保留地址（含重定向跳转后的新地址）→ 直接拒，不抓内网。
- **缩略图只回传 URL，不做服务端压缩**：容器内无 PIL，加装需重建容器（风险大），
  故由 App 端按 URL 自行加载，加载不到即「无图降级」（卡片仍有标题 + 摘要）。
"""
import html as _html
import ipaddress
import json
import os
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler

PATHS = {"/api/agent/linkpreview"}

UA = ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 "
      "(KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1")
TIMEOUT = 6.0
MAX_BYTES = 400 * 1024
MAX_TITLE = 120
MAX_DESC = 220

_META_RE = re.compile(r"<meta\b[^>]*>", re.I | re.S)
_ATTR_RE = re.compile(r"""([a-zA-Z_:][-a-zA-Z0-9_:.]*)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'>]+))""")
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)


def _attr_map(tag):
    out = {}
    for m in _ATTR_RE.finditer(tag):
        key = m.group(1).strip().lower()
        val = m.group(2) or m.group(3) or m.group(4) or ""
        out[key] = _html.unescape(val.strip())
    return out


def _clean(s, limit):
    s = re.sub(r"\s+", " ", (s or "")).strip()
    if len(s) > limit:
        s = s[:limit].rstrip() + "…"
    return s


def _addr_is_blocked(ip_str):
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return True   # 解析不出当不可信 → 拒
    # ⚠️ 本机容器 DNS 对 IPv4-only 站点会额外给出一个 fc00::/7（ULA）的 NAT64 映射地址
    #   （实测 www.example.com → 28.0.0.149 + fc00::83），若把 ULA 一并当内网会把大量
    #   正常站点误伤成「不可抓取」→ 这里对 ULA 放行；真实内网目标仍会被其
    #   IPv4 私网/环回/link-local 孪生地址拦住。
    if ip.version == 6 and ip in ipaddress.ip_network("fc00::/7"):
        return False
    return (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast
            or ip.is_reserved or ip.is_unspecified)


def host_is_public(host):
    """目标主机解析到的**每一个**地址都必须是公网地址，否则拒（防 SSRF）。"""
    if not host:
        return False
    host = host.strip("[]")
    if host.lower() in ("localhost",):
        return False
    try:
        infos = socket.getaddrinfo(host, None)
    except Exception:
        return False
    if not infos:
        return False
    for info in infos:
        ip = info[4][0]
        if _addr_is_blocked(ip):
            return False
    return True


def _decode(body, content_type):
    ct = (content_type or "").lower()
    charset = None
    m = re.search(r"charset=([\w\-]+)", ct)
    if m:
        charset = m.group(1)
    if not charset:
        m = re.search(rb"""charset=["']?([\w\-]+)""", body[:4096], re.I)
        if m:
            charset = m.group(1).decode("ascii", "ignore")
    for enc in (charset, "utf-8", "gb18030", "latin-1"):
        if not enc:
            continue
        try:
            return body.decode(enc)
        except Exception:
            continue
    return body.decode("utf-8", "replace")


def extract_meta(text, base_url):
    """从 HTML 文本抽 og/meta 字段（纯函数，可单测）。"""
    title = desc = image = site = ogurl = ""
    for tag in _META_RE.finditer(text):
        a = _attr_map(tag.group(0))
        prop = (a.get("property") or a.get("name") or "").strip().lower()
        content = a.get("content") or ""
        if not content:
            continue
        if prop in ("og:title",) and not title:
            title = content
        elif prop in ("twitter:title",) and not title:
            title = content
        elif prop in ("og:description", "twitter:description") and not desc:
            desc = content
        elif prop in ("og:image", "og:image:url", "twitter:image") and not image:
            image = content
        elif prop in ("og:site_name",) and not site:
            site = content
        elif prop in ("og:url",) and not ogurl:
            ogurl = content
        elif prop == "description" and not desc:
            desc = content
    if not title:
        m = _TITLE_RE.search(text)
        if m:
            title = re.sub(r"<[^>]+>", "", m.group(1))
    title = _clean(_html.unescape(title), MAX_TITLE)
    desc = _clean(_html.unescape(desc), MAX_DESC)
    site = _clean(_html.unescape(site), 60)
    image = (image or "").strip()
    if image:
        image = urllib.parse.urljoin(base_url, _html.unescape(image).strip())
        if not image.lower().startswith(("http://", "https://")):
            image = ""
    out_url = ""
    if ogurl.strip().lower().startswith(("http://", "https://")):
        out_url = ogurl.strip()
    return {"title": title, "desc": desc, "image": image, "site": site, "ogurl": out_url}


class _SafeRedirect(urllib.request.HTTPRedirectHandler):
    """重定向每一跳都复检目标主机（防 302 到内网）。"""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        host = urllib.parse.urlsplit(newurl).hostname or ""
        if not host_is_public(host):
            raise urllib.error.URLError("redirect to non-public host blocked")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


_OPENER = urllib.request.build_opener(_SafeRedirect)


def fetch_preview(url):
    """抓取一条链接的预览元数据。返回 dict（ok 为真/false）。"""
    url = (url or "").strip()
    if not url.lower().startswith(("http://", "https://")):
        return {"ok": False, "error": "只支持 http/https 链接"}
    parts = urllib.parse.urlsplit(url)
    if not host_is_public(parts.hostname or ""):
        return {"ok": False, "error": "目标不可抓取（内网/环回/非法主机）"}
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.5",
    })
    try:
        resp = _OPENER.open(req, timeout=TIMEOUT)
    except urllib.error.HTTPError as e:
        return {"ok": False, "error": "上游 HTTP %s" % e.code}
    except Exception as e:
        return {"ok": False, "error": "%s: %s" % (type(e).__name__, str(e)[:150])}
    final_url = resp.geturl() or url
    ctype = resp.headers.get("Content-Type", "")
    if ctype and not re.search(r"(text/html|application/xhtml|text/plain)", ctype, re.I):
        try:
            resp.close()
        except Exception:
            pass
        return {"ok": False, "error": "非网页内容（%s）" % ctype.split(";")[0][:40]}
    try:
        body = resp.read(MAX_BYTES)
    except Exception as e:
        return {"ok": False, "error": "读取失败：%s" % str(e)[:120]}
    finally:
        try:
            resp.close()
        except Exception:
            pass
    meta = extract_meta(_decode(body, ctype), final_url)
    if not (meta["title"] or meta["desc"] or meta["image"]):
        return {"ok": False, "error": "页面没有可用的预览信息"}
    host = urllib.parse.urlsplit(final_url).hostname or ""
    return {
        "ok": True,
        "url": final_url,
        "title": meta["title"],
        "desc": meta["desc"],
        "image": meta["image"],
        "site": meta["site"] or host,
    }


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token")

    def _send(self, code, obj):
        payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _path(self):
        return self.path.split("?", 1)[0].rstrip("/") or "/"

    def _check_auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, "X-Link-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        self._send(200, {"ok": True, "paths": sorted(PATHS)})

    def do_POST(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        if self._path() not in PATHS:
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
            self._send(200, {"ok": False, "error": "请求体不是合法 JSON：%s" % str(e)[:120]})
            return
        try:
            self._send(200, fetch_preview(body.get("url", "")))
        except Exception as e:
            self._send(200, {"ok": False, "error": "%s: %s" % (type(e).__name__, str(e)[:150])})

    def log_message(self, fmt, *args):
        pass
