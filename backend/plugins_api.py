#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Hermes 插件 API（工作台「Hermes 插件」入口）

把 Hermes 的**已安装插件**与**官方插件库（catalog）**接到轻聊 App：
  GET  /api/plugins/list                       已安装列表（插件目录 + config.yaml 启停状态 + 桌面面板标记）
  GET  /api/plugins/detail?name=<name>         详情（manifest / README 摘要 / 技能数 / 钩子 / 依赖）
  GET  /api/plugins/catalog?q=&category=&tier=&offset=&limit=
                                               官方插件库（桌面端同一份 catalog JSON，6h 缓存）
  POST /api/plugins/install {name}             从插件库安装（后台 docker exec hermes 容器跑官方 CLI）
  GET  /api/plugins/install_status?job=<id>    安装任务状态（App 轮询）
  POST /api/plugins/toggle {name, enabled}     启用/停用（只改 config.yaml 的 plugins.enabled）

数据源（与 Hermes 桌面端 / CLI **完全同源**，不另建账本）：
  ~/.hermes/plugins/*/plugin.yaml|plugin.json    已安装插件（含手动 cp 进来的）
  ~/.hermes/desktop-plugins/*/plugin.js          纯桌面插件
  ~/.hermes/config.yaml 的 plugins.enabled       启停状态
  ~/.hermes/cache/plugin-catalog.json            官方 catalog 缓存（过期则联网刷新，失败回落旧缓存）

安全：name 白名单 ^[a-z0-9_-]{1,64}$；目录 realpath 必须落在插件根内；写 config.yaml 原子替换并复原属主；
     安装只允许 catalog 内条目（不接受任意 URL）；同一时刻只允许一个安装任务。
"""
import json
import os
import re
import subprocess
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler

import yaml

HERMES_HOME = os.environ.get("QL_HERMES_HOME", "/volume1/docker/hermes/hermes-data")
PLUGINS_ROOT = os.path.join(HERMES_HOME, "plugins")
DESKTOP_ROOT = os.path.join(HERMES_HOME, "desktop-plugins")
CONFIG_PATH = os.path.join(HERMES_HOME, "config.yaml")
CATALOG_CACHE = os.path.join(HERMES_HOME, "cache", "plugin-catalog.json")
CATALOG_URL = "https://hermes-agent.nousresearch.com/docs/api/plugin-catalog.json"
CATALOG_TTL = 6 * 3600
CATALOG_MAX_BYTES = 3 * 1024 * 1024
HERMES_CONTAINER = os.environ.get("QL_HERMES_CONTAINER", "hermes-hermes-1")
HERMES_BIN = "/opt/hermes/bin/hermes"
STATE_DIR = os.environ.get("QL_PLUGIN_STATE_DIR", "/data/streams_data")
JOB_PREFIX = "plugin_install_"
NAME_RE = re.compile(r"^[a-z0-9_-]{1,64}$")
INSTALL_TIMEOUT = 600

_LOCK = threading.Lock()
_INSTALLING = False          # 全局单飞：同一时刻只跑一个安装任务
_CATALOG_MEM = {"ts": 0.0, "data": None}


# ─────────────────────────── 通用工具 ───────────────────────────

def _safe_name(raw):
    name = str(raw or "").strip().lower()
    return name if NAME_RE.match(name) else ""


def _read_yaml(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def _read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def _manifest(dir_path):
    """读插件 manifest：plugin.yaml 优先（Hermes 以此建索引），其次 plugin.json。

    兼容三种落点：包根、`.hermes-plugin/` 子目录（git 插件布局）、`plugin.json`（Agent Plugins v1）。
    """
    cands = [
        os.path.join(dir_path, "plugin.yaml"),
        os.path.join(dir_path, "plugin.json"),
        os.path.join(dir_path, ".hermes-plugin", "plugin.yaml"),
        os.path.join(dir_path, ".hermes-plugin", "plugin.json"),
    ]
    for p in cands:
        if not os.path.isfile(p):
            continue
        if p.endswith(".json"):
            d = _read_json(p)
        else:
            d = _read_yaml(p)
        if d:
            return d, p
    return {}, ""


def _author_text(v):
    if isinstance(v, dict):
        return str(v.get("name") or v.get("url") or "")
    return str(v or "")


def _skills_count(dir_path):
    n = 0
    root = os.path.join(dir_path, "skills")
    try:
        for entry in os.listdir(root):
            if os.path.isfile(os.path.join(root, entry, "SKILL.md")):
                n += 1
    except OSError:
        pass
    return n


def _config_plugins_state():
    """读 config.yaml 的 plugins 段 → (enabled_set, disabled_set, raw_plugins_dict)。"""
    cfg = _read_yaml(CONFIG_PATH)
    p = cfg.get("plugins") if isinstance(cfg.get("plugins"), dict) else {}
    en = {str(x).strip() for x in (p.get("enabled") or []) if str(x).strip()}
    dis = {str(x).strip() for x in (p.get("disabled") or []) if str(x).strip()}
    return en, dis, p


def _enabled_keys_for(dirname, manifest_name):
    """插件可能的 config key：目录名 / manifest 名 / git 插件的 `<dir>/.hermes-plugin`。"""
    keys = {dirname}
    if manifest_name:
        keys.add(manifest_name)
        if manifest_name != dirname:
            keys.add("%s/.hermes-plugin" % dirname)
    keys.add("%s/.hermes-plugin" % dirname)
    return keys


_INSTALL_META_CACHE = {"mtime": 0.0, "data": {}}


def _install_meta(dirname):
    """安装记录在插件根目录的 `.install-metadata.json`（按目录名索引），不是每个插件目录里。

    有 catalog 块 = 从插件库安装；只有 source = git/URL 安装；都没有 = 手动拷贝进来的。
    """
    path = os.path.join(PLUGINS_ROOT, ".install-metadata.json")
    try:
        mt = os.path.getmtime(path)
    except OSError:
        return {}
    if _INSTALL_META_CACHE["mtime"] != mt:
        data = _read_json(path)
        _INSTALL_META_CACHE["data"] = data if isinstance(data, dict) else {}
        _INSTALL_META_CACHE["mtime"] = mt
    rec = _INSTALL_META_CACHE["data"].get(dirname)
    return rec if isinstance(rec, dict) else {}


def _plugin_row(dirname, en, dis):
    dir_path = os.path.join(PLUGINS_ROOT, dirname)
    if not os.path.isdir(dir_path):
        return None
    man, man_path = _manifest(dir_path)
    if not man:
        # 没有 manifest 的目录不是 Hermes 插件（如 model-providers/ 这类配置目录）——不列
        return None
    name = str(man.get("name") or dirname).strip() or dirname
    keys = _enabled_keys_for(dirname, name)
    # 以 config.yaml 为准：命中 enabled 即启用；只命中 disabled 即停用；都没写按 Hermes 默认（用户插件默认关闭）
    hit_en = [k for k in keys if k in en]
    hit_dis = [k for k in keys if k in dis]
    enabled = bool(hit_en) and not hit_dis
    meta = _install_meta(dirname)
    cat = meta.get("catalog") if isinstance(meta.get("catalog"), dict) else {}
    if cat:
        source = "catalog"
    elif meta.get("source"):
        source = "git" if "git" in str(meta.get("source")).lower() else "manual"
    else:
        source = "manual"
    hooks = man.get("hooks") if isinstance(man.get("hooks"), list) else []
    deps = man.get("external_dependencies") if isinstance(man.get("external_dependencies"), list) else []
    desktop_half = os.path.isfile(os.path.join(dir_path, "desktop", "plugin.js"))
    return {
        "name": name,
        "dir": dirname,
        "version": str(man.get("version") or meta.get("version") or ""),
        "description": str(man.get("description") or ""),
        "author": _author_text(man.get("author")),
        "category": str(man.get("category") or ""),
        "enabled": enabled,
        "enabled_keys": hit_en,
        "source": source,
        "source_repo": str(cat.get("repo") or meta.get("source") or ""),
        "catalog_tier": str(cat.get("tier") or ""),
        "desktop": desktop_half,
        "desktop_only": False,
        "manageable": True,
        "skills": _skills_count(dir_path),
        "hooks": [str(h) for h in hooks][:12],
        "deps": [str(d.get("name") or d) if isinstance(d, dict) else str(d) for d in deps][:8],
        "has_readme": os.path.isfile(os.path.join(dir_path, "README.md")),
        "path": dir_path,
        "mtime": int(os.path.getmtime(dir_path)),
    }


def _desktop_only_rows(known_dirs):
    rows = []
    try:
        entries = sorted(os.listdir(DESKTOP_ROOT))
    except OSError:
        return rows
    for d in entries:
        p = os.path.join(DESKTOP_ROOT, d)
        if d.startswith(".") or d in known_dirs or not os.path.isdir(p):
            continue
        if not os.path.isfile(os.path.join(p, "plugin.js")):
            continue
        rows.append({
            "name": d, "dir": d, "version": "", "description": "桌面端插件（仅 Hermes 桌面 App 加载）",
            "author": "", "category": "desktop", "enabled": True, "enabled_keys": [],
            "source": "desktop", "source_repo": "", "catalog_tier": "",
            "desktop": True, "desktop_only": True, "manageable": False,
            "skills": 0, "hooks": [], "deps": [], "has_readme": False,
            "path": p, "mtime": int(os.path.getmtime(p)),
        })
    return rows


def _list_plugins():
    en, dis, _p = _config_plugins_state()
    rows = []
    known = set()
    try:
        entries = sorted(os.listdir(PLUGINS_ROOT))
    except OSError:
        entries = []
    for d in entries:
        if d.startswith(".") or d == "__pycache__":
            continue
        p = os.path.join(PLUGINS_ROOT, d)
        if not os.path.isdir(p):
            continue
        row = _plugin_row(d, en, dis)
        if row:
            rows.append(row)
            known.add(d)
    rows.extend(_desktop_only_rows(known))
    rows.sort(key=lambda r: (not r["enabled"], r["name"].lower()))
    return rows


def _find_plugin(name):
    name = _safe_name(name)
    if not name:
        return None
    for row in _list_plugins():
        if row["name"] == name or row["dir"] == name:
            return row
    return None


def _read_readme(dir_path, limit=4000):
    for fn in ("README.md", "after-install.md"):
        p = os.path.join(dir_path, fn)
        try:
            with open(p, "r", encoding="utf-8", errors="replace") as f:
                return f.read(limit)
        except OSError:
            continue
    return ""


# ─────────────────────────── config.yaml 写入 ───────────────────────────

def _atomic_write(path, text):
    st = os.stat(path)
    tmp = "%s.tmp-qlplugin-%d" % (path, int(time.time() * 1000))
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.chmod(tmp, st.st_mode & 0o777)
    os.replace(tmp, path)
    try:
        os.chown(path, st.st_uid, st.st_gid)   # mode 与 owner 是两件事，原子替换只沿用 mode
    except OSError:
        pass


def _set_plugin_enabled(name, enabled, drop=False):
    """只改 config.yaml 顶层 plugins 段的 enabled 列表，其余字节不动。

    返回 (ok, error)。写后 yaml 回读校验，失败回滚。
    """
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        text = f.read()
    orig = text
    en, dis, _p = _config_plugins_state()
    key = name
    # 已有 key 归一化：manifest 名可能和 config 里的 key 不同（superpowers/.hermes-plugin）
    for k in list(en) + list(dis):
        if k == name or k.endswith("/" + name) or k.split("/")[0] == name:
            key = k
            break
    new_en = set(en)
    new_dis = set(dis)
    if drop:
        # v4.0.126：卸载时把键从 enabled/disabled **两个列表都摘掉**——
        # 只写 disabled 会留下脏条目，同名插件下次安装会被当成「停用」。
        new_en.discard(key)
        new_dis.discard(key)
    elif enabled:
        new_en.add(key)
        new_dis.discard(key)
    else:
        new_en.discard(key)
        new_dis.add(key)

    def render(items, indent="    "):
        if not items:
            return "%s[]" % indent
        return "\n".join("%s- %s" % (indent, it) for it in sorted(items))

    lines = text.splitlines(True)
    start = None
    for i, ln in enumerate(lines):
        if re.match(r"^plugins:\s*$", ln.rstrip("\n")):
            start = i
            break
    if start is None:
        return False, "config.yaml 没有顶层 plugins: 段"
    end = len(lines)
    for j in range(start + 1, len(lines)):
        s = lines[j]
        if s.strip() and not s.startswith((" ", "\t")) and not s.lstrip().startswith("#"):
            end = j
            break

    def block_for(keyname):
        """返回 (start_idx, end_idx, indent)：块范围是 [start, end)，end 指向块后第一行。"""
        for k in range(start + 1, end):
            m = re.match(r"^(\s+)%s:" % re.escape(keyname), lines[k])
            if not m:
                continue
            ind = m.group(1)
            b = k + 1
            while b < end and (lines[b].startswith(ind + " ") or not lines[b].strip()):
                b += 1
            return k, b, ind
        return None, None, None

    for keyname, items in (("enabled", new_en), ("disabled", new_dis)):
        s, e, ind = block_for(keyname)
        ind = ind or "  "
        if items:
            newline = "%s%s:\n%s\n" % (ind, keyname, render(items, ind + "  "))
        else:
            newline = "%s%s: []\n" % (ind, keyname)
        if s is None:
            lines.insert(start + 1, newline)
            end += 1
        else:
            lines[s:e] = [newline]
            end += 1 - (e - s)
    out = "".join(lines)
    try:
        chk = yaml.safe_load(out) or {}
        pchk = chk.get("plugins") or {}
        got = {str(x) for x in (pchk.get("enabled") or [])}
        want = {str(x) for x in new_en}
        if got != want:
            return False, "写后校验失败（enabled 集合不一致）"
        _atomic_write(CONFIG_PATH, out)
    except Exception as e:  # noqa: BLE001
        try:
            _atomic_write(CONFIG_PATH, orig)
        except Exception:  # noqa: BLE001
            pass
        return False, "写入失败：%s" % e
    return True, ""


def _restart_hermes_async():
    def _worker():
        try:
            subprocess.run(["docker", "restart", HERMES_CONTAINER],
                           capture_output=True, text=True, timeout=180)
        except Exception:  # noqa: BLE001
            pass
    threading.Thread(target=_worker, daemon=True, name="plugin-hermes-restart").start()


# ─────────────────────────── 插件库 catalog ───────────────────────────

def _load_catalog(force=False):
    """优先内存 → 磁盘缓存（6h TTL）→ 联网刷新；任何失败都回落旧缓存（不限龄期）。"""
    now = time.time()
    if not force and _CATALOG_MEM["data"] and now - _CATALOG_MEM["ts"] < CATALOG_TTL:
        return _CATALOG_MEM["data"], "memory"
    cached = None
    try:
        if os.path.isfile(CATALOG_CACHE):
            age = now - os.path.getmtime(CATALOG_CACHE)
            with open(CATALOG_CACHE, "r", encoding="utf-8-sig") as f:
                cached = json.load(f)
            if not force and age < CATALOG_TTL and isinstance(cached, dict) and cached.get("entries"):
                _CATALOG_MEM["data"], _CATALOG_MEM["ts"] = cached, now
                return cached, "cache"
    except Exception:  # noqa: BLE001
        cached = None
    try:
        req = urllib.request.Request(CATALOG_URL, headers={"User-Agent": "Qingliao/plugin-catalog"})
        with urllib.request.urlopen(req, timeout=8) as r:
            raw = r.read(CATALOG_MAX_BYTES)
        data = json.loads(raw.decode("utf-8-sig"))
        if isinstance(data, dict) and isinstance(data.get("entries"), list):
            try:
                os.makedirs(os.path.dirname(CATALOG_CACHE), exist_ok=True)
                tmp = CATALOG_CACHE + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(data, f)
                os.replace(tmp, CATALOG_CACHE)
            except OSError:
                pass
            _CATALOG_MEM["data"], _CATALOG_MEM["ts"] = data, now
            return data, "live"
    except Exception:  # noqa: BLE001
        pass
    if isinstance(cached, dict) and cached.get("entries"):
        _CATALOG_MEM["data"], _CATALOG_MEM["ts"] = cached, now - CATALOG_TTL + 300
        return cached, "stale"
    return {"entries": [], "removed": []}, "none"


def _catalog_view(data, q="", category="", tier="", offset=0, limit=30):
    installed = {}
    for row in _list_plugins():
        installed[row["name"]] = row
        installed[row["dir"]] = row
    q = str(q or "").strip().lower()
    category = str(category or "").strip().lower()
    tier = str(tier or "").strip().lower()
    items = []
    for e in data.get("entries") or []:
        if not isinstance(e, dict):
            continue
        name = str(e.get("name") or "")
        if not name:
            continue
        if q and q not in name.lower() and q not in str(e.get("description") or "").lower() \
                and q not in str(e.get("maintainer") or "").lower():
            continue
        cat = str(e.get("category") or "")
        if category and cat != category:
            continue
        if tier and str(e.get("tier") or "") != tier:
            continue
        row = installed.get(name)
        caps = e.get("capabilities") if isinstance(e.get("capabilities"), dict) else {}
        items.append({
            "name": name,
            "description": str(e.get("description") or ""),
            "maintainer": str(e.get("maintainer") or ""),
            "tier": str(e.get("tier") or "community"),
            "category": cat,
            "version": str(e.get("version") or ""),
            "repo": str(e.get("repo") or ""),
            "sha": str(e.get("sha") or ""),
            "docs_url": str(e.get("docs_url") or ""),
            "tools": [str(t) for t in (caps.get("provides_tools") or [])][:8],
            "requires_env": [str(t) for t in (caps.get("requires_env") or [])][:8],
            "installed": bool(row),
            "installed_version": (row or {}).get("version", ""),
            "installed_enabled": bool((row or {}).get("enabled")),
        })
    items.sort(key=lambda i: (not i["installed"], i["tier"] != "official", i["name"]))
    total = len(items)
    offset = max(0, int(offset or 0))
    limit = max(1, min(100, int(limit or 30)))
    cats = sorted({str(e.get("category") or "") for e in (data.get("entries") or []) if isinstance(e, dict)} - {""})
    return {"total": total, "items": items[offset:offset + limit], "categories": cats}


# ─────────────────────────── 安装任务 ───────────────────────────

def _job_path(job):
    return os.path.join(STATE_DIR, JOB_PREFIX + job + ".json")


def _job_write(job, **kw):
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        with open(_job_path(job), "w", encoding="utf-8") as f:
            json.dump(kw, f, ensure_ascii=False)
    except OSError:
        pass


def _job_read(job):
    return _read_json(_job_path(job)) if _safe_job(job) else {}


def _safe_job(job):
    return bool(re.match(r"^[a-f0-9]{8,32}$", str(job or "")))


def _do_install(name, job):
    global _INSTALLING
    log = []

    def run(cmd, timeout=INSTALL_TIMEOUT):
        log.append("$ " + " ".join(cmd))
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            out = ((r.stdout or "") + (r.stderr or "")).strip()
            if out:
                log.append(out[-3000:])
            return r.returncode
        except Exception as e:  # noqa: BLE001
            log.append("执行失败：%s" % e)
            return -1

    try:
        _job_write(job, state="running", name=name, startedAt=int(time.time()),
                   log="正在下载并安装（约 30~120 秒）…")
        rc = run(["docker", "exec", "-u", "10000", HERMES_CONTAINER, HERMES_BIN,
                  "plugins", "install", name, "--enable"])
        row = None
        for r in _list_plugins():
            if r["name"] == name or r["dir"] == name:
                row = r
                break
        if rc == 0 and row:
            _job_write(job, state="done", name=name, startedAt=0, finishedAt=int(time.time()),
                       log="\n".join(log)[-6000:], plugin=row,
                       hint="已安装并启用；约 30 秒后（网关重启完成）可在聊天中使用")
            # 新插件要重启网关才会被加载
            _restart_hermes_async()
        else:
            _job_write(job, state="error", name=name, finishedAt=int(time.time()),
                       log="\n".join(log)[-6000:],
                       error="安装未成功（退出码 %s）" % rc)
    except Exception as e:  # noqa: BLE001
        _job_write(job, state="error", name=name, finishedAt=int(time.time()), error=str(e)[:300],
                   log="\n".join(log)[-6000:])
    finally:
        with _LOCK:
            _INSTALLING = False


# ─────────────────────────── HTTP ───────────────────────────

class Handler(BaseHTTPRequestHandler):
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
        import auth_api
        return auth_api.check_auth(self.headers, "X-Plugins-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def _normalize(self):
        # /api/agent/plugins 是给 lucky / 蜂窝 relay 走的别名
        if self.path.startswith("/api/agent/plugins"):
            self.path = "/api/plugins" + self.path[len("/api/agent/plugins"):]

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        self._normalize()
        parsed = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(parsed.query)
        if parsed.path.startswith("/api/plugins/list"):
            rows = _list_plugins()
            en, dis, _p = _config_plugins_state()
            self._send(200, {"ok": True, "plugins": rows,
                             "counts": {"total": len(rows),
                                        "enabled": sum(1 for r in rows if r["enabled"]),
                                        "disabled": sum(1 for r in rows if not r["enabled"]),
                                        "manual": sum(1 for r in rows if r["source"] == "manual")},
                             "enabled_keys": sorted(en)})
            return
        if parsed.path.startswith("/api/plugins/detail"):
            row = _find_plugin(q.get("name", [""])[0])
            if not row:
                self._send(404, {"ok": False, "error": "插件不存在"})
                return
            man, man_path = _manifest(row["path"])
            row = dict(row)
            row["readme"] = _read_readme(row["path"])
            row["manifest_path"] = man_path
            row["manifest"] = {k: man.get(k) for k in ("name", "version", "description", "author",
                                                       "hooks", "provides_tools", "external_dependencies")
                               if k in man}
            self._send(200, {"ok": True, "plugin": row})
            return
        if parsed.path.startswith("/api/plugins/catalog"):
            data, src = _load_catalog(force=(q.get("fresh", ["0"])[0] == "1"))
            view = _catalog_view(data,
                                 q=q.get("q", [""])[0],
                                 category=q.get("category", [""])[0],
                                 tier=q.get("tier", [""])[0],
                                 offset=q.get("offset", ["0"])[0],
                                 limit=q.get("limit", ["30"])[0])
            view.update({"ok": True, "source": src})
            self._send(200, view)
            return
        if parsed.path.startswith("/api/plugins/install_status"):
            job = q.get("job", [""])[0]
            st = _job_read(job)
            if not st:
                self._send(404, {"ok": False, "error": "任务不存在"})
                return
            st = dict(st)
            st["ok"] = True
            self._send(200, st)
            return
        if parsed.path.startswith("/api/plugins/restart_status"):
            self._send(200, {"ok": True, "installing": _INSTALLING})
            return
        self._send(404, {"ok": False, "error": "Not Found"})

    def do_POST(self):
        if not self._check_auth():
            self._send(401, {"ok": False, "error": "未授权"})
            return
        self._normalize()
        parsed = urllib.parse.urlparse(self.path)
        try:
            n = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(n) or b"{}") if n else {}
        except Exception:  # noqa: BLE001
            body = {}
        if not isinstance(body, dict):
            body = {}
        if parsed.path.startswith("/api/plugins/install"):
            self._do_install(body)
            return
        if parsed.path.startswith("/api/plugins/toggle"):
            self._do_toggle(body)
            return
        if parsed.path.startswith("/api/plugins/uninstall"):
            self._do_uninstall(body)
            return
        self._send(404, {"ok": False, "error": "Not Found"})

    def _do_install(self, body):
        global _INSTALLING
        name = _safe_name(body.get("name"))
        if not name:
            self._send(400, {"ok": False, "error": "name 非法"})
            return
        data, _src = _load_catalog()
        entry = next((e for e in (data.get("entries") or [])
                      if isinstance(e, dict) and str(e.get("name") or "") == name), None)
        if entry is None:
            self._send(400, {"ok": False, "error": "插件库中没有该插件（只允许安装 catalog 条目）"})
            return
        if _find_plugin(name):
            self._send(409, {"ok": False, "error": "该插件已安装"})
            return
        with _LOCK:
            if _INSTALLING:
                self._send(409, {"ok": False, "error": "已有安装任务在跑，请稍后再试"})
                return
            _INSTALLING = True
            job = "%08x%08x" % (int(time.time()), abs(hash(name)) & 0xFFFFFFFF)
            threading.Thread(target=_do_install, args=(name, job), daemon=True,
                             name="plugin-install-%s" % name).start()
        self._send(200, {"ok": True, "job": job, "name": name,
                         "msg": "已开始安装，约 30~120 秒"})

    def _do_uninstall(self, body):
        """v4.0.126：卸载插件 = 摘 config 键 + `hermes plugins remove` + 重启网关。

        · 只允许卸载插件根下的目录（NAME_RE 白名单，防 ../ 逃逸）
        · 桌面端插件（desktop-plugins/）不归轻聊管，直接拒
        · 删除后不自动备份：插件是 catalog 可重装的，用户点一下就回来
        """
        name = str(body.get("name") or "").strip()
        row = _find_plugin(name)
        if not row:
            self._send(404, {"ok": False, "error": "插件不存在"})
            return
        if not row.get("manageable", True):
            self._send(400, {"ok": False, "error": "桌面端插件由桌面 App 管理，轻聊不能卸载"})
            return
        dirname = str(row.get("dir") or row.get("name") or "")
        if not NAME_RE.match(dirname):
            self._send(400, {"ok": False, "error": "插件目录名非法"})
            return
        try:
            r = subprocess.run(["docker", "exec", "-u", "10000", HERMES_CONTAINER,
                                HERMES_BIN, "plugins", "remove", dirname],
                               capture_output=True, text=True, timeout=180)
            out = ((r.stdout or "") + (r.stderr or "")).strip()
            rc = r.returncode
        except Exception as e:  # noqa: BLE001
            self._send(500, {"ok": False, "error": "执行卸载失败：%s" % str(e)[:200]})
            return
        if rc != 0:
            self._send(500, {"ok": False, "error": "卸载未成功（退出码 %s）" % rc,
                             "log": out[-1200:]})
            return
        # 目录已删，再清 config 键（顺序反了会留下指向不存在目录的键）
        _set_plugin_enabled(str(row.get("name") or dirname), False, drop=True)
        _restart_hermes_async()
        self._send(200, {"ok": True, "name": row.get("name") or dirname, "removed": dirname,
                         "restart": True, "msg": "已卸载，约 30 秒后网关重启生效",
                         "log": out[-600:]})

    def _do_toggle(self, body):
        name = str(body.get("name") or "").strip()
        enabled = bool(body.get("enabled"))
        row = _find_plugin(name)
        if not row:
            self._send(404, {"ok": False, "error": "插件不存在"})
            return
        if not row.get("manageable", True):
            self._send(400, {"ok": False, "error": "桌面端插件的开关由桌面 App 管理，轻聊不能改"})
            return
        ok, err = _set_plugin_enabled(row["name"], enabled)
        if not ok:
            self._send(500, {"ok": False, "error": err})
            return
        _restart_hermes_async()
        self._send(200, {"ok": True, "name": row["name"], "enabled": enabled,
                         "restart": True, "msg": "已%s，约 30 秒后网关重启生效" % ("启用" if enabled else "停用")})
