#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""网盘接入 API（设置 → 网盘接入 / 文件管理 → 网盘文件浏览）

产品约定（用户明确）：
  · 入口在「设置」里独立的「网盘接入」项，**不塞进连接器卡片**。
  · 接入方式 = 粘贴网盘官方 skill 链接 + 授权码，后端完成「装 skill + 授权 + 登记」。
  · 文件管理里新增「网盘文件浏览」，从这里查看已接入网盘的文件。

端点（unified_router 9127 挂载 /api/clouddrive 前缀）：
  GET  /api/clouddrive/drives              → {"ok":true,"drives":[{id,name,nickname,status,error}]}
  POST /api/clouddrive/add                 {skill_url, auth_code, name?}
  POST /api/clouddrive/remove               {id}
  GET  /api/clouddrive/list?drive=<id>&fid=<fid>   （fid 空 = 根目录 0）
  GET  /api/clouddrive/download?drive=<id>&fid=<fid>&name=<文件名>

为什么 CLI 要 docker exec：官方 skill 的 CLI 是 node 程序，而 node 只装在 hermes 容器里
（qingliao 容器只有 python3）。skills 目录是同一宿主目录
（/data/hermes/skills → hermes 容器内 /data/hermes/skills），
所以这里统一「qingliao 容器内 docker exec hermes-container node ...」。
探活顺序固定：install.sh → login --token → get-user-info → 一个最轻的读操作（browse 根目录），
三段都拿到 code:0 才算接入成功（只到 install.sh 不算数）。
"""
import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import zipfile
from http.server import BaseHTTPRequestHandler

# ── 环境 ──
HERMES_CONTAINER = os.environ.get("QL_HERMES_CONTAINER", os.environ.get("QL_HERMES_CONTAINER", "hermes-container"))
# 宿主 skills 目录（qingliao 容器挂载 /data；hermes 容器内对应 /data/hermes/skills）
HOST_SKILLS_DIR = os.environ.get("QL_HOST_SKILLS_DIR", os.environ.get("QL_HERMES_DATA", "/data/hermes") + "/skills")
HERMES_SKILLS_DIR = os.environ.get("QL_HERMES_DATA", "/data/hermes") + "/skills"
DRIVE_REGISTRY = os.environ.get("QL_DRIVE_REGISTRY", "/data/cloud_drives.json")
DL_DIR = os.environ.get("QL_CLOUDDRIVE_DL", "/data/clouddrive_dl")

# CLI 需要的 host-agent 标记（官方 CLI 靠 env 探针识别宿主，不设就报「无法识别 Agent 环境」）
AGENT_ENV = {"HERMES_INTERACTIVE": "1"}

CLI_TIMEOUT = 120          # 单次 CLI 调用超时（秒）
INSTALL_TIMEOUT = 300       # install.sh 超时
MAX_ZIP_BYTES = 60 * 1024 * 1024
MAX_TOTAL = 20              # 台账里最多登记的网盘数

_LOCK = threading.Lock()

# ── 台账（多网盘）──
# [{"id":"quarkclouddrive","name":"夸克网盘","nickname":"夸父9469","status":"ready|error",
#   "error":"…","added_at":1700000000}]
_DRIVES_CACHE = None


def _load_drives():
    global _DRIVES_CACHE
    if _DRIVES_CACHE is not None:
        return _DRIVES_CACHE
    try:
        with open(DRIVE_REGISTRY, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            _DRIVES_CACHE = data
        else:
            _DRIVES_CACHE = []
    except Exception:  # noqa: BLE001
        _DRIVES_CACHE = []
    return _DRIVES_CACHE


def _save_drives(items):
    global _DRIVES_CACHE
    os.makedirs(os.path.dirname(DRIVE_REGISTRY) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(DRIVE_REGISTRY) or ".", suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, DRIVE_REGISTRY)
    _DRIVES_CACHE = items


# ── CLI 调用 ──

def _skill_path(host_dir, skill_id):
    return os.path.join(HOST_SKILLS_DIR, skill_id)


def run_cli(skill_id, args, timeout=CLI_TIMEOUT):
    """在 hermes 容器内跑官方 CLI；返回 (code, stdout_text)。"""
    entry = os.environ.get("QL_HERMES_DATA", "/data/hermes") + "/skills/%s/scripts/quark-drive.cjs" % skill_id
    cmd = ["docker", "exec"]
    for k, v in AGENT_ENV.items():
        cmd += ["-e", "%s=%s" % (k, v)]
    cmd += [HERMES_CONTAINER, "node", entry] + list(args)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return 124, ""
    except Exception as exc:  # noqa: BLE001
        return 125, str(exc)
    return r.returncode, (r.stdout or "") + (("\n" + r.stderr) if r.stderr else "")


def parse_cli_lines(text):
    """官方 CLI 是 NDJSON（一行一个 {code,msg,data}）。取最后一条 result 行 + 汇总所有 list 行。"""
    items, result = [], None
    for line in (text or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except Exception:  # noqa: BLE001
            continue
        if not isinstance(obj, dict):
            continue
        if obj.get("type") == "list" and isinstance(obj.get("data"), dict):
            items.append(obj["data"])
        elif obj.get("type") == "result":
            result = obj
        elif result is None and "code" in obj:
            result = obj
    return items, result


def cli_ok(result):
    return bool(result) and result.get("code") == 0


def cli_err(result, default="操作失败"):
    if not result:
        return default
    msg = str(result.get("msg") or "")
    return ("%s(code %s)" % (msg, result.get("code"))) if msg else default


# ── 安装 ──

def download_skill_zip(skill_url, workdir):
    """下载官方 skill zip（只允许 https，限大小，返回文件路径）。"""
    if not re.match(r"^https://", skill_url):
        raise ValueError("技能地址必须是 https 链接")
    req = urllib.request.Request(skill_url, headers={"User-Agent": "Qingliao/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp, \
            open(os.path.join(workdir, "skill.zip"), "wb") as out:
        total = 0
        while True:
            chunk = resp.read(256 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_ZIP_BYTES:
                raise ValueError("技能包过大（>60MB）")
            out.write(chunk)
    if total == 0:
        raise ValueError("技能包下载为空")
    return os.path.join(workdir, "skill.zip")


def safe_extract_zip(zip_path, dest):
    """解压并防 Zip Slip（成员路径必须落在 dest 内）。"""
    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            target = os.path.normpath(os.path.join(dest, info.filename))
            if os.path.commonpath([target, os.path.abspath(dest)]) != os.path.abspath(dest):
                raise ValueError("技能包内含非法路径：%s" % info.filename)
        z.extractall(dest)


def detect_skill_id(dest):
    """从解压结果里找含 SKILL.md 的一级目录（官方包形如 quarkclouddrive-1.0.20/）。"""
    if os.path.isfile(os.path.join(dest, "SKILL.md")):
        return ""  # 平铺包:SKILL.md 在 zip 根(官方 1.0.20+ 即此形态)
    entries = [e for e in os.listdir(dest) if os.path.isdir(os.path.join(dest, e))]
    for e in entries:
        if os.path.isfile(os.path.join(dest, e, "SKILL.md")):
            return e
    raise ValueError("技能包里没找到 SKILL.md，可能不是官方 skill 包")


def install_skill(skill_url):
    """下载+解压+跑 install.sh，返回 (skill_id, log)。"""
    workdir = tempfile.mkdtemp(prefix="qlskill_", dir="/tmp")
    try:
        zip_path = download_skill_zip(skill_url, workdir)
        safe_extract_zip(zip_path, workdir)  # 下载后必须解压,否则 detect 找不到 SKILL.md
        raw = detect_skill_id(workdir)
        if not raw:
            # zip 根目录就是 skill 本体：直接当 staging
            staging = workdir
            skill_id = _guess_skill_id_from_url(skill_url)
        else:
            staging = os.path.join(workdir, raw)
            skill_id = _guess_skill_id_from_url(skill_url) or raw
        skill_id = re.sub(r"[^A-Za-z0-9_.-]", "", skill_id)[:40]
        if not skill_id:
            raise ValueError("无法确定 skill 名称")
        dest = _skill_path(HOST_SKILLS_DIR, skill_id)
        os.makedirs(HOST_SKILLS_DIR, exist_ok=True)
        if os.path.isdir(dest):
            shutil.rmtree(dest)
        shutil.copytree(staging, dest)
        log = ""
        installer = os.path.join(dest, "scripts", "install.sh")
        if not os.path.isfile(installer):  # 平铺包:install.sh 在根级(官方 1.0.20+)
            installer = os.path.join(dest, "install.sh")
        if os.path.isfile(installer):
            # install.sh 需要 node,只在 hermes 容器里有;dest 已挂载为她的 /data/hermes/skills/<id>
            hermes_dest = os.path.join(os.environ.get("QL_HERMES_DATA", "/data/hermes") + "/skills", skill_id)
            r = subprocess.run(["docker", "exec", HERMES_CONTAINER, "bash", hermes_dest + installer[len(dest):]],
                               capture_output=True, text=True,
                               timeout=INSTALL_TIMEOUT, cwd=dest)
            log = ((r.stdout or "") + (r.stderr or ""))[-2000:]
            if r.returncode != 0:
                shutil.rmtree(dest, ignore_errors=True)
                raise ValueError("安装脚本失败：%s" % log[-300:])
        return skill_id, log
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def _guess_skill_id_from_url(skill_url):
    base = skill_url.rstrip("/").rsplit("/", 1)[-1]
    name = re.sub(r"\.(zip|tgz|tar\.gz)$", "", base, flags=re.I)
    name = re.sub(r"-\d[\w.]*$", "", name)      # 去掉 -1.0.20 版本后缀
    return re.sub(r"[^A-Za-z0-9_.-]", "", name)[:40]


def bind_account(skill_id, auth_code):
    """授权绑定：login --token → get-user-info → browse 根目录。"""
    rc, out = run_cli(skill_id, ["login", "--token", auth_code])
    items, result = parse_cli_lines(out)
    if result is None:
        raise ValueError("授权命令无响应（退出码 %s）" % rc)
    code = result.get("code")
    if code == 0 or code == -118:      # -118 = 已有有效授权
        pass
    else:
        raise ValueError("授权失败：%s" % cli_err(result))
    rc, out = run_cli(skill_id, ["get-user-info"])
    _, result = parse_cli_lines(out)
    if not cli_ok(result):
        raise ValueError("账号信息读取失败：%s" % cli_err(result, "可能未授权"))
    nickname = ""
    try:
        nickname = str(result["data"]["userInfo"]["nickname"])
    except Exception:  # noqa: BLE001
        nickname = ""
    rc, out = run_cli(skill_id, ["browse", "--parent-fid", "0", "--page-size", "1"])
    items, result = parse_cli_lines(out)
    if not cli_ok(result):
        raise ValueError("网盘文件列表读取失败：%s" % cli_err(result, "接入未生效"))
    return nickname


def probe_drive(skill_id):
    """复查已接入网盘是否仍可用（列表页进入时顺手更新状态）。"""
    rc, out = run_cli(skill_id, ["get-user-info"])
    _, result = parse_cli_lines(out)
    if not cli_ok(result):
        return None, cli_err(result, "授权已失效")
    try:
        return str(result["data"]["userInfo"]["nickname"]), ""
    except Exception:  # noqa: BLE001
        return "", ""


def remove_drive(skill_id):
    """解绑（服务端撤销授权）+ 删本地 skill 目录。"""
    rc, out = run_cli(skill_id, ["unauthorize"], timeout=60)
    _, result = parse_cli_lines(out)
    msg = "" if cli_ok(result) else cli_err(result, "解绑未确认")
    dest = _skill_path(HOST_SKILLS_DIR, skill_id)
    shutil.rmtree(dest, ignore_errors=True)
    return msg


# ── 展示名 ──
DISPLAY_NAMES = {"quarkclouddrive": "夸克网盘"}


def display_name(skill_id):
    return DISPLAY_NAMES.get(skill_id, skill_id)


class Handler(BaseHTTPRequestHandler):
    """unified_router 委托 handler（照 mcp_api 模式）"""

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
        return auth_api.check_auth(self.headers, "X-Cloud-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def log_message(self, fmt, *args):
        return

    # ── GET ──
    def do_GET(self):
        if not self._check_auth():
            self._send(401, {"ok": False, "error": "未授权"})
            return
        # v4.0.x: unified_router 借 /api/agent 前缀做 lucky(16666) 别名路由,
        # 但 handler 分支只认 /api/clouddrive/* → 别名请求进来全落 404。
        # 在解析前把别名前缀归一,原路径不受影响。
        if self.path.startswith("/api/agent/clouddrive"):
            self.path = self.path.replace("/api/agent/clouddrive", "/api/clouddrive", 1)
        parsed = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(parsed.query)
        if parsed.path.startswith("/api/clouddrive/drives"):
            self._send(200, {"ok": True, "drives": self._drives_payload()})
            return
        if parsed.path.startswith("/api/clouddrive/list"):
            self._do_list(q.get("drive", [""])[0], q.get("fid", [""])[0])
            return
        if parsed.path.startswith("/api/clouddrive/download"):
            self._do_download(q.get("drive", [""])[0], q.get("fid", [""])[0],
                              q.get("name", [""])[0])
            return
        self._send(404, {"ok": False, "error": "Not Found"})

    def _drives_payload(self):
        with _LOCK:
            items = list(_load_drives())
        return [{"id": d.get("id", ""), "name": d.get("name") or display_name(d.get("id", "")),
                 "nickname": d.get("nickname", ""), "status": d.get("status", "ready"),
                 "error": d.get("error", ""), "added_at": d.get("added_at", 0)}
                for d in items]

    def _find_drive(self, drive_id):
        with _LOCK:
            for d in _load_drives():
                if d.get("id") == drive_id:
                    return d
        return None

    def _do_list(self, drive_id, fid):
        drive = self._find_drive(drive_id)
        if not drive:
            self._send(404, {"ok": False, "error": "网盘未接入"})
            return
        parent = fid or "0"
        rc, out = run_cli(drive_id, ["browse", "--parent-fid", parent, "--page-size", "100"])
        items, result = parse_cli_lines(out)
        if not cli_ok(result):
            self._send(200, {"ok": False, "error": cli_err(result, "读取网盘文件失败")})
            return
        entries = []
        for it in items:
            if not isinstance(it, dict) or "filename" not in it:
                continue
            entries.append({
                "fid": str(it.get("fid", "")),
                "name": str(it.get("filename", "")),
                "is_dir": int(it.get("file_type", 0) or 0) == 0,
                "size": int(it.get("size", 0) or 0),
                "mtime": int(it.get("updated_at", 0) or 0) // 1000,
            })
        self._send(200, {"ok": True, "drive": drive_id, "fid": parent, "entries": entries})

    def _do_download(self, drive_id, fid, name):
        drive = self._find_drive(drive_id)
        if not drive:
            self._send(404, {"ok": False, "error": "网盘未接入"})
            return
        if not fid:
            self._send(400, {"ok": False, "error": "缺少文件 fid"})
            return
        os.makedirs(DL_DIR, exist_ok=True)
        safe = re.sub(r"[^A-Za-z0-9一-鿿._-]", "_", name or "download")[:80]
        outdir = tempfile.mkdtemp(prefix="dl_", dir=DL_DIR)
        rc, out = run_cli(drive_id, ["download", "--fid", fid, "--output-dir", outdir],
                          timeout=300)
        _, result = parse_cli_lines(out)
        files = [os.path.join(outdir, f) for f in os.listdir(outdir)
                 if os.path.isfile(os.path.join(outdir, f))]
        if not files:
            shutil.rmtree(outdir, ignore_errors=True)
            self._send(200, {"ok": False, "error": cli_err(result, "下载失败")})
            return
        path = files[0]
        try:
            size = os.path.getsize(path)
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(size))
            self.send_header("Content-Disposition",
                             'attachment; filename*=UTF-8\'\'%s' %
                             urllib.parse.quote(safe))
            self.end_headers()
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(256 * 1024)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
        finally:
            shutil.rmtree(outdir, ignore_errors=True)

    # ── POST ──
    def do_POST(self):
        if not self._check_auth():
            self._send(401, {"ok": False, "error": "未授权"})
            return
        # v4.0.x: unified_router 借 /api/agent 前缀做 lucky(16666) 别名路由,
        # 但 handler 分支只认 /api/clouddrive/* → 别名请求进来全落 404。
        # 在解析前把别名前缀归一,原路径不受影响。
        if self.path.startswith("/api/agent/clouddrive"):
            self.path = self.path.replace("/api/agent/clouddrive", "/api/clouddrive", 1)
        parsed = urllib.parse.urlparse(self.path)
        try:
            n = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(n) or b"{}") if n else {}
        except Exception:  # noqa: BLE001
            body = {}

        if parsed.path.startswith("/api/clouddrive/add"):
            self._do_add(body)
            return
        if parsed.path.startswith("/api/clouddrive/remove"):
            self._do_remove(body)
            return
        self._send(404, {"ok": False, "error": "Not Found"})

    def _do_add(self, body):
        url = str(body.get("skill_url", "")).strip()
        code = str(body.get("auth_code", "")).strip()
        if not url:
            self._send(400, {"ok": False, "error": "请填写技能地址"})
            return
        if not code:
            self._send(400, {"ok": False, "error": "请填写授权码"})
            return
        with _LOCK:
            drives = _load_drives()
            if len(drives) >= MAX_TOTAL:
                self._send(400, {"ok": False, "error": "接入数量已达上限"})
                return
        try:
            skill_id, log = install_skill(url)
        except Exception as exc:  # noqa: BLE001
            self._send(200, {"ok": False, "error": "安装失败：%s" % exc})
            return
        with _LOCK:
            drives = _load_drives()
            for d in drives:
                if d.get("id") == skill_id:
                    drives.remove(d)
        try:
            nickname = bind_account(skill_id, code)
        except Exception as exc:  # noqa: BLE001
            with _LOCK:
                drives = _load_drives()
                drives.append({"id": skill_id, "name": display_name(skill_id),
                               "nickname": "", "status": "error", "error": str(exc)[:200],
                               "added_at": int(time.time())})
                _save_drives(drives)
            self._send(200, {"ok": False, "error": str(exc)[:300], "drive": skill_id})
            return
        entry = {"id": skill_id, "name": display_name(skill_id), "nickname": nickname,
                 "status": "ready", "error": "", "added_at": int(time.time())}
        with _LOCK:
            drives = _load_drives()
            drives.append(entry)
            _save_drives(drives)
        self._send(200, {"ok": True, "drive": entry,
                         "msg": "已接入" + (("（%s）" % nickname) if nickname else "")})

    def _do_remove(self, body):
        drive_id = str(body.get("id", "")).strip()
        drive = self._find_drive(drive_id)
        if not drive:
            self._send(404, {"ok": False, "error": "网盘未接入"})
            return
        msg = ""
        try:
            msg = remove_drive(drive_id)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)[:200]
        with _LOCK:
            drives = [d for d in _load_drives() if d.get("id") != drive_id]
            _save_drives(drives)
        self._send(200, {"ok": True, "id": drive_id,
                         "msg": "已移除" + ("（%s）" % msg if msg else "")})
