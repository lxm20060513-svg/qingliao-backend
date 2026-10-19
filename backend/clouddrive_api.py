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
  GET  /api/clouddrive/list?drive=<id>&fid=<fid>   （fid 空 = 根目录 0；GET 形态保留，兼容旧客户端）
  POST /api/clouddrive/list {drive,fid}            （v4.0.114：弱网加固，App 现走这条）
  GET  /api/clouddrive/download?drive=<id>&fid=<fid>&name=<文件名>

为什么 CLI 要 docker exec：官方 skill 的 CLI 是 node 程序，而 node 只装在 hermes 容器里
（qingliao 容器只有 python3）。skills 目录是同一宿主目录
（/data/hermes/skills → hermes 容器内 /data/hermes/skills），
所以这里统一「qingliao 容器内 docker exec hermes-container node ...」。
探活顺序固定：install.sh → login --token → get-user-info → 一个最轻的读操作（browse 根目录），
三段都拿到 code:0 才算接入成功（只到 install.sh 不算数）。
"""
import base64
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


# ── 百度网盘适配（官方 baidu-netdisk/bdpan-storage）──
# 与夸克「三处都不同」，所以按 drive id 分流，夸克链路（run_cli / quark-drive.cjs）一行未动：
#   ① CLI 不是 node 而是 bash 二进制 bdpan（装在 hermes 容器 /opt/data/home/.local/bin）
#   ② 命令按「路径」而不是 fid（App 只透传 fid，这里把路径 b64 包成 fid）
#   ③ 授权是 OOB 授权码 + 本地 config.json（没有 --token 那种服务端换取）
BAIDU_ID = "baidu-drive"
BAIDU_NAME = "百度网盘"
BAIDU_URL_HINTS = ("bdpan", "baidu-netdisk")
BAIDU_SKILL_ZIP = "https://codeload.github.com/baidu-netdisk/bdpan-storage/zip/refs/heads/main"
BAIDU_CLI = os.environ.get("QL_BAIDU_CLI", "/opt/data/home/.local/bin/bdpan")
# ⚠️ docker exec 默认 HOME=/root，而 bdpan 凭据在 $HOME/.config/bdpan → 不显式给 HOME
# 会稳定报「请先执行 bdpan login」（实测：直连 hermes 容器已登录、从 qingliao 容器
# docker exec 进去就变未登录）。这一条是本适配最容易踩的坑。
BAIDU_HOME = os.environ.get("QL_BAIDU_HOME", "/opt/data/home")
BAIDU_ROOT = "/apps/bdpan"          # 官方 CLI 只允许在该应用目录内操作
BAIDU_ROOT_FID = "bdp:root"
BAIDU_TIMEOUT = 120
# ⚠️ 下载落点必须是「两个容器都看得见的同一个物理目录」：bdpan 在 hermes 容器里跑，
# 而读文件、回传是 qingliao 容器干的。qingliao 的 /data 在 hermes 容器里并不存在
# （实测：hermes 容器内无 /data），所以改用 hermes-data —— 它被两边同时挂载。
BAIDU_DL_HERMES = os.environ.get("QL_BAIDU_DL_HERMES", "/opt/data/clouddrive_dl")
BAIDU_DL_LOCAL = os.environ.get("QL_BAIDU_DL_LOCAL",
                                "/volume1/docker/hermes/hermes-data/clouddrive_dl")


def is_baidu(drive_id):
    t = str(drive_id or "")
    return t == BAIDU_ID or t.startswith("baidu")


def is_baidu_url(url):
    u = str(url or "").lower()
    return any(h in u for h in BAIDU_URL_HINTS)


def _b64e(text):
    return base64.urlsafe_b64encode(text.encode("utf-8")).decode().rstrip("=")


def _b64d(text):
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4)).decode("utf-8")


def bd_fid(path):
    """网盘路径 → App 认的 fid（App 只透传不解析，形态自由）。"""
    return "bdp:" + _b64e(path)


def bd_path(fid):
    """fid → 应用目录内的相对路径（根 = ""）。非本适配生成的 fid 一律回落根，不猜。"""
    raw = str(fid or "")
    if not raw or raw in ("0", BAIDU_ROOT_FID):
        return ""
    if not raw.startswith("bdp:"):
        return ""
    try:
        p = _b64d(raw[4:])
    except Exception:  # noqa: BLE001
        return ""
    if not p.startswith(BAIDU_ROOT):
        return ""          # 非应用目录内的路径一律回落根（bdpan 自身也只允许该目录）
    p = p[len(BAIDU_ROOT):].strip("/")
    if ".." in p.split("/"):
        return ""
    return p


def parse_bd_json(text):
    """bdpan --json 会先打进度条再打正文，这里取「第一个能解析完整的 JSON 值」。"""
    t = text or ""
    dec = json.JSONDecoder()
    for idx, ch in enumerate(t):
        if ch in "{[":
            try:
                obj, _ = dec.raw_decode(t[idx:])
            except Exception:  # noqa: BLE001
                continue
            return obj
    return None


def run_bdpan(args, timeout=BAIDU_TIMEOUT, stdin_text=None):
    """在 hermes 容器内跑 bdpan（stdin_text 供 login --set-code-stdin 喂授权码）。"""
    cmd = ["docker", "exec", "-e", "HOME=" + BAIDU_HOME, "-e", "HERMES_INTERACTIVE=1"]
    if stdin_text is not None:
        cmd.append("-i")
    cmd.append(HERMES_CONTAINER)
    cmd.append(BAIDU_CLI)
    cmd += list(args) + ["--json", "--no-check-update"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                           input=stdin_text)
    except subprocess.TimeoutExpired:
        return 124, ""
    except Exception as exc:  # noqa: BLE001
        return 125, str(exc)
    return r.returncode, (r.stdout or "") + (("\n" + r.stderr) if r.stderr else "")


def _bd_epoch(iso):
    try:
        import datetime
        return int(datetime.datetime.fromisoformat(str(iso)).timestamp())
    except Exception:  # noqa: BLE001
        return 0


def bd_entries(rel):
    """列目录 → App 期望的 entries 形态。rel="" 表示应用目录根。"""
    rc, out = run_bdpan(["ls"] + ([rel] if rel else []))
    data = parse_bd_json(out)
    if isinstance(data, dict):
        raise ValueError(str(data.get("error") or "读取网盘文件失败"))
    if not isinstance(data, list):
        raise ValueError("网盘返回异常（退出码 %s）" % rc)
    entries = []
    for it in data:
        if not isinstance(it, dict):
            continue
        name = str(it.get("server_filename") or "")
        if not name:
            continue
        entries.append({
            "fid": bd_fid(str(it.get("path") or "")),
            "name": name,
            "is_dir": bool(it.get("isdir")),
            "size": int(it.get("size") or 0),
            "mtime": _bd_epoch(it.get("server_mtime")),
        })
    return entries


def bd_whoami():
    rc, out = run_bdpan(["whoami"])
    data = parse_bd_json(out)
    if not isinstance(data, dict) or not data.get("authenticated"):
        return ""
    return str(data.get("username") or "")


def probe_baidu():
    name = bd_whoami()
    if not name:
        return None, "授权已失效（bdpan 未登录）"
    return name, ""


def baidu_cli_ready():
    try:
        r = subprocess.run(["docker", "exec", HERMES_CONTAINER, "test", "-x", BAIDU_CLI],
                           capture_output=True, timeout=30)
    except Exception:  # noqa: BLE001
        return False
    return r.returncode == 0


def install_baidu_skill():
    """从官方仓库装 skills/baidu-drive 那一层（整仓 zip 顶层没有 SKILL.md，不能走 detect_skill_id）。"""
    workdir = tempfile.mkdtemp(prefix="qlskill_", dir="/tmp")
    try:
        zip_path = download_skill_zip(BAIDU_SKILL_ZIP, workdir)
        safe_extract_zip(zip_path, workdir)
        staging = ""
        for name in sorted(os.listdir(workdir)):
            cand = os.path.join(workdir, name, "skills", BAIDU_ID)
            if os.path.isfile(os.path.join(cand, "SKILL.md")):
                staging = cand
                break
        if not staging:
            raise ValueError("技能包里没找到 skills/%s（仓库结构可能变了）" % BAIDU_ID)
        dest = _skill_path(HOST_SKILLS_DIR, BAIDU_ID)
        if os.path.isdir(dest):
            shutil.rmtree(dest, ignore_errors=True)
        shutil.copytree(staging, dest)
        for f in ("install.sh", "login.sh"):
            p = os.path.join(dest, "scripts", f)
            if os.path.isfile(p):
                os.chmod(p, 0o755)
        return dest
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def ensure_baidu_ready():
    """skill 目录 / CLI 二进制缺一补一（幂等，都齐时零动作）。"""
    if not os.path.isfile(os.path.join(_skill_path(HOST_SKILLS_DIR, BAIDU_ID), "SKILL.md")):
        install_baidu_skill()
    if baidu_cli_ready():
        return
    script = "/opt/data/skills/%s/scripts/install.sh" % BAIDU_ID
    r = subprocess.run(["docker", "exec", "-e", "HOME=" + BAIDU_HOME, HERMES_CONTAINER,
                        "bash", script, "--yes"],
                       capture_output=True, text=True, timeout=INSTALL_TIMEOUT)
    if r.returncode != 0 or not baidu_cli_ready():
        raise ValueError("bdpan CLI 安装失败：%s"
                         % (((r.stdout or "") + (r.stderr or ""))[-200:] or "无输出"))


def bind_baidu(code):
    """授权：给了授权码就走 --set-code-stdin；没给则要求 CLI 已登录（幂等）。"""
    if code:
        rc, out = run_bdpan(["login", "--set-code-stdin", "--accept-disclaimer"],
                            timeout=90, stdin_text=code + "\n")
        data = parse_bd_json(out)
        if isinstance(data, dict) and data.get("error"):
            raise ValueError(str(data["error"])[:200])
        if rc != 0 and not bd_whoami():
            raise ValueError("授权失败（退出码 %s）" % rc)
    name = bd_whoami()
    if not name:
        raise ValueError("百度网盘未登录：请先取授权码（bdpan login --get-auth-url）再接入")
    try:
        bd_entries("")
    except Exception as exc:  # noqa: BLE001
        raise ValueError("网盘文件列表读取失败：%s" % str(exc)[:160])
    return name


def remove_baidu():
    """解绑 = 注销服务端登录态；skill 目录不动（那是 Hermes 侧技能，不归本台账管）。"""
    rc, out = run_bdpan(["logout"], timeout=60)
    data = parse_bd_json(out)
    if isinstance(data, dict) and data.get("error"):
        return str(data["error"])[:160]
    return ""


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
    if is_baidu(skill_id):
        return probe_baidu()
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
    if is_baidu(skill_id):
        msg = remove_baidu()
        return msg
    rc, out = run_cli(skill_id, ["unauthorize"], timeout=60)
    _, result = parse_cli_lines(out)
    msg = "" if cli_ok(result) else cli_err(result, "解绑未确认")
    dest = _skill_path(HOST_SKILLS_DIR, skill_id)
    shutil.rmtree(dest, ignore_errors=True)
    return msg


# ── 展示名 ──
DISPLAY_NAMES = {"quarkclouddrive": "夸克网盘", BAIDU_ID: BAIDU_NAME}


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
        if is_baidu(drive_id):
            self._do_list_baidu(drive_id, fid)
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
        if is_baidu(drive_id):
            self._do_download_baidu(drive_id, fid, name)
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
        # v4.0.114（用户「让弱网更稳」）：列表也收 POST —— iOS 蜂窝直连「带 query 的 GET」会静默挂
        # 10s 超时（App 侧 AuthStore 有实测定性；files_api 的产物列表 v4.0.110 同因已改 POST）。
        # 前端改用 POST body {drive,fid}；GET 形态保留，旧客户端不受影响。
        if parsed.path.startswith("/api/clouddrive/list"):
            self._do_list(str(body.get("drive", "")), str(body.get("fid", "")))
            return
        self._send(404, {"ok": False, "error": "Not Found"})

    def _do_add(self, body):
        url = str(body.get("skill_url", "")).strip()
        code = str(body.get("auth_code", "")).strip()
        if not url:
            self._send(400, {"ok": False, "error": "请填写技能地址"})
            return
        if is_baidu_url(url):
            self._do_add_baidu(url, code)
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

    def _do_list_baidu(self, drive_id, fid):
        rel = bd_path(fid)
        try:
            entries = bd_entries(rel)
        except Exception as exc:  # noqa: BLE001
            self._send(200, {"ok": False, "error": str(exc)[:200]})
            return
        self._send(200, {"ok": True, "drive": drive_id,
                         "fid": fid or BAIDU_ROOT_FID, "entries": entries})

    def _do_download_baidu(self, drive_id, fid, name):
        rel = bd_path(fid)
        if not rel:
            self._send(400, {"ok": False, "error": "缺少文件路径"})
            return
        os.makedirs(BAIDU_DL_LOCAL, exist_ok=True)
        safe = re.sub(r"[^A-Za-z0-9一-鿿._-]", "_", name or "download")[:80]
        outdir = tempfile.mkdtemp(prefix="dl_", dir=BAIDU_DL_LOCAL)
        # bdpan 跑在 hermes 容器里 → 落点写「hermes 视角」；本进程读文件用「qingliao 视角」
        outdir_h = os.path.join(BAIDU_DL_HERMES, os.path.basename(outdir))
        dest_h = os.path.join(outdir_h, safe)
        dest = os.path.join(outdir, safe)
        # ⚠️ bdpan download 第二个参数是「落点」：给已存在的目录会报
        # rename … file exists（实测），所以这里传文件路径而不是目录。
        rc, out = run_bdpan(["download", rel, dest_h], timeout=300)
        data = parse_bd_json(out)
        if not os.path.isfile(dest):
            err = str(data.get("error") or "") if isinstance(data, dict) else ""
            shutil.rmtree(outdir, ignore_errors=True)
            self._send(200, {"ok": False,
                             "error": err or ("下载失败（退出码 %s）" % rc)})
            return
        try:
            size = os.path.getsize(dest)
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(size))
            self.send_header("Content-Disposition",
                             'attachment; filename*\'\'%s' %
                             urllib.parse.quote(safe))
            self.end_headers()
            with open(dest, "rb") as f:
                while True:
                    chunk = f.read(256 * 1024)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
        finally:
            shutil.rmtree(outdir, ignore_errors=True)

    def _do_add_baidu(self, url, code):
        """百度网盘接入：补齐 skill/CLI → 授权 → 登记台账（可重入）。"""
        with _LOCK:
            drives = _load_drives()
            if len(drives) >= MAX_TOTAL and not any(d.get("id") == BAIDU_ID for d in drives):
                self._send(400, {"ok": False, "error": "接入数量已达上限"})
                return
        try:
            ensure_baidu_ready()
            nickname = bind_baidu(code)
        except Exception as exc:  # noqa: BLE001
            err = str(exc)[:300]
            with _LOCK:
                drives = [d for d in _load_drives() if d.get("id") != BAIDU_ID]
                drives.append({"id": BAIDU_ID, "name": BAIDU_NAME, "nickname": "",
                               "status": "error", "error": err,
                               "added_at": int(time.time())})
                _save_drives(drives)
            self._send(200, {"ok": False, "error": err, "drive": BAIDU_ID})
            return
        entry = {"id": BAIDU_ID, "name": BAIDU_NAME, "nickname": nickname,
                 "status": "ready", "error": "", "added_at": int(time.time())}
        with _LOCK:
            drives = [d for d in _load_drives() if d.get("id") != BAIDU_ID]
            drives.append(entry)
            _save_drives(drives)
        self._send(200, {"ok": True, "drive": entry,
                         "msg": "已接入" + BAIDU_NAME +
                                (("（%s）" % nickname) if nickname else "")})

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
