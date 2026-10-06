#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""后端一键更新 + 版本检测 API：/api/update

面向 App「关于 / 后端版本」卡片弹窗的「一键更新」按钮。

接口：
  GET  /api/update/check    检测 GitHub 上有没有新版（只读，不改任何文件）
  POST /api/update/apply    执行更新（异步，立即返回 job_id；**会重启容器**）
  GET  /api/update/status   查更新任务进度 / 结果（重启后仍可读，走磁盘状态文件）

为什么不用 update.sh（重要，别照抄 update.sh 的逻辑）：
  update.sh 走 `git fetch` + `git merge --ff-only` + `docker compose up -d --build`，
  但**生产部署目录不是 git 仓库，NAS 宿主与容器内都没有 git 二进制**
  （实测 2026-10-01：`git -C 轻聊web rev-parse` → command not found，
    容器内 `which git` → not found）。update.sh 只适用于「用户自己 clone 下来的目录」。
  ⇒ 本模块在生产走**无 git 路径**：GitHub API 取最新 tag/commit →
     urllib 下载 codeload tar.gz → 解包 → 只替换 backend/ 下的 *.py →
     py_compile 逐个语法校验 → 写 QL_VERSION → docker compose restart。
  仓库里有 git 时（源码直跑场景）也照样走 tarball，保持唯一真值来源，不做双路径。

安全设计：
1. **全接口鉴权**（auth_api.check_auth）。这是能替换后端代码、改容器状态的接口，
   绝不能像 /api/version 那样免鉴权 —— 免鉴权等于任何人都能远程换掉后端。
2. **不碰用户数据**：只替换 backend/ 下的 .py；data/ 目录一个字节都不动。
3. **更新前必备份**：backend/ 整目录 tar 到 backups/backend-<ts>.tar.gz。
4. **单任务互斥**：同一时刻只允许一个更新任务（_JOB_LOCK + 状态文件），
   防连点两下并发重启容器。
5. **重启前查在途任务**：容器 restart 会杀掉所有进行中的流式 AI 对话
   （_tasks 在内存里）。有在途任务时默认拒绝并告诉用户稍后再试，force=true 才强推。
6. **异步执行 + 磁盘状态**：容器重启 = 进程换掉，同步 HTTP 连接必断，
   所以更新在后台线程跑，进度写 QL_UPDATE_STATE_DIR/job.json，
   重启后 GET /api/update/status 仍能读到结果（App 可轮询）。
7. **失败可回滚**：代码替换失败（语法校验挂）自动还原备份目录。

关键环境变量：
  QL_BACKEND_DIR         backend 目录（默认按生产路径，再退回本文件所在目录）
  QL_ROOT_DIR            仓库根（默认 backend 的上级目录）
  QL_DOCKER_COMPOSE_FILE compose 文件路径（用于 restart / 需要时 --build）
  QL_DOCKER_CONTAINER    容器名（默认 qingliao）
  QL_UPDATE_REPO         GitHub 仓库（默认 lxm20060513-svg/qingliao-backend）
  QL_UPDATE_STATE_DIR    任务状态目录（默认 QL_DATA_DIR/update_state）
  QL_UPDATE_DISABLED=1   硬开关关掉写操作（只留 check）
"""
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict
from http.server import BaseHTTPRequestHandler

# ── 常量 ────────────────────────────────────────────────────
DEFAULT_REPO = os.environ.get("QL_UPDATE_REPO", "lxm20060513-svg/qingliao-backend")
CONTAINER = os.environ.get("QL_DOCKER_CONTAINER", "qingliao")
STATE_DIR = os.environ.get(
    "QL_UPDATE_STATE_DIR",
    os.path.join(os.environ.get("QL_DATA_DIR", "/data"), "update_state"))

_HERE = os.path.dirname(os.path.abspath(__file__))
# 生产：/volume1/docker/hermes/微信文件/轻聊web/backend；源码直跑：本文件同级
BACKEND_DIR = os.environ.get(
    "QL_BACKEND_DIR",
    "/volume1/docker/hermes/微信文件/轻聊web/backend"
    if os.path.isdir("/volume1/docker/hermes/微信文件/轻聊web/backend")
    else _HERE)
ROOT_DIR = os.environ.get("QL_UPDATE_ROOT", os.path.dirname(BACKEND_DIR.rstrip("/")))
COMPOSE_FILE = os.environ.get(
    "QL_DOCKER_COMPOSE_FILE",
    os.path.join(os.path.dirname(ROOT_DIR), "docker", "docker-compose.yml"))
COMPOSE_DIR = os.path.dirname(COMPOSE_FILE)

# ⚠️ 状态文件路径**必须在每次使用时现算**，不能在 import 时定死：
#   STATE_DIR 来自环境变量，改环境变量后重载模块仍要指向新目录（测试沙箱、
#   运维临时改 QL_UPDATE_STATE_DIR 都依赖这一点）。import 时定死曾导致
#   _write_state 静默写失败（目录不存在被 except 吞掉），更新结果全丢。
LOG_FILE_NAME = "job.log"
STATE_FILE_NAME = "job.json"
CHECK_CACHE_NAME = "check_cache.json"

_HTTP_TIMEOUT = 30          # GitHub API 单次超时
_DL_TIMEOUT = 180           # tar.gz 下载超时
_MAX_TGZ = 60 * 1024 * 1024  # 60MB 上限，防下到异常大包把内存/磁盘打爆
_CHECK_CACHE_TTL = 300      # check 结果缓存 5 分钟（GitHub 匿名 API 限流 60 次/小时）

# 每次更新必给用户的注意事项（App 直接显示，别自己编措辞）
WARNINGS = [
    "更新过程会**重启后端容器**（约 10-30 秒），期间 App 会短暂连不上，属正常现象。",
    "重启会中断**正在进行中的 AI 对话**，请先等对话结束再点更新。",
    "仅替换 backend/ 代码，**不会动你的 data/ 数据**（会话/记忆/配置全部保留）。",
    "更新前会自动把 backend/ 打包备份到 backups/，出问题可手动还原。",
]


# ── 状态存取 ────────────────────────────────────────────────
_JOB_LOCK = threading.Lock()
_running: Dict[str, Any] = {"job_id": None}


def _now():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _state_path(name):
    """状态/日志文件全路径（每次现算，见 STATE_FILE_NAME 处的说明）"""
    return os.path.join(STATE_DIR, name)


def _read_state():
    try:
        with open(_state_path(STATE_FILE_NAME), encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _write_state(d):
    """原子写状态（tmp + replace），重启后 status 才能读到完整结果"""
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        path = _state_path(STATE_FILE_NAME)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        # 状态写不进去 = 用户重启后查不到结果，必须留痕而不是彻底静默
        print("[update] 状态写入失败：%s" % str(e)[:150], flush=True)


def _log(msg):
    line = "[%s] %s" % (_now(), msg)
    print("[update] " + msg, flush=True)
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        with open(_state_path(LOG_FILE_NAME), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def _tail_log(n=60):
    try:
        with open(_state_path(LOG_FILE_NAME), encoding="utf-8") as f:
            return f.read().splitlines()[-n:]
    except Exception:
        return []


# ── 本地版本 ────────────────────────────────────────────────
def _local_version():
    """读本地版本（复用 version_api 的三级回退，逻辑只有一份）"""
    info = {}
    try:
        import version_api
        info = version_api.get_version_info() or {}
    except Exception:
        pass
    if info.get("commit"):
        return {"version": info.get("version", "") or "unknown",
                "commit": info.get("commit", ""),
                "built": info.get("built", "") or ""}
    # version_api 读不到 commit（没 env 没 QL_VERSION 没 .git）→ 自己再试一次
    for name, path in (("QL_BACKEND_COMMIT", os.path.join(BACKEND_DIR, "QL_VERSION")),):
        try:
            with open(path, encoding="utf-8") as f:
                lines = [l.strip() for l in f.read().splitlines() if l.strip()]
            if lines:
                return {"version": lines[0] if len(lines) == 3 else "unknown",
                        "commit": lines[1] if len(lines) > 1 else "",
                        "built": lines[2] if len(lines) > 2 else ""}
        except Exception:
            continue
    return {"version": os.environ.get("QL_BACKEND_VERSION", "") or "unknown",
            "commit": os.environ.get("QL_BACKEND_COMMIT", "") or "",
            "built": os.environ.get("QL_BACKEND_BUILT", "") or ""}


# ── 远端版本（GitHub API，匿名可用，限流 60/h）───────────────
def _gh_json(path):
    url = "https://api.github.com/repos/%s%s" % (DEFAULT_REPO, path)
    req = urllib.request.Request(url, headers={
        "Accept": "application/vnd.github+json",
        "User-Agent": "qingliao-backend-update-check",
    })
    with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as r:
        return json.loads(r.read().decode("utf-8"))


def _remote_version():
    """取远端最新 tag 与 main 最新 commit（任一失败都返回 ok=False + 原因）"""
    out: Dict[str, Any] = {"tag": "", "tag_commit": "", "commit": "", "commit_date": "",
                           "commit_msg": "", "tarball": "", "notes": ""}
    # 最新 tag（按时间倒序第一个）
    try:
        tags = _gh_json("/tags?per_page=10")
        if isinstance(tags, list) and tags:
            t = tags[0]
            out["tag"] = (t.get("name") or "").strip()
            sha = (t.get("commit") or {}).get("sha") or ""
            out["tag_commit"] = sha[:7]
    except Exception as e:
        out["ok"] = False
        out["error"] = "读取远端 tag 失败：%s" % str(e)[:150]
        return out
    try:
        c = _gh_json("/commits/main")
        out["commit"] = (c.get("sha") or "")[:7]
        out["commit_date"] = (((c.get("commit") or {}).get("committer") or {})
                              .get("date") or "")[:10]
        out["commit_msg"] = ((c.get("commit") or {}).get("message") or "").splitlines()[0][:100]
    except Exception as e:
        out["ok"] = False
        out["error"] = "读取远端提交失败：%s" % str(e)[:150]
        return out
    out["tarball"] = ("https://codeload.github.com/%s/tar.gz/refs/heads/main" % DEFAULT_REPO)
    out["ok"] = True
    return out


# ── 有没有新版 ──────────────────────────────────────────────
def check_update(force=False):
    """版本检测。返回体里 has_update=false 时 App 不要弹升级提示（用户明令：有新版才提示）。"""
    local = _local_version()
    cache_key = _state_path(CHECK_CACHE_NAME)
    if not force:
        try:
            if os.path.exists(cache_key) and time.time() - os.path.getmtime(cache_key) < _CHECK_CACHE_TTL:
                with open(cache_key, encoding="utf-8") as f:
                    cached = json.load(f)
                cached["cached"] = True
                return cached
        except Exception:
            pass

    remote = _remote_version()
    base = {
        "ok": False,
        "local": local,
        "has_update": False,
        "remote": {},
        "behind": 0,
        "commits": [],
        "restart_required": True,
        "warnings": WARNINGS,
        "checked_at": _now(),
    }
    if not remote.get("ok"):
        base["error"] = remote.get("error", "远端信息获取失败")
        return base

    remote_commit = remote.get("commit", "")
    # 判断口径：本地 commit 与远端 main commit 不一致 = 有更新。
    # 不用 tag 比较（本地可能没打 tag，或 tag 与 main 不在一条线上会误报"已是最新"）。
    has_update = bool(local.get("commit")) and remote_commit and local["commit"] != remote_commit
    if not local.get("commit"):
        # 读不出本地 commit（既没 env 也没 QL_VERSION）→ 不猜，交给用户判断
        has_update = False
        base["local_unknown"] = True

    # ⚠️ 生产 commit 不在 GitHub 上时（生产用 ql.py 逐文件部署，从不 push，
    # 本地 commit 对 GitHub 是 404），behind_by/commits 都拿不到。此时**如实告诉调用方
    # "提交清单不可用"，不能默默回 behind=0** —— 那会让 App 显示"落后 0 个提交"，
    # 看起来像没有实质更新，与 has_update=true 自相矛盾。
    behind = 0
    commits = []
    notes = []
    if has_update:
        try:
            cmp_ = _gh_json("/compare/%s...%s" % (local.get("commit", ""), remote_commit))
            behind = int((cmp_ or {}).get("behind_by") or 0)
            commits = [{"sha": (x.get("sha") or "")[:7],
                        "date": ((x.get("commit") or {}).get("committer") or {}).get("date", "")[:10],
                        "msg": ((x.get("commit") or {}).get("message") or "").splitlines()[0][:100]}
                       for x in ((cmp_ or {}).get("commits") or [])[:30]]
        except urllib.error.HTTPError as e:
            if e.code == 404:
                notes.append("本地版本（%s）不在 GitHub 远端（生产为逐文件部署，从不 push），"
                             "无法生成提交清单。更新将直接对齐远端 main 最新代码。"
                             % local.get("commit", ""))
            else:
                notes.append("读取提交清单失败：HTTP %s" % e.code)
        except Exception as e:
            notes.append("读取提交清单失败：%s" % str(e)[:100])

    base.update({
        "ok": True,
        "has_update": has_update,
        "remote": {"tag": remote.get("tag"), "tag_commit": remote.get("tag_commit"),
                   "commit": remote_commit, "date": remote.get("commit_date"),
                   "msg": remote.get("commit_msg"), "repo": DEFAULT_REPO},
        "behind": behind,
        "commits": commits,
        "notes": notes,
        "cached": False,
    })
    if has_update:
        try:
            os.makedirs(STATE_DIR, exist_ok=True)
            with open(cache_key, "w", encoding="utf-8") as f:
                json.dump(base, f, ensure_ascii=False)
        except Exception:
            pass
    return base


# ── 在途任务检查（重启会杀掉它们）──────────────────────────
def _active_tasks():
    """取进行中的流式任务数（复用 stream_api 自己的收集函数，不另造一份判定口径）"""
    try:
        import stream_api
        fn = getattr(stream_api, "_collect_active_tasks", None)
        if fn:
            tasks = fn() or []
            busy = [t for t in tasks
                    if (t.get("state") or {}).get("streaming")
                    or t.get("state", {}).get("status") in ("streaming", "running")]
            return busy
    except Exception:
        pass
    return []


# ── 下载 + 解包 ─────────────────────────────────────────────
def _download_tarball(url):
    """流式下载到临时文件并返回路径（不把整包读进内存）"""
    req = urllib.request.Request(url, headers={"User-Agent": "qingliao-backend-updater"})
    fd, path = tempfile.mkstemp(prefix="ql-update-", suffix=".tar.gz")
    total = 0
    try:
        with urllib.request.urlopen(req, timeout=_DL_TIMEOUT) as r, os.fdopen(fd, "wb") as f:
            while True:
                chunk = r.read(262144)
                if not chunk:
                    break
                total += len(chunk)
                if total > _MAX_TGZ:
                    raise RuntimeError("下载包超过 %dMB，已中止" % (_MAX_TGZ // 1048576))
                f.write(chunk)
    except Exception:
        try:
            os.unlink(path)
        except Exception:
            pass
        raise
    return path, total


def _safe_members(tar, want_prefix):
    """只取 tar 里 backend/ 下的 .py，且做路径穿越防护（拒绝 ../ 与绝对路径）"""
    picked = []
    base = os.path.realpath(want_prefix) + os.sep
    for m in tar.getmembers():
        if not m.isfile():
            continue
        name = m.name
        parts = name.split("/")
        if ".." in parts or name.startswith("/"):
            continue
        if "backend" not in parts:
            continue
        idx = parts.index("backend")
        rel = os.path.join(*parts[idx + 1:]) if len(parts) > idx + 1 else ""
        if not rel.endswith(".py"):
            continue
        real = os.path.realpath(os.path.join(want_prefix, rel))
        if not real.startswith(base):
            continue
        picked.append((m, rel))
    return picked


# ── 备份 / 还原 ─────────────────────────────────────────────
def _backup_backend(tag):
    """整目录 tar 备份 backend/（不含 data/）；失败返回 None（调用方决定是否中止）"""
    try:
        bdir = os.path.join(ROOT_DIR, "backups")
        os.makedirs(bdir, exist_ok=True)
        dst = os.path.join(bdir, "backend-%s.tar.gz" % tag)
        import tarfile as _tf
        with _tf.open(dst, "w:gz") as t:
            t.add(BACKEND_DIR, arcname="backend")
        return dst
    except Exception as e:
        _log("⚠️ 备份失败：%s（继续更新，出问题无法自动还原）" % str(e)[:150])
        return None


def _restore_backup(bak):
    """从备份还原 backend/（语法校验失败时用）"""
    try:
        import tarfile as _tf
        with _tf.open(bak, "r:gz") as t:
            t.extractall(ROOT_DIR)
        _log("已从备份还原 backend/：%s" % bak)
        return True
    except Exception as e:
        _log("❌ 还原失败：%s（请手动解包 %s）" % (str(e)[:150], bak))
        return False


# ── 更新主流程（后台线程执行）────────────────────────────────
def _run_update(job_id, force):
    st: Dict[str, Any] = {"job_id": job_id, "state": "running", "started_at": _now(),
                          "steps": [], "error": "", "restart_required": True}

    def step(msg, ok=True):
        st["steps"].append({"t": _now(), "msg": msg, "ok": ok})
        _write_state(st)
        _log(("✅ " if ok else "❌ ") + msg)

    # ① 在途任务检查
    busy = [] if force else _active_tasks()
    if busy:
        st["state"] = "failed"
        st["error"] = "有 %d 个进行中的对话/任务，重启会中断它们。等它们结束再更新，或确认后重试（force=true）。" % len(busy)
        st["finished_at"] = _now()
        step(st["error"], False)
        _write_state(st)
        return

    # ② 拉远端信息
    try:
        remote = _remote_version()
        if not remote.get("ok"):
            raise RuntimeError(remote.get("error", "远端信息获取失败"))
        target = remote["commit"]
        step("远端最新：%s / %s（%s）" % (remote.get("tag") or "无 tag", target,
                                          remote.get("commit_date") or ""))
    except Exception as e:
        st["state"] = "failed"
        st["error"] = str(e)[:200]
        st["finished_at"] = _now()
        step(st["error"], False)
        _write_state(st)
        return

    # ③ 备份
    tag = time.strftime("%Y%m%d-%H%M%S")
    bak = _backup_backend(tag)
    step("已备份 backend/ → %s" % (bak or "（备份失败，继续）"))

    # ④ 下载
    tmp_tgz = None
    try:
        tmp_tgz, size = _download_tarball(remote["tarball"])
        step("已下载源码包 %.1f MB" % (size / 1048576.0))
    except Exception as e:
        st["state"] = "failed"
        st["error"] = "下载源码包失败：%s" % str(e)[:200]
        st["finished_at"] = _now()
        step(st["error"], False)
        _write_state(st)
        return

    # ⑤ 解包 + 只取 backend/*.py
    try:
        import tarfile as _tf
        with _tf.open(tmp_tgz, "r:gz") as t:
            members = _safe_members(t, BACKEND_DIR)
            if not members:
                raise RuntimeError("源码包里没有 backend/*.py，已中止（包结构不对）")
            extract_dir = tempfile.mkdtemp(prefix="ql-extract-")
            for m, rel in members:
                dst = os.path.join(extract_dir, rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                src = t.extractfile(m)
                if src:
                    with open(dst, "wb") as f:
                        shutil.copyfileobj(src, f)
        step("解出 %d 个后端 .py 文件" % len(members))
    except Exception as e:
        st["state"] = "failed"
        st["error"] = "解包失败：%s" % str(e)[:200]
        st["finished_at"] = _now()
        step(st["error"], False)
        _write_state(st)
        return
    finally:
        if tmp_tgz:
            try:
                os.unlink(tmp_tgz)
            except Exception:
                pass

    # ⑥ 语法校验（**替换前**全量编译，挂了就一个文件都不动）
    bad = []
    for root, _, files in os.walk(extract_dir):
        for fn in files:
            if not fn.endswith(".py"):
                continue
            p = os.path.join(root, fn)
            r = subprocess.run([sys.executable, "-m", "py_compile", p],
                               capture_output=True, text=True)
            if r.returncode != 0:
                bad.append("%s: %s" % (os.path.relpath(p, extract_dir),
                                       (r.stderr or r.stdout).strip().splitlines()[-1:]))
    if bad:
        st["state"] = "failed"
        st["error"] = "新代码语法校验失败，未改动任何文件：%s" % "；".join(bad[:3])
        st["finished_at"] = _now()
        step(st["error"], False)
        _write_state(st)
        return
    step("新代码语法校验通过")

    # ⑦ 替换（逐文件覆盖；只 .py，data/ 一个字节都不动）
    replaced = 0
    for root, _, files in os.walk(extract_dir):
        for fn in files:
            if not fn.endswith(".py"):
                continue
            src = os.path.join(root, fn)
            rel = os.path.relpath(src, extract_dir)
            dst = os.path.join(BACKEND_DIR, rel)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copyfile(src, dst)
            replaced += 1
    step("已替换 %d 个文件（仅 backend/ 下的 .py）" % replaced)

    # ⑧ 写版本文件（重启后 /api/version 立刻报新版本；env 变量仍是权威源）
    try:
        ver = _remote_version().get("tag") or "unknown"
        with open(os.path.join(BACKEND_DIR, "QL_VERSION"), "w", encoding="utf-8") as f:
            f.write("%s\n%s\n%s\n" % (ver, target,
                                      remote.get("commit_date") or time.strftime("%Y-%m-%d")))
        step("版本信息已写入 backend/QL_VERSION")
    except Exception as e:
        step("版本文件写入失败（不影响运行）：%s" % str(e)[:100], False)

    # ⑨ 清理 pycache，避免旧 .pyc 干扰
    for root, dirs, _ in os.walk(BACKEND_DIR):
        for d in list(dirs):
            if d == "__pycache__":
                shutil.rmtree(os.path.join(root, d), ignore_errors=True)
                dirs.remove(d)

    # ⑩ 重启容器
    st["restarting"] = True
    _write_state(st)
    step("正在重启容器 %s（约 10-30 秒，App 会短暂连不上）…" % CONTAINER)
    rc, out = _restart_container()
    st["restarting"] = False
    if rc != 0:
        st["state"] = "failed"
        st["error"] = "容器重启失败：%s" % out[:300]
        st["finished_at"] = _now()
        step(st["error"], False)
        _write_state(st)
        return

    # ⑪ 就绪等待 + 健康检查（重启会换进程，本进程可能已被换掉，失败也没关系）
    step("已发出重启指令，等待服务就绪…")
    ready = _wait_ready(80)
    st["state"] = "success" if ready else "unknown"
    st["finished_at"] = _now()
    st["backup"] = bak
    st["replaced_files"] = replaced
    st["target"] = {"tag": remote.get("tag"), "commit": target}
    if ready:
        step("✅ 更新完成，服务已就绪")
    else:
        step("⚠️ 80 秒内未探测到服务恢复，请手动确认：docker logs --tail=50 %s" % CONTAINER, False)
    _write_state(st)


def _restart_container():
    """重启容器：优先 docker compose restart（保留现有配置），失败退回 docker restart"""
    if os.path.exists(COMPOSE_FILE):
        try:
            r = subprocess.run(["docker", "compose", "-f", COMPOSE_FILE, "restart"],
                               cwd=COMPOSE_DIR, capture_output=True, text=True, timeout=120)
            if r.returncode == 0:
                return 0, (r.stdout or "")[-500:]
            # compose restart 在部分 UGOS 环境不可用 → 退回 docker restart
        except Exception:
            pass
    try:
        r = subprocess.run(["docker", "restart", CONTAINER],
                           capture_output=True, text=True, timeout=120)
        return r.returncode, (r.stdout + r.stderr)[-500:]
    except Exception as e:
        return 1, str(e)[:300]


def _wait_ready(seconds):
    """轮询本机 9127 健康接口；重启换进程时本连接已断，探测失败属预期"""
    import urllib.parse
    url = "http://127.0.0.1:9127/api/version"
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                if r.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(3)
    return False


# ── HTTP Handler ────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    """GET /api/update/check · POST /api/update/apply · GET /api/update/status"""

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token, X-Update-Password")

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            pass   # 更新过程中连接被重启掐断是正常的，别再往上抛

    def _auth(self):
        """鉴权：与 secrets_api/docker_api 同款（token 为主，密码头兜底默认关闭）"""
        if os.environ.get("QL_UPDATE_DISABLED") == "1" and "/check" in self.path:
            return True
        try:
            import auth_api
            return auth_api.check_auth(self.headers, "X-Update-Password", "")
        except Exception:
            return False

    def _read_json(self):
        try:
            n = int(self.headers.get("Content-Length", "0"))
            return json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            return {}

    def do_OPTIONS(self):
        try:
            self.send_response(204)
            self._cors()
            self.end_headers()
        except Exception:
            pass

    def do_GET(self):
        if not self._auth():
            self._send(401, {"ok": False, "error": "未授权"})
            return
        path = self.path.split("?")[0]
        if path.startswith("/api/update/check"):
            force = "force=1" in self.path or "force=true" in self.path
            self._send(200, check_update(force=force))
            return
        if path.startswith("/api/update/status"):
            st = _read_state()
            st["log"] = _tail_log(60)
            st["warnings"] = WARNINGS
            if _running["job_id"]:
                st["running"] = True
            self._send(200, {"ok": True, "job": st,
                             "busy_tasks": len(_active_tasks())})
            return
        self._send(404, {"ok": False, "error": "Not Found"})

    def do_POST(self):
        if not self._auth():
            self._send(401, {"ok": False, "error": "未授权"})
            return
        path = self.path.split("?")[0]
        if not path.startswith("/api/update/apply"):
            self._send(404, {"ok": False, "error": "Not Found"})
            return
        if os.environ.get("QL_UPDATE_DISABLED") == "1":
            self._send(200, {"ok": False, "error": "本机已禁用后端更新（QL_UPDATE_DISABLED=1）"})
            return

        body = self._read_json()
        force = bool(body.get("force"))

        # 互斥：防连点两下并发重启
        with _JOB_LOCK:
            if _running["job_id"]:
                self._send(200, {"ok": False, "job_id": _running["job_id"],
                                 "error": "已有更新任务在执行，请等它结束"})
                return
            job_id = "upd-" + time.strftime("%Y%m%d-%H%M%S")
            _running["job_id"] = job_id

        _write_state({"job_id": job_id, "state": "queued", "started_at": _now(),
                      "steps": [], "restart_required": True, "restarting": False})
        t = threading.Thread(target=_run_update_wrapper, args=(job_id, force), daemon=True)
        t.start()
        # 立即返回，不等更新（容器一重启这条 HTTP 连接必断）
        self._send(200, {"ok": True, "job_id": job_id, "state": "running",
                         "restart_required": True, "warnings": WARNINGS,
                         "poll": "/api/update/status"})

    def log_message(self, format, *args):
        pass   # 静默：这个接口不该刷日志


def _run_update_wrapper(job_id, force):
    """跑完更新后清掉互斥标记（异常也清，否则一次失败就把更新功能永久锁死）"""
    try:
        _run_update(job_id, force)
    except Exception as e:
        _log("❌ 更新线程异常：%s" % str(e)[:200])
        st = _read_state()
        st.update({"state": "failed", "error": str(e)[:300], "finished_at": _now()})
        _write_state(st)
    finally:
        with _JOB_LOCK:
            _running["job_id"] = None