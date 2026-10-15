#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""改密后「一键同步宿主环境」(v4.0.116)

    POST /api/auth/sync-env   {"password": "<刚在 App 里设置的新密码>"}  → 起后台同步任务
    GET  /api/auth/sync-env                                            → 任务状态（免鉴权，只回状态）

为什么需要它
------------
用户「修改登录密码」只重写 `auth_config.json` 的哈希（+ 吊销全部 token），**不动容器 env**。
而宿主上还有两份由 env 密码派生的副本在用：
  ① `docker/.env` 的 `QL_PASSWORD`（容器 env 真源）
  ② `/opt/data/.ql_cred`（Hermes 侧 rule.py 管自动化规则读的副本）
漂移后：Hermes 侧「密码换 token」的链路（rule.py 等）静默 401 —— 用户完全看不出来。

本模块用容器里挂着的 `docker.sock` 起一次性 helper 容器（docker:cli）在**宿主**上：
  ① 备份并改写 `docker/.env` 的 QL_PASSWORD（逐行替换，密码含特殊字符也安全）
  ② 刷新 `/opt/data/.ql_cred`（chown 10000:10000 + 600，与 Hermes 容器同属主）
  ③ `docker compose up -d --no-build --no-deps qingliao` 重建容器（~15s，env 才生效）
  ④ 回读新容器 env 与期望值比对，结果写入结果文件

安全
----
* 密码**只经 stdin 管道**传给 helper，不进 argv / 环境变量 / 日志；helper 输出只留 OK/FAIL。
* POST 必须用**正确的新密码**自证身份（改密后 token 已吊销，所以不能靠 token）。
* 状态文件只记状态/步骤/时间戳，不记密码。
* 重建容器会杀掉本进程 → 状态由 helper 落盘的结果文件在下次 GET 时归一化。
"""
import json
import os
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler

try:
    import auth_api
except Exception:
    auth_api = None

DATA_DIR = os.environ.get("QL_DATA_DIR", "/data")
HERMES_DATA = os.environ.get("QL_HERMES_DATA_DIR", "")
CONTAINER = os.environ.get("QL_SELF_CONTAINER", "qingliao")
HELPER_IMAGE = os.environ.get("QL_SELFUPDATE_HELPER_IMAGE", "docker:cli")
STATE_PATH = os.path.join(DATA_DIR, "pwenv_sync_state.json")
RESULT_PATH = os.path.join(DATA_DIR, "pwenv_sync_result.txt")
LOG_PATH = os.path.join(DATA_DIR, "pwenv_sync.log")
STALE_SECONDS = 10 * 60
_lock = threading.Lock()

_HELPER_SH = r'''
set -u
NEW=$(cat)                     # 密码只从 stdin 读进来：不进 argv / env / 日志
if [ -z "$NEW" ]; then printf 'FAIL 空密码' > "__RES__"; exit 2; fi
ENVF="$PWD/.env"
if [ ! -f "$ENVF" ]; then printf 'FAIL 找不到 .env' > "__RES__"; exit 3; fi
BAK="$ENVF.bak-pwchange-$(date +%Y%m%d%H%M%S)"
cp -a "$ENVF" "$BAK" || { printf 'FAIL 备份失败' > "__RES__"; exit 4; }
TMP="$ENVF.new.$$"
found=0
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in
    QL_PASSWORD=*) printf 'QL_PASSWORD=%s\n' "$NEW"; found=1 ;;
    *)             printf '%s\n' "$line" ;;
  esac
done < "$ENVF" > "$TMP" || { printf 'FAIL 改写失败' > "__RES__"; exit 5; }
if [ "$found" = "0" ]; then printf 'QL_PASSWORD=%s\n' "$NEW" >> "$TMP"; fi
mv "$TMP" "$ENVF" || { printf 'FAIL 落盘失败' > "__RES__"; exit 6; }
if [ -d /hermes ]; then
  printf '%s\n' "$NEW" > /hermes/.ql_cred \
    && chmod 600 /hermes/.ql_cred \
    && chown 10000:10000 /hermes/.ql_cred 2>/dev/null || true
fi
echo "[helper] compose up -d"
docker compose up -d --no-build --no-deps __CTR__ 2>&1 | tail -3
i=0; GOT=""
while [ $i -lt 45 ]; do
  GOT=$(docker exec __CTR__ printenv QL_PASSWORD 2>/dev/null) && [ -n "$GOT" ] && break
  i=$((i+1)); sleep 2
done
if [ "$GOT" = "$NEW" ]; then
  echo "[helper] 新容器 env 已生效"
  printf 'OK %s' "$(date +'%F %T')" > "__RES__"
else
  echo "[helper] 新容器 env 未生效"
  printf 'FAIL 新容器 env 未生效（可回滚：cp -a %s %s）' "$BAK" "$ENVF" > "__RES__"
fi
'''


def _load_state():
    try:
        with open(STATE_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"status": "idle"}


def _save_state(state):
    try:
        tmp = STATE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, STATE_PATH)
    except Exception:
        pass


def _compose_dir():
    d = (os.environ.get("QL_COMPOSE_DIR") or "").strip()
    if d and os.path.isdir(d):
        return d
    try:
        r = subprocess.run(
            ["docker", "inspect", CONTAINER, "--format",
             '{{index .Config.Labels "com.docker.compose.project.working_dir"}}'],
            capture_output=True, text=True, timeout=20)
        d = (r.stdout or "").strip()
        if d and os.path.isdir(d):
            return d
    except Exception:
        pass
    return ""


def _tail(n=8):
    try:
        with open(LOG_PATH, encoding="utf-8", errors="ignore") as f:
            return "".join(f.readlines()[-n:]).strip()[:600]
    except Exception:
        return ""


def _normalize(state):
    if state.get("status") != "running":
        return state
    if os.path.exists(RESULT_PATH):
        try:
            with open(RESULT_PATH, encoding="utf-8", errors="ignore") as f:
                r = f.read().strip()
        except Exception:
            r = ""
        if r:
            if r.startswith("OK"):
                state["status"] = "done"
                state["step"] = "同步完成，后端已用新密码重启"
                state.pop("error", None)
            else:
                state["status"] = "failed"
                state["error"] = r[:300]
                state["step"] = "同步失败"
            state["finished_at"] = time.time()
            _save_state(state)
        return state
    if time.time() - float(state.get("started_at") or 0) > STALE_SECONDS:
        state["status"] = "failed"
        state["error"] = "任务超时（未拿到 helper 结果）"
        _save_state(state)
    return state


def start_sync(pw, who=""):
    if not pw or len(pw) < 6:
        return {"ok": False, "error": "密码不合法"}
    if auth_api is not None:
        user = getattr(auth_api, "DEFAULT_USER", "qingliao")
        try:
            if not auth_api.verify_password(user, pw):
                return {"ok": False, "error": "密码不正确"}
        except Exception as e:
            return {"ok": False, "error": "校验失败: %s" % str(e)[:80]}
    cdir = _compose_dir()
    if not cdir or not os.path.isfile(os.path.join(cdir, ".env")):
        return {"ok": False, "error": "本部署未暴露 compose 目录，无法自动同步（请手动跑 sync_password.py）"}

    with _lock:
        st = _load_state()
        if st.get("status") == "running" and \
                time.time() - float(st.get("started_at") or 0) < STALE_SECONDS:
            return {"ok": False, "error": "已有同步任务在进行中", "state": _normalize(st)}
        try:
            os.remove(RESULT_PATH)
        except FileNotFoundError:
            pass
        task = time.strftime("%Y%m%d%H%M%S")
        state = {"status": "running", "task": task, "started_at": time.time(),
                 "step": "正在改写宿主 .env / .ql_cred 并重建容器（约 15 秒）",
                 "who": who, "compose_dir": cdir}
        _save_state(state)

        sh = (_HELPER_SH.replace("__RES__", RESULT_PATH)
                       .replace("__CTR__", CONTAINER))
        cmd = ["docker", "run", "--rm", "-i",
               "-v", "%s:%s" % (cdir, cdir),
               "-v", "%s:%s" % (DATA_DIR, DATA_DIR),
               "-v", "%s:/hermes" % HERMES_DATA,
               "-v", "/var/run/docker.sock:/var/run/docker.sock",
               "-w", cdir, HELPER_IMAGE, "sh", "-c", sh]
        try:
            with open(LOG_PATH, "w", encoding="utf-8") as logf:
                logf.write("[%s] 启动 helper：%s\n" % (time.strftime("%F %T"), cdir))
                p = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                     stdout=logf, stderr=subprocess.STDOUT)
                p.stdin.write((pw + "\n").encode("utf-8"))
                p.stdin.flush()
                p.stdin.close()
        except Exception as e:
            state.update({"status": "failed", "error": str(e)[:200]})
            _save_state(state)
            return {"ok": False, "error": "启动 helper 失败: %s" % str(e)[:120]}
    return {"ok": True, "task": task, "status": "running",
            "hint": "同步中：约 15 秒，期间后端会短暂重启、App 会失联，请轮询本接口"}


class Handler(BaseHTTPRequestHandler):
    """POST /api/auth/sync-env  {"password": "..."}
       GET  /api/auth/sync-env   → 状态"""

    def _send(self, code, obj):
        try:
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except Exception:
            pass

    def do_OPTIONS(self):
        try:
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "X-Auth-Token, Content-Type")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.end_headers()
        except Exception:
            pass

    def do_GET(self):
        st = _normalize(_load_state())
        out = {"ok": True, "status": st.get("status", "idle"),
               "step": st.get("step", ""), "task": st.get("task", "")}
        if st.get("status") == "failed":
            out["error"] = st.get("error", "同步失败")
        out["log"] = _tail() if st.get("status") in ("failed", "running") else ""
        self._send(200, out)

    def do_POST(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
            req = json.loads(self.rfile.read(n) or b"{}") if n else {}
        except Exception:
            req = {}
        pw = (req.get("password") or "").strip()
        who = (self.headers.get("X-Forwarded-For") or self.client_address[0] or "")[:60]
        r = start_sync(pw, who)
        self._send(200 if r.get("ok") else 400, r)

    def log_message(self, format, *args):
        pass
