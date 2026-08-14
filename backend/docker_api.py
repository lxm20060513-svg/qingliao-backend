#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Docker 管理 API：docker compose 一键部署（看板 Docker 卡片用）

接口：
  GET  /api/docker/ps                      容器状态列表
  GET  /api/docker/compose?name=xxx        读取已部署的 compose 内容
  POST /api/docker/deploy  {name, yaml}    部署（建目录/写 yaml/compose up）
  POST /api/docker/down    {name}          停止并移除容器

安全：名称白名单（防路径穿越）、yaml 大小限制、部署日志返回
"""
import json
import os
import re
import subprocess
from http.server import BaseHTTPRequestHandler

DOCKER_ROOT = os.environ.get("QL_DOCKER_ROOT", "/data/docker")
NAME_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
MAX_YAML = 50000


def _clean_yaml(text):
    """清洗常见不可见字符：不换行空格(U+00A0)/全角空格(U+3000)/行尾CR → 普通空格
    （从网页/文档复制的 YAML 缩进常带 nbsp，YAML 解析失败，用户实测 line7 报错）"""
    return text.replace("\u00a0", " ").replace("\u3000", " ").replace("\r\n", "\n").replace("\r", "\n")


def _deploy(name, yaml_text):
    if not NAME_RE.match(name):
        return False, "名称不合法（仅字母/数字/下划线/中划线，1-64 字符）"
    if not yaml_text or not yaml_text.strip():
        return False, "YAML 内容为空"
    if len(yaml_text) > MAX_YAML:
        return False, "YAML 超过 50KB 限制"
    yaml_text = _clean_yaml(yaml_text)
    d = os.path.join(DOCKER_ROOT, name)
    try:
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "docker-compose.yml"), "w", encoding="utf-8") as f:
            f.write(yaml_text)
        # 语法校验（config 不实际启动）
        r = subprocess.run(["docker", "compose", "-p", name, "config", "-q"],
                           cwd=d, capture_output=True, text=True, timeout=30)
        if r.returncode != 0:
            return False, "compose 语法错误：\n" + (r.stdout + r.stderr)[-1200:]
        # 正式部署（-p 固定项目名 = 目录名，容器名 <name>_<service>_1）
        r = subprocess.run(["docker", "compose", "-p", name, "up", "-d"],
                           cwd=d, capture_output=True, text=True, timeout=180)
        if r.returncode == 0:
            return True, (r.stdout or "部署成功")[-800:]
        return False, (r.stdout + r.stderr)[-1500:]
    except subprocess.TimeoutExpired:
        return False, "部署超时（180s）"
    except Exception as e:
        return False, str(e)[:300]


def _ps():
    """全部容器（含已停止）；自定义 json 模板（排除 Labels 避免解析失败），compose 单独过滤查询"""
    try:
        p = subprocess.run(
            ["docker", "ps", "-a", "--format",
             '{"name":{{json .Names}},"status":{{json .Status}},"ports":{{json .Ports}}}'],
            capture_output=True, text=True, timeout=15)
        # compose 容器名集合（官方标签过滤）
        pc = subprocess.run(
            ["docker", "ps", "-a", "--filter", "label=com.docker.compose.project",
             "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=15)
        compose_names = set(x.strip() for x in pc.stdout.splitlines() if x.strip())
        if p.returncode != 0:
            return []
        out = []
        for line in p.stdout.strip().splitlines():
            try:
                d = json.loads(line)
            except Exception:
                continue
            n = d.get("name") or ""
            if not n:
                continue
            out.append({"name": n,
                        "status": d.get("status", ""),
                        "ports": d.get("ports", "") or "",
                        "is_compose": n in compose_names})
        return out
    except Exception:
        return []


def _read_compose(name):
    if not NAME_RE.match(name):
        return None
    p = os.path.join(DOCKER_ROOT, name, "docker-compose.yml")
    try:
        with open(p, encoding="utf-8") as f:
            return f.read()
    except OSError:
        return None


def _compose_action(name, action):
    """stop（停止） / start（启动） / down（删除容器，保留配置目录）"""
    if not NAME_RE.match(name):
        return False, "名称不合法"
    d = os.path.join(DOCKER_ROOT, name)
    if not os.path.isdir(d):
        return False, "未找到项目目录"
    cmd = ["docker", "compose", "-p", name] + ([action] if action in ("stop", "start") else ["down"])
    try:
        r = subprocess.run(cmd, cwd=d, capture_output=True, text=True, timeout=120)
        if r.returncode == 0:
            msg = {"stop": "已停止", "start": "已启动", "down": "已删除容器（配置保留，可重新部署）"}[action]
            return True, msg
        return False, (r.stdout + r.stderr)[-800:]
    except Exception as e:
        return False, str(e)[:200]


def _container_action(name, action):
    """通用容器操作（非 compose 项目，按容器名直接操作）"""
    try:
        cmd = {"stop": ["docker", "stop", name],
               "start": ["docker", "start", name],
               "rm": ["docker", "rm", "-f", name]}.get(action, ["docker", action, name])
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if r.returncode == 0:
            msg = {"stop": "已停止", "start": "已启动", "rm": "已删除容器"}.get(action, "操作成功")
            return True, msg
        return False, (r.stdout + r.stderr)[-500:]
    except Exception as e:
        return False, str(e)[:200]


def _resolve_action(name, action):
    """按项目目录决定：有 docker-compose.yml 的目录走 compose 命令，否则按容器名通用操作
    （修复：目录存在但无 compose 文件（UGOS 部署）时 compose 报 no config 失败，卡片点了没反应）"""
    d = os.path.join(DOCKER_ROOT, name)
    if (NAME_RE.match(name) and os.path.isdir(d)
            and os.path.exists(os.path.join(d, "docker-compose.yml"))):
        return _compose_action(name, action)
    return _container_action(name, action)


class DockerHandler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token, X-Docker-Password")

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
        return auth_api.check_auth(self.headers, "X-Docker-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def _read_json(self):
        try:
            n = int(self.headers.get("Content-Length", "0"))
            return json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            return {}

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
        params = urllib.parse.parse_qs(parsed.query)
        if parsed.path.startswith("/api/docker/ps"):
            self._send(200, {"ok": True, "containers": _ps()})
            return
        if parsed.path.startswith("/api/docker/updates"):
            self._send(200, {"ok": True, "updates": _updates()})
            return
        if parsed.path.startswith("/api/docker/images"):
            self._send(200, {"ok": True, "images": _images()})
            return
        if parsed.path.startswith("/api/docker/compose"):
            name = params.get("name", [""])[0]
            y = _read_compose(name)
            if y is None:
                self._send(404, {"error": "未找到该 compose 文件"})
            else:
                self._send(200, {"ok": True, "name": name, "yaml": y})
            return
        self._send(404, {"error": "Not Found"})

    def do_POST(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        body = self._read_json()
        if parsed.path.startswith("/api/docker/image/rm"):
            iid = (body.get("id") or "").strip()
            if not iid:
                self._send(200, {"ok": False, "message": "缺少镜像 ID"})
                return
            ok, msg = _image_rm(iid)
            self._send(200, {"ok": ok, "message": msg})
            return
        if parsed.path.startswith("/api/docker/check-update"):
            name = (body.get("name") or "").strip()
            if not name:
                self._send(200, {"ok": False, "message": "缺少容器名"})
                return
            ok, has, msg = _check_update(name)
            self._send(200, {"ok": ok, "has_update": has, "message": msg})
            return
        if parsed.path.startswith("/api/docker/upgrade"):
            name = (body.get("name") or "").strip()
            if not name:
                self._send(200, {"ok": False, "message": "缺少容器名"})
                return
            ok, msg = _upgrade(name)
            self._send(200, {"ok": ok, "message": msg})
            return
        if parsed.path.startswith("/api/docker/deploy"):
            name = (body.get("name") or "").strip()
            yaml_text = body.get("yaml") or ""
            ok, msg = _deploy(name, yaml_text)
            self._send(200, {"ok": ok, "message": msg, "containers": _ps()})
            return
        if parsed.path.startswith("/api/docker/down"):
            name = (body.get("name") or "").strip()
            ok, msg = _resolve_action(name, "down")
            self._send(200, {"ok": ok, "message": msg, "containers": _ps()})
            return
        if parsed.path.startswith("/api/docker/stop"):
            name = (body.get("name") or "").strip()
            ok, msg = _resolve_action(name, "stop")
            self._send(200, {"ok": ok, "message": msg, "containers": _ps()})
            return
        if parsed.path.startswith("/api/docker/start"):
            name = (body.get("name") or "").strip()
            ok, msg = _resolve_action(name, "start")
            self._send(200, {"ok": ok, "message": msg, "containers": _ps()})
            return
        if parsed.path.startswith("/api/docker/rm"):
            name = (body.get("name") or "").strip()
            ok, msg = _resolve_action(name, "rm")
            self._send(200, {"ok": ok, "message": msg, "containers": _ps()})
            return
        self._send(404, {"error": "Not Found"})

    def log_message(self, fmt, *args):
        pass


def _images():
    """镜像列表（含悬空 <none>）；in_use = 有【运行中】容器使用（{{json .}} 输出）"""
    try:
        p = subprocess.run(["docker", "ps", "--format", "{{.Image}}"],
                           capture_output=True, text=True, timeout=15)
        running_refs = set(x.strip() for x in p.stdout.splitlines() if x.strip())

        p2 = subprocess.run(["docker", "images", "--format", "{{json .}}"],
                            capture_output=True, text=True, timeout=15)
        pd = subprocess.run(["docker", "images", "--filter", "dangling=true", "--format", "{{json .}}"],
                            capture_output=True, text=True, timeout=15)
        out = []
        seen = set()
        for line in (p2.stdout + "\n" + pd.stdout).strip().splitlines():
            try:
                d = json.loads(line)
            except Exception:
                continue
            repo = d.get("Repository") or ""
            tag = d.get("Tag") or ""
            name = ("<none>:<none>" if not repo or repo == "<none>" else
                    "%s:%s" % (repo, tag))
            iid = d.get("ID") or ""
            if not iid or iid in seen:
                continue
            seen.add(iid)
            in_use = any(iid in ref or (ref.count(":") == 1 and ref.endswith(":" + tag))
                         for ref in running_refs)
            out.append({"name": name, "id": iid,
                        "size": d.get("Size", "") or "", "in_use": in_use})
        return out
    except Exception:
        return []


def _image_rm(image_id):
    """删除镜像（未被容器占用时成功）"""
    try:
        p = subprocess.run(["docker", "rmi", image_id],
                           capture_output=True, text=True, timeout=60)
        if p.returncode == 0:
            return True, "已删除"
        return False, p.stderr.strip()[-120:] or "删除失败"
    except Exception as e:
        return False, str(e)[:120]


def _image_digest(repo_tag):
    """当前本地镜像 digest（json 输出，规避 \t 分隔环境失效）"""
    try:
        p = subprocess.run(["docker", "images", "--digests", "--format", "{{json .}}"],
                           capture_output=True, text=True, timeout=15)
        for line in p.stdout.strip().splitlines():
            try:
                d = json.loads(line)
            except Exception:
                continue
            name = "%s:%s" % (d.get("Repository") or "", d.get("Tag") or "")
            if name == repo_tag:
                return d.get("Digest")
    except Exception:
        pass
    return None


def _container_image(name):
    """容器使用的镜像 repo:tag（无 tag 补 latest）"""
    try:
        p = subprocess.run(["docker", "inspect", name, "--format", "{{.Config.Image}}"],
                           capture_output=True, text=True, timeout=15)
        img = p.stdout.strip()
        if img and ":" not in img.split("/")[-1]:
            img += ":latest"
        return img
    except Exception:
        return None


def _check_update(name):
    """拉取最新镜像并对比 digest（已拉取，升级可直接重建）；返回 (ok, has_update, msg)"""
    img = _container_image(name)
    if not img:
        return False, False, "无法获取容器镜像"
    old = _image_digest(img)
    p = subprocess.run(["docker", "pull", img], capture_output=True, text=True, timeout=300)
    if p.returncode != 0:
        return False, False, (p.stderr.strip()[-100:] or "拉取失败")
    new = _image_digest(img)
    return True, (old is not None and new != old), ("" if (old and new != old) else "已是最新")


def _upgrade(name):
    """升级容器：compose 项目 = compose pull + up -d；非 compose 仅提示（重建需手动）"""
    d = os.path.join(DOCKER_ROOT, name)
    if NAME_RE.match(name) and os.path.isdir(d) and os.path.exists(os.path.join(d, "docker-compose.yml")):
        p = subprocess.run(["docker", "compose", "up", "-d", "--pull", "always"],
                           cwd=d, capture_output=True, text=True, timeout=600)
        if p.returncode == 0:
            return True, "升级完成，容器已重建"
        return False, p.stderr.strip()[-150:] or "升级失败"
    # 非 compose：镜像已拉新（check 时），但容器重建需原配置——提示手动
    return False, "非 compose 容器无法自动重建（UGOS 部署），请在原平台手动升级"


def _updates():
    """检测 compose 容器的镜像更新（并行查 registry digest，每容器 6s 超时；缓存 10 分钟）"""
    import time
    from concurrent.futures import ThreadPoolExecutor
    cache_file = "/tmp/docker_updates_cache.json"
    try:
        if os.path.exists(cache_file) and time.time() - os.path.getmtime(cache_file) < 600:
            with open(cache_file) as f:
                return json.load(f)
    except Exception:
        pass

    def check_one(c):
        try:
            name = c.get("name") or ""
            img = _container_image(name)
            if not img:
                return name, None
            local = _image_digest(img)
            remote = _registry_digest(img)
            if local and remote:
                return name, remote != local
        except Exception:
            pass
        return (c.get("name") or ""), None

    result = {}
    try:
        targets = [c for c in _ps() if c.get("is_compose")]
        with ThreadPoolExecutor(max_workers=6) as ex:
            for name, has in ex.map(check_one, targets):
                if has is not None:
                    result[name] = has
    except Exception:
        pass
    try:
        with open(cache_file, "w") as f:
            json.dump(result, f)
    except Exception:
        pass
    return result


def _registry_digest(img):
    """查询远程 registry 最新 digest（dockerCopilot 同款方案：HEAD + 多 Accept + TLS 跳过）"""
    import urllib.request
    import ssl
    repo, _, tag = img.rpartition(":")
    if not tag or "/" not in repo:
        tag = "latest"
        repo = img
    host = "registry-1.docker.io"
    path_repo = repo
    if "/" in repo and "." in repo.split("/")[0]:
        host = repo.split("/")[0]
        path_repo = repo.split("/", 1)[1]
    elif "/" not in repo:
        path_repo = "library/" + repo
    base = "https://%s/v2/%s/manifests/%s" % (host, path_repo, tag)
    headers = {
        "Accept": "application/vnd.docker.distribution.manifest.v2+json, "
                  "application/vnd.docker.distribution.manifest.list.v2+json, "
                  "application/vnd.oci.image.manifest.v1+json, "
                  "application/vnd.oci.image.index.v1+json"
    }
    ctx = ssl._create_unverified_context()
    try:
        if host == "registry-1.docker.io":
            req = urllib.request.Request(base, headers=headers, method="HEAD")
            try:
                urllib.request.urlopen(req, timeout=30, context=ctx)
            except urllib.error.HTTPError as e:
                if e.code == 401:
                    token = json.loads(e.read().decode() or "{}").get("token")
                    headers["Authorization"] = "Bearer " + token
        req = urllib.request.Request(base, headers=headers, method="HEAD")
        with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
            return r.headers.get("Docker-Content-Digest")
    except Exception:
        return None

