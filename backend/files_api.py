#!/usr/bin/env python3
"""文件管理 API：/api/files/list?path=... /api/files/download?path=...
浏览 NAS 安全根目录内的文件，支持列表/下载/删除（限制在根目录内防穿越）
新增：/api/files/upload 上传到微信文件目录
密码保护：所有请求需携带 X-Files-Password header
"""
import http.server
import json
import os
import threading
import base64
import upload_config_helper
import urllib.parse
import cgi
import time
import hmac

# 文件管理访问密码（修改这里即可更换密码）
FILES_PASSWORD = os.environ.get("QL_FILES_PASSWORD", "")

# v3.0.54：蜂窝分片上传重组状态（内存态；staging 文件落 UPLOAD_DIR/.chunks/，成功后 rename 到 uploads/）
_chunk_lock = threading.Lock()
_chunks = {}   # uploadId -> {path, parts:set, total, ext, created}
CHUNK_MAX_TOTAL = 512          # 单片图像最多分片数（超大图保护）
CHUNK_MAX_SLICE = 2 * 1024 * 1024
CHUNK_MAX_B64 = 3 * 1024 * 1024
CHUNK_TTL = 120                # 未完成分片超过 120s 清理
_ALLOWED_IMG_EXT = ('jpg', 'jpeg', 'png', 'webp', 'gif', 'bmp', 'heic')

def _expire_chunks(now):
    """清理超时未完成的分片 staging，防目录/内存堆积"""
    stale = [u for u, s in _chunks.items() if now - s['created'] > CHUNK_TTL]
    for u in stale:
        s = _chunks.pop(u, None)
        if s:
            try:
                os.remove(s['path'])
            except OSError:
                pass

# 安全根目录：轻聊数据目录（前端只允许浏览这里）
ROOT = os.environ.get("QL_DATA_DIR", "/data")
# 额外允许浏览的目录（只读列表，不在此列表的根不可访问）
# 注意：不包含 hermes-data 根（其下有 config.yaml 等敏感文件）
ALLOWED_ROOTS = [ROOT]
# 上传目标目录
UPLOAD_DIR = os.environ.get("QL_UPLOAD_DIR", "/data/uploads")


def resolve_path(p):
    """把请求的相对路径解析为绝对路径，并做穿越防护（commonpath 严格校验）"""
    if not p:
        p = ''
    # 去掉可能的根标记
    p = p.lstrip('/')
    # 优先：上传目录（微信文件）下的文件 —— 聊天附件上传的位置
    up_abs = os.path.normpath(os.path.join(upload_config_helper.get_dir(), p))
    try:
        if os.path.commonpath([up_abs, upload_config_helper.get_dir()]) == upload_config_helper.get_dir():
            if os.path.exists(up_abs):
                return up_abs, upload_config_helper.get_dir()
    except ValueError:
        pass
    abs_path = os.path.normpath(os.path.join(ROOT, p))
    # commonpath 严格校验：必须在任一允许根内（防 startswith 前缀绕过）
    for r in ALLOWED_ROOTS:
        try:
            if os.path.commonpath([abs_path, r]) == r:
                return abs_path, r
        except ValueError:
            continue
    return None, None


# ─────────────────────────────────────────────────────────────────────────────
# v3.9.19：下载/预览门禁。原先 is_visible 只在 list_dir 生效 → download/preview
# 可直连取到数据目录下的隐藏文件（实测 data/.secrets_key 可被带 token 下载）。
# 沙箱根本来已限死（data/ + uploads/）且需要鉴权，故严重度为低；但补这一层零成本。
# ─────────────────────────────────────────────────────────────────────────────
_BLOCKED_NAMES = {'.env', '.env.local', '.secrets_key', 'auth.json', 'auth_tokens.json',
                  'secrets.json', '.inbox_token', '.nas_cred', 'credentials.json'}
_BLOCKED_SUFFIX = ('.key', '.pem', '.p12', '.pfx')
_BLOCKED_PREFIX = ('id_rsa', 'id_ed25519')


def is_downloadable(abs_path):
    """download/preview 前的门禁：隐藏文件、敏感文件名、密钥类后缀一律不放行。"""
    name = os.path.basename(abs_path or '')
    if not name or not is_visible(name):
        return False
    low = name.lower()
    if low in _BLOCKED_NAMES or low.endswith(_BLOCKED_SUFFIX) or low.startswith(_BLOCKED_PREFIX):
        return False
    return True


# 限额集中定义（原先散落各处，改之前先看这里）
MAX_PREVIEW_BYTES = 200000          # /api/files/preview 文本预览截断

def is_visible(name):
    """过滤隐藏文件/系统文件"""
    if name.startswith('.') or name.startswith('@'):
        return False
    return True


def list_dir(abs_path):
    entries = []
    try:
        for name in os.listdir(abs_path):
            if not is_visible(name):
                continue
            full = os.path.join(abs_path, name)
            try:
                st = os.stat(full)
                entries.append({
                    'name': name,
                    'is_dir': os.path.isdir(full),
                    'size': st.st_size if os.path.isfile(full) else 0,
                    'mtime': int(st.st_mtime),
                })
            except OSError:
                continue
    except OSError as e:
        return None, str(e)
    # 目录在前，文件在后，各自按名称排序
    entries.sort(key=lambda x: (not x['is_dir'], x['name'].lower()))
    return entries, None


def human_size(n):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if n < 1024 or unit == 'GB':
            return f"{n:.1f} {unit}" if unit != 'B' else f"{n} B"
        n /= 1024
    return f"{n} GB"


class FilesHandler(http.server.BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Files-Password, X-Auth-Token")

    def _check_auth(self):
        # v3.9.39 安全修复：撤掉 '/api/files/pin_' 免鉴权豁免（commit 5b84d57）。
        # 原 pin_read/pin_write 的 path 是绝对路径且不过 resolve_path 沙箱（只校验 .json 后缀），
        # 未鉴权即可读 auth_tokens.json 摘要表、再 pin_write 自铸摘要 = 全后端鉴权绕过。
        # App 侧 PinStore/MemoStore/TodoStore 均经 auth.json 带 X-Auth-Token，零客户端改动。
        import auth_api
        return auth_api.check_auth(self.headers, 'X-Files-Password', FILES_PASSWORD)

    def _send_json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        try:
            n = int(self.headers.get('Content-Length', 0))
            if n <= 0:
                return {}
            return json.loads(self.rfile.read(n).decode('utf-8'))
        except Exception:
            return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        # 密码校验
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        path_param = params.get('path', [''])[0]
        # v3.0.37：图片持久化 —— 上传目录内文件匿名可读（图片 URL 直接加载/分享，无需 token）
        # 仅对 download + 上传目录生效；list/config 仍走鉴权
        is_up = parsed.path.startswith('/api/files/download') and bool(path_param)
        up_abs = os.path.normpath(os.path.join(upload_config_helper.get_dir(), path_param.lstrip('/'))) if is_up else None
        if is_up and up_abs and os.path.commonpath([up_abs, upload_config_helper.get_dir()]) == upload_config_helper.get_dir() and os.path.isfile(up_abs):
            pass  # 上传目录文件匿名可读，跳过鉴权
        elif not self._check_auth():
            self._send_json(401, {"error": "需要密码"})
            return
        if parsed.path.startswith("/api/files/config"):
            self._send_json(200, {"ok": True, "upload_dir": upload_config_helper.get_dir()})
            return
        if parsed.path.startswith('/api/files/list'):
            abs_path, _ = resolve_path(path_param)
            if not abs_path:
                body = json.dumps({"error": "路径超出允许范围"}).encode()
                self.send_response(403)
                self._cors()
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            entries, err = list_dir(abs_path)
            if err:
                body = json.dumps({"error": err}).encode()
                self.send_response(500)
            else:
                # 当前目录相对路径（用于面包屑）
                rel = os.path.relpath(abs_path, ROOT)
                body = json.dumps({
                    "cwd": '' if rel == '.' else rel.replace(os.sep, '/'),
                    "entries": entries,
                    "dir_count": sum(1 for e in entries if e['is_dir']),
                    "file_count": sum(1 for e in entries if not e['is_dir']),
                }).encode()
                self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if parsed.path.startswith('/api/files/download'):
            abs_path, _ = resolve_path(path_param)
            if not abs_path or not os.path.isfile(abs_path):
                self.send_error(404)
                return
            # v3.9.19：敏感/隐藏文件不放行（原先只过滤了列目录）
            if not is_downloadable(abs_path):
                self._send_json(403, {"error": "该文件不允许下载"})
                return
            name = os.path.basename(abs_path)
            size = os.path.getsize(abs_path)
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Disposition", 'attachment; filename="' + urllib.parse.quote(name) + '"')
            self.send_header("Content-Length", str(size))
            self.end_headers()
            with open(abs_path, 'rb') as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    try:
                        self.wfile.write(chunk)
                    except (BrokenPipeError, ConnectionResetError):
                        break
            return

        if parsed.path.startswith('/api/files/preview'):
            abs_path, _ = resolve_path(path_param)
            if not abs_path or not os.path.isfile(abs_path):
                self.send_error(404)
                return
            # v3.9.19：敏感/隐藏文件不放行（与 download 同一门禁）
            if not is_downloadable(abs_path):
                self._send_json(403, {"error": "该文件不允许预览"})
                return
            ext = os.path.splitext(abs_path)[1].lower()
            # 图片直接返回（浏览器可预览），文本转码
            if ext in ('.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.svg', '.ico'):
                ctype = {
                    '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
                    '.gif': 'image/gif', '.webp': 'image/webp', '.bmp': 'image/bmp',
                    '.svg': 'image/svg+xml', '.ico': 'image/x-icon'
                }.get(ext, 'application/octet-stream')
                self.send_response(200)
                self._cors()
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(os.path.getsize(abs_path)))
                self.end_headers()
                with open(abs_path, 'rb') as f:
                    while True:
                        chunk = f.read(65536)
                        if not chunk:
                            break
                        try:
                            self.wfile.write(chunk)
                        except (BrokenPipeError, ConnectionResetError):
                            break
                return
            # 文本文件：读前 200KB 返回
            if ext in ('.txt', '.md', '.json', '.log', '.html', '.htm', '.css', '.js', '.py', '.yml', '.yaml', '.conf', '.ini', '.csv', '.xml'):
                try:
                    with open(abs_path, 'rb') as f:
                        data = f.read(MAX_PREVIEW_BYTES)
                    text = data.decode('utf-8', errors='replace')
                except OSError as e:
                    body = json.dumps({"error": str(e)}).encode()
                    self.send_response(500)
                    self._cors()
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                body = json.dumps({"text": text, "truncated": os.path.getsize(abs_path) > MAX_PREVIEW_BYTES}).encode()
                self.send_response(200)
                self._cors()
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            # 其他类型提示不支持预览
            body = json.dumps({"error": "该文件类型不支持预览，请下载查看"}).encode()
            self.send_response(415)
            self._cors()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return


        # v3.0.74: pin read (NAS file -> base64)
        if parsed.path.startswith('/api/files/pin_read'):
            pin_path = params.get('path', [''])[0]
            if not pin_path:
                self._send_json(400, {"error": "missing path"})
                return
            if not pin_path.endswith('.json'):
                self._send_json(403, {"error": "only .json allowed"})
                return
            try:
                with open(pin_path, 'rb') as f:
                    d = f.read()
                self._send_json(200, {"data": base64.b64encode(d).decode()})
            except FileNotFoundError:
                self._send_json(200, {"data": None})
            except OSError as e:
                self._send_json(500, {"error": str(e)[:150]})
            return
        self.send_error(404)

    def do_POST(self):
        # 密码校验
        if not self._check_auth():
            self._send_json(401, {"error": "需要密码"})
            return
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith('/api/files/delete'):
            body = self._read_json()
            rel = (body or {}).get('path') or ''
            abs_path, _root = resolve_path(rel)
            if not abs_path:
                self._send_json(403, {"error": "路径超出允许范围"})
                return
            if not os.path.exists(abs_path):
                self._send_json(404, {"error": "文件不存在"})
                return
            try:
                if os.path.isdir(abs_path):
                    import shutil
                    shutil.rmtree(abs_path)
                else:
                    os.remove(abs_path)
                self._send_json(200, {"ok": True})
            except OSError as e:
                self._send_json(500, {"error": "删除失败: " + str(e)[:150]})
            return
        if parsed.path.startswith('/api/files/rename'):
            body = self._read_json() or {}
            rel = body.get('path') or ''
            new_name = (body.get('new_name') or '').strip()
            abs_path, _root = resolve_path(rel)
            if not abs_path or not os.path.exists(abs_path):
                self._send_json(404, {"error": "文件不存在"})
                return
            if not new_name or '/' in new_name or new_name in ('.', '..'):
                self._send_json(400, {"error": "无效的新名称"})
                return
            new_abs = os.path.join(os.path.dirname(abs_path), new_name)
            if os.path.exists(new_abs):
                self._send_json(400, {"error": "同名文件已存在"})
                return
            try:
                os.rename(abs_path, new_abs)
                self._send_json(200, {"ok": True})
            except OSError as e:
                self._send_json(500, {"error": "重命名失败: " + str(e)[:150]})
            return
        if parsed.path.startswith('/api/files/mkdir'):
            body = self._read_json() or {}
            rel = body.get('path') or ''
            abs_path, _root = resolve_path(rel)
            if not abs_path:
                self._send_json(403, {"error": "路径超出允许范围"})
                return
            try:
                os.makedirs(abs_path, exist_ok=True)
                self._send_json(200, {"ok": True})
            except OSError as e:
                self._send_json(500, {"error": "创建目录失败: " + str(e)[:150]})
            return
        if parsed.path.startswith('/api/files/config'):
            body = self._read_json() or {}
            path = (body.get("upload_dir") or "").strip()
            ok, msg = upload_config_helper.set_dir(path)
            self._send_json(200, {"ok": ok, "message": msg, "upload_dir": upload_config_helper.get_dir()})
            return
        if parsed.path.startswith('/api/files/upload_chunk'):
            # v3.0.54：蜂窝分片上传 —— 客户端把图切成小块 base64 JSON POST（小 body 蜂窝可过），
            # 服务端按 offset(索引*片大小) 写 staging 文件，收齐后 rename 到 uploads/ 返回 url。
            body = self._read_json() or {}
            upid = (body.get('uploadId') or '').strip()
            try:
                index = int(body.get('index', -1))
                total = int(body.get('total', 0))
                slice_size = int(body.get('slice', 0))
            except Exception:
                self._send_json(400, {"error": "无效的分片参数"})
                return
            ext = (body.get('ext') or 'jpg').strip().lstrip('.')
            if ext not in _ALLOWED_IMG_EXT:
                ext = 'jpg'
            b64 = body.get('base64') or ''
            if not upid or index < 0 or total <= 0 or slice_size <= 0 or not b64:
                self._send_json(400, {"error": "分片参数缺失"})
                return
            if total > CHUNK_MAX_TOTAL or slice_size > CHUNK_MAX_SLICE or len(b64) > CHUNK_MAX_B64 \
                    or slice_size * total > 50 * 1024 * 1024:
                self._send_json(400, {"error": "分片过大"})
                return
            try:
                data = base64.b64decode(b64)
            except Exception:
                self._send_json(400, {"error": "base64 解码失败"})
                return

            up_dir = upload_config_helper.get_dir()
            chunk_dir = os.path.join(up_dir, '.chunks')
            try:
                os.makedirs(chunk_dir, exist_ok=True)
            except OSError as e:
                self._send_json(500, {"error": "无法创建分片目录: " + str(e)})
                return

            now = time.time()
            with _chunk_lock:
                _expire_chunks(now)
                st = _chunks.get(upid)
                if st is None:
                    st = {
                        'path': os.path.join(chunk_dir, 'up_' + upid + '.part'),
                        'parts': set(),
                        'total': total,
                        'ext': ext,
                        'created': now,
                    }
                    _chunks[upid] = st
                st['parts'].add(index)
                offset = index * slice_size
                try:
                    fd = os.open(st['path'], os.O_CREAT | os.O_RDWR, 0o644)
                    with os.fdopen(fd, 'r+b') as f:
                        f.seek(offset)
                        f.write(data)
                except OSError as e:
                    self._send_json(500, {"error": "分片写入失败: " + str(e)})
                    return

                if len(st['parts']) < st['total']:
                    # 未收齐：继续收下一片
                    self._send_json(200, {"ok": True, "received": len(st['parts']), "total": st['total']})
                    return

                # 收齐 → 组装落 uploads/
                _chunks.pop(upid, None)
                final_name = "up_{}_{}.{}".format(int(now), upid[:8], st['ext'])
                final_path = os.path.join(up_dir, final_name)
            # 锁外做 IO 收尾（rename/copy）
            try:
                os.replace(st['path'], final_path)
            except OSError:
                # 跨设备等异常：先 copy 再删 staging
                try:
                    import shutil
                    shutil.copy2(st['path'], final_path)
                    os.remove(st['path'])
                except OSError as e:
                    self._send_json(500, {"error": "文件组装失败: " + str(e)})
                    return
            size = os.path.getsize(final_path)
            url = "/api/files/download?path=" + urllib.parse.quote(final_name)
            self._send_json(200, {"ok": True, "saved": final_name, "size": size, "url": url})
            return
        if parsed.path.startswith('/api/files/upload'):
            # 确保上传目录存在
            try:
                os.makedirs(upload_config_helper.get_dir(), exist_ok=True)
            except OSError as e:
                body = json.dumps({"error": "无法创建上传目录: " + str(e)}).encode()
                self.send_response(500)
                self._cors()
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            # 解析 multipart/form-data
            ctype = self.headers.get('Content-Type', '')
            try:
                form = cgi.FieldStorage(
                    fp=self.rfile,
                    headers=self.headers,
                    environ={'REQUEST_METHOD': 'POST',
                             'CONTENT_TYPE': ctype,
                             'CONTENT_LENGTH': self.headers.get('Content-Length', '0')}
                )
            except Exception as e:
                body = json.dumps({"error": "解析上传失败: " + str(e)}).encode()
                self.send_response(400)
                self._cors()
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            file_item = form['file'] if 'file' in form else None
            if file_item is None or not file_item.filename:
                body = json.dumps({"error": "未收到文件"}).encode()
                self.send_response(400)
                self._cors()
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            # 清理文件名（只取 basename 防路径穿越）
            filename = os.path.basename(file_item.filename)
            if not filename:
                body = json.dumps({"error": "文件名无效"}).encode()
                self.send_response(400)
                self._cors()
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            dest = os.path.join(upload_config_helper.get_dir(), filename)
            # 同名文件加时间戳后缀
            if os.path.exists(dest):
                name, ext = os.path.splitext(filename)
                dest = os.path.join(upload_config_helper.get_dir(), f"{name}_{int(time.time())}{ext}")

            try:
                with open(dest, 'wb') as f:
                    # FieldStorage 已把内容读入临时文件
                    data = file_item.file.read()
                    f.write(data)
                saved_name = os.path.basename(dest)
                # v3.0.37：图片持久化 —— 返回相对路径 url（App 端拼各自 baseURL：WiFi/蜂窝中继均可用）
                body = json.dumps({"ok": True, "saved": saved_name, "size": len(data),
                                   "url": "/api/files/download?path=" + urllib.parse.quote(saved_name)}).encode()
                self.send_response(200)
            except OSError as e:
                body = json.dumps({"error": "保存失败: " + str(e)}).encode()
                self.send_response(500)
            self._cors()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return


        # v3.0.74: pin write (base64 -> NAS file)
        if parsed.path.startswith('/api/files/pin_write'):
            body = self._read_json()
            if not body or 'path' not in body or 'data' not in body:
                self._send_json(400, {"error": "missing path/data"})
                return
            pin_path = body['path']
            if not pin_path.endswith('.json'):
                self._send_json(403, {"error": "only .json allowed"})
                return
            try:
                os.makedirs(os.path.dirname(pin_path), exist_ok=True)
                d = base64.b64decode(body['data'])
                with open(pin_path, 'wb') as f:
                    f.write(d)
                self._send_json(200, {"ok": True, "size": len(d)})
            except Exception as e:
                self._send_json(500, {"error": str(e)[:150]})
            return
        self.send_error(404)

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    server = http.server.HTTPServer(("0.0.0.0", 9129), FilesHandler)
    print("Files API listening on :9129")
    server.serve_forever()
