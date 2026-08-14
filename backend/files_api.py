#!/usr/bin/env python3
"""文件管理 API：/api/files/list?path=... /api/files/download?path=...
浏览 NAS 安全根目录内的文件，支持列表/下载/删除（限制在根目录内防穿越）
新增：/api/files/upload 上传到微信文件目录
密码保护：所有请求需携带 X-Files-Password header
"""
import http.server
import json
import os
DATA_DIR = os.environ.get("QL_DATA_DIR", "/data")
import upload_config_helper
import urllib.parse
import cgi
import time
import hmac

# 文件管理访问密码（修改这里即可更换密码）
FILES_PASSWORD = os.environ.get("QL_PASSWORD", "change-me")

# 安全根目录：轻聊数据目录（前端只允许浏览这里）
ROOT = DATA_DIR
# 额外允许浏览的目录（只读列表，不在此列表的根不可访问）
# 注意：根目录下避免放置敏感文件
ALLOWED_ROOTS = [ROOT]
# 上传目标目录
UPLOAD_DIR = os.environ.get("QL_UPLOAD_DIR", os.path.join(DATA_DIR, "uploads"))


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
        if not self._check_auth():
            self._send_json(401, {"error": "需要密码"})
            return
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        path_param = params.get('path', [''])[0]

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
                        data = f.read(200000)
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
                body = json.dumps({"text": text, "truncated": os.path.getsize(abs_path) > 200000}).encode()
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
        if parsed.path.startswith("/api/files/config"):
            body = self._read_json() or {}
            path = (body.get("upload_dir") or "").strip()
            ok, msg = upload_config_helper.set_dir(path)
            self._send_json(200, {"ok": ok, "message": msg, "upload_dir": upload_config_helper.get_dir()})
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
                body = json.dumps({"ok": True, "saved": saved_name, "size": len(data)}).encode()
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

        self.send_error(404)

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    server = http.server.HTTPServer(("0.0.0.0", 9129), FilesHandler)
    print("Files API listening on :9129")
    server.serve_forever()
