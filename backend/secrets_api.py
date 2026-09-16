import json, os, uuid, threading
from cryptography.fernet import Fernet
from http.server import BaseHTTPRequestHandler, HTTPServer

# 凭据安全存储：Fernet 对称加密文件（密钥 600 权限，防 NAS 本地文件泄露）
STORE = os.environ.get('QL_SECRETS_STORE', '/data/secrets_store.json.enc')
KEY_FILE = os.environ.get('QL_SECRETS_KEY', '/data/.secrets_key')
ALLOWED_TYPES = ('nas', 'router', 'other')

_lock = threading.Lock()

# v3.0.6 security review：secrets 服务鉴权——复用轻聊统一 token 校验（防明文密码泄露）
def _authorized(h):
    """校验 X-Auth-Token（轻聊登录 token）。secrets 含明文密码，必须鉴权。"""
    try:
        import auth_api
        return auth_api.check_auth(h.headers, "X-Secrets-Password", "")
    except Exception:
        return False

def _load_key():
    if os.path.exists(KEY_FILE):
        return open(KEY_FILE).read().strip()
    key = Fernet.generate_key().decode()
    with open(KEY_FILE, 'w') as f:
        f.write(key)
    os.chmod(KEY_FILE, 0o600)
    return key

_fernet = Fernet(_load_key().encode())

def _load():
    if not os.path.exists(STORE):
        return []
    try:
        data = _fernet.decrypt(open(STORE, 'rb').read())
        items = json.loads(data)
        return items if isinstance(items, list) else []
    except Exception:
        return []

def _save(items):
    enc = _fernet.encrypt(json.dumps(items, ensure_ascii=False).encode('utf-8'))
    with open(STORE, 'wb') as f:
        f.write(enc)
    os.chmod(STORE, 0o600)

def find_router_cred():
    """供 router_api 等模块读取：优先取 type=router 且含密码的条目"""
    for it in _load():
        if it.get('type') == 'router' and it.get('password'):
            return it
    return None

class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def _send(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self._cors()
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _read_body(self):
        try:
            length = int(self.headers.get('Content-Length', 0))
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            return json.loads(raw.decode('utf-8'))
        except Exception:
            return {}

    def _public(self, it):
        # 列表视图：不含密码明文
        return {
            'id': it.get('id'),
            'name': it.get('name', ''),
            'type': it.get('type', 'other'),
            'address': it.get('address', ''),
            'username': it.get('username', ''),
            'has_password': bool(it.get('password')),
        }

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        # v3.0.6 security review：secrets 必须鉴权（含明文密码，防泄露）
        if not _authorized(self):
            self._send({'ok': False, 'error': 'unauthorized'}, 401)
            return
        path = self.path
        # 列表
        if path == '/api/secrets' or path.startswith('/api/secrets?'):
            with _lock:
                items = _load()
            self._send({'ok': True, 'secrets': [self._public(it) for it in items]})
            return
        # 单条明文（?reveal=true）
        if path.startswith('/api/secrets/'):
            rest = path[len('/api/secrets/'):]
            sid = rest.split('?')[0]
            reveal = 'reveal=true' in rest
            with _lock:
                items = _load()
            it = next((x for x in items if x.get('id') == sid), None)
            if not it:
                self._send({'ok': False, 'error': '条目不存在'}, 404)
                return
            if reveal:
                self._send({'ok': True, 'id': sid, 'password': it.get('password', ''),
                            'address': it.get('address', ''), 'username': it.get('username', '')})
            else:
                self._send({'ok': True, 'secret': self._public(it)})
            return
        self._send({'ok': False, 'error': 'not found'}, 404)

    def do_POST(self):
        # v3.0.6 security review：secrets 必须鉴权
        if not _authorized(self):
            self._send({'ok': False, 'error': 'unauthorized'}, 401)
            return
        if self.path != '/api/secrets':
            self._send({'ok': False, 'error': 'not found'}, 404)
            return
        body = self._read_body()
        name = str(body.get('name', '')).strip()
        stype = str(body.get('type', 'other')).strip()
        address = str(body.get('address', '')).strip()
        username = str(body.get('username', '')).strip()
        password = str(body.get('password', ''))
        sid = str(body.get('id', '') or '').strip()
        if not name or stype not in ALLOWED_TYPES:
            self._send({'ok': False, 'error': '名称必填，类型需为 nas/router/other'}, 400)
            return
        with _lock:
            items = _load()
            if sid:
                it = next((x for x in items if x.get('id') == sid), None)
                if not it:
                    self._send({'ok': False, 'error': '条目不存在'}, 404)
                    return
                it.update({'name': name, 'type': stype, 'address': address,
                           'username': username})
                if password:
                    it['password'] = password
            else:
                sid = uuid.uuid4().hex[:10]
                items.append({'id': sid, 'name': name, 'type': stype,
                              'address': address, 'username': username,
                              'password': password})
            _save(items)
        self._send({'ok': True, 'id': sid})

    def do_DELETE(self):
        # v3.0.6 security review：secrets 必须鉴权
        if not _authorized(self):
            self._send({'ok': False, 'error': 'unauthorized'}, 401)
            return
        if not self.path.startswith('/api/secrets/'):
            self._send({'ok': False, 'error': 'not found'}, 404)
            return
        sid = self.path[len('/api/secrets/'):].split('?')[0]
        with _lock:
            items = _load()
            new = [x for x in items if x.get('id') != sid]
            if len(new) == len(items):
                self._send({'ok': False, 'error': '条目不存在'}, 404)
                return
            _save(new)
        self._send({'ok': True, 'deleted': 1})

    def log_message(self, fmt, *args):
        pass


def run_server(port=9135):
    server = HTTPServer(('127.0.0.1', port), Handler)
    print(f"Secrets API on :{port}", flush=True)
    server.serve_forever()

if __name__ == '__main__':
    run_server()
