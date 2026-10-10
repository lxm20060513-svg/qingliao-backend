# -*- coding: utf-8 -*-
# 跑法（容器内，数据重定向 /tmp，不碰生产）：
#   python3 /data/hermes/scripts/ql.py nas put <本文件> 微信文件/轻聊web/backend
#   python3 /data/hermes/scripts/ql.py nas exec "docker exec -w os.environ.get("QL_WEB_ROOT", "/data") + "/backend" -e QL_DATA_DIR=/tmp -e PYTHONPATH=os.environ.get("QL_WEB_ROOT", "/data") + "/backend" qingliao python3 <文件名>"
"""HTTP 端到端自测：POST /api/inbox/push 带 goal_report（真路由，token 走环境变量，不落盘）

目的：证明运行中的容器里「桥 → inbox 路由 → goal_module 回写」这条线是通的，
响应里如实带回 goal_ok；测完把这条自测消息就地 mark_done 清掉。
"""
import json
import os
import urllib.request

import inbox_api

tok = os.environ.get('QL_TOK', '')
payload = {'text': '🧪 自测：长期目标回写链路（可忽略）', 'source_task_id': 'zd-selftest',
           'task_type': 'system', 'want_id': True,
           'goal_report': {'jobId': 'NOPE-JOB', 'report': '自测：非目标 job', 'doneSteps': []}}
req = urllib.request.Request('http://127.0.0.1:9127/api/inbox/push',
                             data=json.dumps(payload).encode(), method='POST')
req.add_header('Content-Type', 'application/json')
req.add_header('X-Inbox-Token', tok)
try:
    r = json.loads(urllib.request.urlopen(req, timeout=10).read() or b'{}')
except Exception as e:
    r = {'error': str(e)[:160]}
print('HTTP 带 goal_report 响应:', r)
mid = r.get('id') or ''
print('自测消息清理 mark_done:', inbox_api.mark_done(mid) if mid else '无 id（未入队）')

# 反向：不带 goal_report 的老调用方，响应体不应出现 goal_ok（老行为零变化）
payload2 = {'text': '🧪 自测：老调用方（可忽略）', 'source_task_id': 'zd-selftest2',
            'task_type': 'system', 'want_id': True}
req2 = urllib.request.Request('http://127.0.0.1:9127/api/inbox/push',
                              data=json.dumps(payload2).encode(), method='POST')
req2.add_header('Content-Type', 'application/json')
req2.add_header('X-Inbox-Token', tok)
r2 = json.loads(urllib.request.urlopen(req2, timeout=10).read() or b'{}')
print('HTTP 老调用方响应（不含 goal_ok 即正确）:', r2)
mid2 = r2.get('id') or ''
print('清理:', inbox_api.mark_done(mid2) if mid2 else '无 id')
