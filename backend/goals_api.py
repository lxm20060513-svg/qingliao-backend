#!/usr/bin/env python3
# v4.0.7 · 长期目标后端（轻聊 iOS「长期目标」栏目的服务端半边）
#
# 闭环：AI 在聊天里判定「我在筹备 XX」是长期目标 → 回一张「建目标卡」→ 用户点确认
#       → POST /api/life/goal → 落 goals.json + 建一个每天跑的 cron job（早推进 + 晚复盘）
#       → 两段汇报经 hermes cron 的 deliver 落到 App 任务中心 + 微信（投递不用我们管）
#       → cron 跑完调 POST /api/life/goal/report 回写 lastReport，卡片显示进度。
#
# 🚨 与 cron_api.py 同源的约定：
#   · HERMES_API 是 127.0.0.1:9123（容器内直连，绕开 9127 的 token 门）
#   · deliver 白名单校验，防存储型 XSS
#   · 密钥只从环境变量取，源码不落硬编码
#
# 数据落在 goals.json —— 与待办/备忘同一层 iOS 端文件通道（/api/files/pin_read|pin_write），
# 本文件不重复造 iOS 已经在用的落点，只负责「建目标时建 job」和「cron 回写汇报」。

import json, os, time, threading, urllib.request, urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone

HERMES_API = 'http://127.0.0.1:9123'
HERMES_KEY = os.environ.get("STREAM_HERMES_KEY") or os.environ.get("QL_AGENT_KEY") or ""
CRON_PASSWORD = os.environ.get("QL_PASSWORD", "")
DATA_DIR = os.environ.get("QL_LIFE_DIR", os.environ.get("QL_DATA_DIR", "/data"))
GOALS_FILE = os.path.join(DATA_DIR, "goals.json")

# 写锁：iOS 端也是 FIFO 串行写，服务端同样不能并发覆盖
_write_lock = threading.Lock()


def _iso_now():
    """goals.json 专用时间戳：UTC + 'Z' + 无小数秒 —— 与 goal_module._iso_now 同口径。

    iOS 端 SyncedStore 用 JSONDecoder .iso8601 解码，naive/带微秒的串会让**整个文件**
    解码失败并被 try? 吞掉 → 界面停在本地旧快照。详见 goal_module._iso_now 注释。"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Handler(BaseHTTPRequestHandler):
    def _check_auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, 'X-Cron-Password', CRON_PASSWORD)

    def send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ── goals.json 读写 ────────────────────────────────────
    def _read_goals(self):
        """返回 (goals_by_id, raw_list)。文件坏/空 → 空列表（不抛，前端当空态）。"""
        if not os.path.exists(GOALS_FILE):
            return {}
        try:
            with open(GOALS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not isinstance(data, list):
                return {}
            return {g.get('id'): g for g in data if isinstance(g, dict) and g.get('id')}
        except Exception:
            return {}

    def _write_goals(self, goals_by_id):
        items = sorted(goals_by_id.values(), key=lambda g: g.get('updatedAt', ''), reverse=True)
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = GOALS_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(items, f, ensure_ascii=False)
        os.replace(tmp, GOALS_FILE)   # 原子替换：iOS 端不会读到半个文件

    # ── hermes job 转发 ────────────────────────────────────
    def _hermes(self, method, path, payload=None, timeout=12):
        url = f"{HERMES_API}{path}"
        data = json.dumps(payload).encode('utf-8') if payload is not None else None
        req = urllib.request.Request(url, data=data, headers={
            'Authorization': f'Bearer {HERMES_KEY}',
            'Content-Type': 'application/json'
        }, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode('utf-8'))

    def _delete_job(self, job_id):
        if not job_id:
            return
        try:
            self._hermes('DELETE', f"/api/jobs/{job_id}")
        except Exception as e:
            print(f"[goals] delete job {job_id} failed: {e}")

    def _create_job(self, name, cron, prompt, deliver='weixin'):
        """deliver 白名单校验，防存储型 XSS（与 cron_api.py 同口径）。"""
        if deliver not in ('origin', 'weixin', 'local', 'all'):
            deliver = 'origin'
        payload = {
            'name': str(name)[:200],
            'prompt': str(prompt)[:4000],
            'schedule': str(cron)[:100],
            'enabled': True,
            'deliver': deliver
        }
        return self._hermes('POST', '/api/jobs', payload)

    # ── 早/晚两段 prompt 模板 ─────────────────────────────
    def _auto_split(self, title):
        """手工建目标时的兜底拆解：后端不调 LLM（省 token + 不卡 HTTP），
        套一个通用里程碑模板，真正的智能拆解交给 AI 在聊天里给（口径 1）。"""
        return [
            f"明确「{title}」的完成标准（做成什么样算完成）",
            "拆出关键里程碑与时间点",
            "推进第一个里程碑",
            "复盘并调整后续计划",
        ]

    def _morning_prompt(self, goal):
        steps = goal.get('steps') or []
        nxt = next((s for s in steps if not s.get('done')), None)
        done_n = sum(1 for s in steps if s.get('done'))
        step_lines = "\n".join(
            f"  {'[x]' if s.get('done') else '[ ]'} {s.get('title','')}" for s in steps
        ) or "  （AI 未拆步骤，请先拆 3~6 步）"
        nxt_txt = nxt.get('title') if nxt else "（全部完成，进入收尾或拆新目标）"
        return (
            f"你在为用户推进一个长期目标（每天固定时段提醒一次）。\n\n"
            f"目标：{goal.get('title','')}\n"
            f"当前进度：{done_n}/{len(steps)}\n"
            f"步骤清单：\n{step_lines}\n\n"
            f"今天要推进的一步：{nxt_txt}\n\n"
            f"请用中文输出今天的推进提醒，包含三部分：\n"
            f"1) 今天推进哪一步（具体到动作，别只说「继续努力」）\n"
            f"2) 需要用户本人做什么（最多 1 件事，一句话说清）\n"
            f"3) 一句鼓励/提醒（别灌鸡汤，务实）\n\n"
            f"控制 200 字以内。最后另起一段，严格按以下格式输出回写内容（供 App 卡片显示）：\n"
            f"##GOAL_REPORT##\n"
            f"今天推进：<一句话>\n"
            f"需要你做：<一句话>\n"
            f"##END##"
        )

    def _evening_prompt(self, goal):
        steps = goal.get('steps') or []
        done_n = sum(1 for s in steps if s.get('done'))
        step_lines = "\n".join(
            f"  {'[x]' if s.get('done') else '[ ]'} {s.get('title','')}" for s in steps
        ) or "  （未拆步骤）"
        return (
            f"你在为用户复盘一个长期目标的今天（每天固定时段复盘一次）。\n\n"
            f"目标：{goal.get('title','')}\n"
            f"当前进度：{done_n}/{len(steps)}\n"
            f"步骤清单：\n{step_lines}\n\n"
            f"请用中文输出今晚的复盘，包含：\n"
            f"1) 今天做了什么 / 推进了哪一步（没有就说没有，别编）\n"
            f"2) 还剩多少（{len(steps) - done_n} 步）\n"
            f"3) 明天计划推进哪一步\n\n"
            f"控制 200 字以内。最后另起一段，严格按以下格式输出回写内容：\n"
            f"##GOAL_REPORT##\n"
            f"今日：<一句话>\n"
            f"剩余：<已完>/<总数>\n"
            f"明日：<一句话>\n"
            f"##END##"
        )

    # ── 路由 ──────────────────────────────────────────────
    def do_GET(self):
        if not self._check_auth():
            self.send_json({'error': 'unauthorized'}, 401)
            return
        if self.path.startswith('/api/life/goal'):
            with _write_lock:
                self.send_json(sorted(self._read_goals().values(),
                                      key=lambda g: g.get('updatedAt', ''), reverse=True))
        else:
            self.send_error(404)

    def do_POST(self):
        if not self._check_auth():
            self.send_json({'error': 'unauthorized'}, 401)
            return
        length = int(self.headers.get('Content-Length', 0))
        try:
            data = json.loads(self.rfile.read(length).decode('utf-8'))
        except Exception:
            self.send_error(400)
            return

        if self.path == '/api/life/goal':
            self._create_goal(data)
        elif self.path == '/api/life/goal/report':
            self._write_report(data)
        else:
            self.send_error(404)

    def _create_goal(self, data):
        title = str(data.get('title', '')).strip()
        if not title:
            self.send_json({'error': 'title required'}, 400)
            return
        # 手工建目标往往只给标题（AI 建目标才带步骤）→ 这里补一次拆解，
        # 否则 cron 早/晚两段拿到的是「未拆步骤」，每天只能推空话。
        steps_in = data.get('steps') or []
        if not steps_in:
            steps_in = [{'id': str(uuid_hex()), 'title': t} for t in self._auto_split(title)]
        steps = []
        for s in steps_in[:12]:                     # 封顶 12 步，别让 AI 拆出 50 步
            st = str(s.get('title', '') if isinstance(s, dict) else s).strip()
            if not st:
                continue
            steps.append({
                'id': s.get('id') if isinstance(s, dict) and s.get('id') else str(uuid_hex()),
                'title': st,
                'todoLinked': bool(s.get('todoLinked')) if isinstance(s, dict) else False,
                'done': False, 'doneAt': None
            })
        morning_on = bool(data.get('morningEnabled', True))
        evening_on = bool(data.get('eveningEnabled', True))
        if not morning_on and not evening_on:
            morning_on = True                        # 至少留一段，否则建了目标永远不响（只开早间）
        mh = int(data.get('morningHour', 9))
        eh = int(data.get('eveningHour', 21))
        gid = data.get('id') or str(uuid_hex())
        now_iso = _iso_now()

        job_ids = []
        try:
            if morning_on:
                r = self._create_job(f"目标·早推进·{title}"[:60], f"{min(max(mh,0),23)} 9 * * *",
                                     self._morning_prompt({'title': title, 'steps': steps}))
                jid = (r.get('job') or r).get('id') if isinstance(r, dict) else None
                if jid:
                    job_ids.append(jid)
            if evening_on:
                r = self._create_job(f"目标·晚复盘·{title}"[:60], f"{min(max(eh,0),23)} 21 * * *",
                                     self._evening_prompt({'title': title, 'steps': steps}))
                jid = (r.get('job') or r).get('id') if isinstance(r, dict) else None
                if jid:
                    job_ids.append(jid)
        except Exception as e:
            # 建 job 失败不回滚目标：用户至少还能看到这条目标，本地可稍后重试
            print(f"[goals] create job failed for {gid}: {e}")

        goal = {
            'id': gid, 'title': title, 'steps': steps,
            'cronJobID': job_ids[0] if job_ids else '',
            'morningEnabled': morning_on, 'eveningEnabled': evening_on,
            'morningHour': min(max(mh, 0), 23), 'eveningHour': min(max(eh, 0), 23),
            'createdAt': now_iso, 'updatedAt': now_iso,
            'lastReport': '', 'lastPushedAt': None, 'paused': False
        }
        with _write_lock:
            goals = self._read_goals()
            goals[gid] = goal
            self._write_goals(goals)
        self.send_json(goal)

    def _write_report(self, data):
        gid = str(data.get('goalId', ''))
        report = str(data.get('report', '')).strip()
        if not gid or not report:
            self.send_json({'error': 'goalId and report required'}, 400)
            return
        with _write_lock:
            goals = self._read_goals()
            g = goals.get(gid)
            if not g:
                self.send_json({'error': 'goal not found'}, 404)
                return
            g['lastReport'] = report[:2000]
            g['lastPushedAt'] = _iso_now()
            g['updatedAt'] = g['lastPushedAt']
            # cron 报「今天推进了 X」→ 把那一步勾上（不猜：只认后端明确传来的 doneStepIds）
            for sid in (data.get('doneStepIds') or [])[:12]:
                for s in g.get('steps', []):
                    if s.get('id') == sid and not s.get('done'):
                        s['done'] = True
                        s['doneAt'] = g['lastPushedAt']
            self._write_goals(goals)
        self.send_json({'ok': True})

    def do_DELETE(self):
        if not self._check_auth():
            self.send_json({'error': 'unauthorized'}, 401)
            return
        # /api/life/goal?id=xxx
        gid = ''
        if '?' in self.path:
            from urllib.parse import parse_qs, urlparse
            gid = (parse_qs(urlparse(self.path).query).get('id') or [''])[0]
        if not gid:
            self.send_error(400)
            return
        with _write_lock:
            goals = self._read_goals()
            g = goals.pop(gid, None)
            if g:
                self._write_goals(goals)
        # 删目标必须把 cron job 一起删掉，否则明天还会推一个已经不存在的目标
        if g:
            self._delete_job(g.get('cronJobID'))
        self.send_json({'ok': True, 'deleted': bool(g)})


def uuid_hex():
    import uuid
    return uuid.uuid4().hex


if __name__ == '__main__':
    port = int(os.environ.get('GOALS_PORT', '9131'))
    print(f"[goals] listening on {port}, data={GOALS_FILE}", flush=True)
    HTTPServer(('0.0.0.0', port), Handler).serve_forever()
