# -*- coding: utf-8 -*-
# 跑法（容器内，数据重定向 /tmp，不碰生产）：
#   python3 /data/hermes/scripts/ql.py nas put <本文件> 微信文件/轻聊web/backend
#   python3 /data/hermes/scripts/ql.py nas exec "docker exec -w os.environ.get("QL_WEB_ROOT", "/data") + "/backend" -e QL_DATA_DIR=/tmp -e PYTHONPATH=os.environ.get("QL_WEB_ROOT", "/data") + "/backend" qingliao python3 <文件名>"
"""v4.0.44 审查修复自测（容器内跑，数据全走 /tmp，不碰生产）。

覆盖审查抓到的 5 条：
  ① push 返回序（want_id + goal_report 时不能退化成纯 mid）
  ② 一 job 多目标 → 全部回写（此前 break 只更新第一个）
  ③ 联动失败必须如实报 warn（此前静默 ok=True）
  ④ 问题卡答完立刻收尾（此前永久 pending → 每 60s 重投同一张卡）
  ⑤ 问题卡 task_id 稳定（此前带秒级时间戳 → 重推变 id，App 去重失效）
"""
import json
import goal_module as gm
import inbox_api

gm.GOALS_PATH = '/tmp/g3_goals.json'
gm.TODOS_PATH = '/tmp/g3_todos.json'
inbox_api.QUEUE_FILE = '/tmp/g3_queue.json'
inbox_api.ARCHIVE_DIR = '/tmp/g3_archive'

FAIL = []


def ck(desc, cond):
    print(("  OK  " if cond else "  XX  ") + desc)
    if not cond:
        FAIL.append(desc)


def seed(goals, todos=()):
    json.dump(goals, open('/tmp/g3_goals.json', 'w'), ensure_ascii=False)
    json.dump(list(todos), open('/tmp/g3_todos.json', 'w'), ensure_ascii=False)
    json.dump([], open('/tmp/g3_queue.json', 'w'), ensure_ascii=False)


def goal(gid, job, title='测试目标', steps=None):
    return {'id': gid, 'title': title, 'cronJobIDs': [job], 'reports': [],
            'originSessionId': '', 'steps': steps or [
                {'id': 's1', 'title': '第一步', 'done': True},
                {'id': 's2', 'title': '第二步'},
                {'id': 's3', 'title': '第三步'}]}


print('(1) push 返回序：want_id=True + goal_report')
seed([goal('gA1', 'JOBX')])
ok, msg = inbox_api.push('回写测试', task_id='t-order', task_type='question', want_id=True,
                         goal_report={'jobId': 'JOBX', 'report': '今天推进：跑了第二步\n完成步骤：2',
                                      'doneSteps': [2]})
ck('push 返回 (True, dict) 而不是纯 mid', ok is True and isinstance(msg, dict))
ck('dict 带 goal_ok=True', bool(msg.get('goal_ok')) is True)
ck('dict 同时带 mid（want_id 语义不丢）', bool(msg.get('mid')))
ck('回写真落盘（reports 有 cron 来源）',
   any(r.get('src') == 'cron' for r in json.load(open('/tmp/g3_goals.json'))[0]['reports']))

print('(2) 一个 job 挂多个目标 → 全部回写')
seed([goal('gB1', 'JOBY', title='目标甲'), goal('gB2', 'JOBY', title='目标乙')])
code, body = gm.goals_report_from_cron('JOBY', '今天推进：都跑了\n完成步骤：2')
ck('返回 updated=2', body.get('updated') == 2)
gs = json.load(open('/tmp/g3_goals.json'))
ck('两个目标都写了 reports', all(g['reports'] for g in gs))

print('(3) 联动失败如实报 warn')
seed([goal('gC1', 'JOBZ', title='目标丙', steps=[
    {'id': 's1', 'title': '第一步'}, {'id': 's2', 'title': '第二步'}])], todos=[])
real_push = inbox_api.push


def boom(*a, **kw):
    raise RuntimeError('模拟推送炸')


inbox_api.push = boom
try:
    code, body = gm.goals_report_from_cron('JOBZ', '今天推进：跑了第一步\n完成步骤：1')
finally:
    inbox_api.push = real_push
ck('body 带 warn（不再静默 ok=True）', bool(body.get('warn')))
ck('warn 指出通知没发出', any('通知' in w for w in (body.get('warn') or [])))

print('(4) 问题卡答完立刻收尾 + (5) task_id 稳定')
seed([goal('gD1', 'JOBD', title='目标丁')])
seen = []


def spy(text, task_id=None, **kw):
    seen.append(task_id)
    return real_push(text, task_id=task_id, **kw)


inbox_api.push = spy
saved = gm._WATCH_MAX
gm._WATCH_MAX = 0            # 先不起 watcher 线程（避免与下面同步调用抢答案）
try:
    g0 = json.load(open('/tmp/g3_goals.json'))[0]
    mid1 = gm._ask_user(g0, '这步要不要现在做？', ['继续推进', '按计划'])
    mid2 = gm._ask_user(g0, '这步要不要现在做？', ['继续推进', '按计划'])
finally:
    gm._WATCH_MAX = saved
    inbox_api.push = real_push
ck('两次推卡 task_id 完全相同（稳定）', bool(seen) and seen[0] == seen[1])
ck('task_id 形如 goalask-<gid8>-<当前步序号2>', seen and seen[0] == 'goalask-gD1-2')
ck('拿到 mid', bool(mid1) and mid1 == mid2)
q = json.load(open('/tmp/g3_queue.json'))
ck('卡在队列里且 task_type=question', len(q) == 1 and q[0].get('task_type') == 'question')
ck('卡带 session_id 归属', 'session_id' in q[0])
inbox_api.answer_question(mid1, '我知道了，按计划来')
gm._WATCHERS.add(mid1)
gm._watch_answer(mid1, 'gD1', '')          # 同步跑（不走线程）
q2 = json.load(open('/tmp/g3_queue.json'))
ck('答完卡已从队列移除（mark_done 收尾）', not [x for x in q2 if x.get('id') == mid1])
ck('答案写进目标时间线', any(r.get('kind') == 'answer' for r in
                        json.load(open('/tmp/g3_goals.json'))[0]['reports']))
ck('watcher 名额已腾出（finally discard）', mid1 not in gm._WATCHERS)
ck('答案卡已进幂等集', mid1 in gm._ANSWER_DONE)

print('(6) 边界：非目标 job 仍 skipped（行为不变）')
seed([goal('gE1', 'JOBE')])
code, body = gm.goals_report_from_cron('NOPE', '随便一句')
ck('skipped=True 且 ok=True', body.get('skipped') is True and body.get('ok') is True)

print()
print('审查修复自测：%d 通过 / %d 失败' % (21 - len(FAIL), len(FAIL)))
raise SystemExit(1 if FAIL else 0)
