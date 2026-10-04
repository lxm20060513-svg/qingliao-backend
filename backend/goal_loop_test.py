# -*- coding: utf-8 -*-
# 跑法（容器内，数据重定向 /tmp，不碰生产）：
#   python3 /opt/data/scripts/ql.py nas put <本文件> 微信文件/轻聊web/backend
#   python3 /opt/data/scripts/ql.py nas exec "docker exec -w '/volume1/docker/hermes/微信文件/轻聊web/backend' -e QL_DATA_DIR=/tmp -e PYTHONPATH='/volume1/docker/hermes/微信文件/轻聊web/backend' qingliao python3 <文件名>"
"""v4.0.44 自测：报告回写 → 勾步骤 / 划待办 / 推通知 / 落时间线（数据全走 /tmp，不碰生产）"""
import json
import goal_module as gm
import inbox_api

gm.GOALS_PATH = '/tmp/gt.json'
gm.TODOS_PATH = '/tmp/tt.json'

sent = []


def fake_push(text, task_id=None, task_type="reply", want_id=False, session_id=None, goal_report=None):
    sent.append((task_type, text[:60], session_id))
    return (True, 'x')


inbox_api.push = fake_push

g = {'id': 'gtest1234', 'title': '测试目标',
     'steps': [{'id': 's1', 'title': '第一步', 'done': True},
               {'id': 's2', 'title': '第二步'},
               {'id': 's3', 'title': '第三步'}],
     'reports': [], 'cronJobIDs': ['JOBX'], 'originSessionId': ''}
json.dump([g], open('/tmp/gt.json', 'w'), ensure_ascii=False)
json.dump([{'content': '［目标·测试目标］第二步', 'done': False},
           {'content': '［目标·测试目标］第三步', 'done': False}],
          open('/tmp/tt.json', 'w'), ensure_ascii=False)

print('1) 按 cronJobID 回写:', gm.goals_report_from_cron(
    'JOBX', '今天推进：跑了第二步\n完成步骤：2\n选项：无'))
d = json.load(open('/tmp/gt.json'))[0]
print('2) 步骤状态:', [(s['title'], bool(s.get('done')), bool(s.get('startedAt'))) for s in d['steps']])
print('3) 待办划掉:', [(t['content'], bool(t.get('done'))) for t in json.load(open('/tmp/tt.json'))])
print('4) 推送:', sent)
print('5) 时间线:', [(r.get('src'), r.get('text')[:18]) for r in d['reports']])
print('6) 未知 job:', gm.goals_report_from_cron('NOPE', 'x'))
print('7) 解析器:', gm._parse_done_steps('完成步骤：2,3'), gm._parse_done_steps('完成步骤：无'),
      gm._parse_options('选项：要 | 不要'), gm._parse_options('选项：无'))
print('8) 作业文案:', gm._current_step_text(d))
