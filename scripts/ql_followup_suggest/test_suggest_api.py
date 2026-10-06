import os
import sys
os.environ.setdefault("QL_DATA_DIR", "/opt/data/cache/scratch/suggest/data")
os.makedirs(os.environ["QL_DATA_DIR"], exist_ok=True)
import types
# 不导入真实 stream_api（它 import 时就 makedirs('/data') 需 root）；
# 打桩一个同名模块，只提供 build_questions 真正用到的两个符号。
_fake = types.ModuleType("stream_api")
_fake.AGENT_MODEL = "test-model"
_fake._chat_once = lambda body, url=None, key=None: {"choices": [{"message": {"content": "[]"}}]}
sys.modules["stream_api"] = _fake
stream_api = _fake

import json
sys.path.insert(0, "/opt/data/ql_backend_repo/backend")
import suggest_api as S

calls = []


def stub(text):
    def _c(body, url=None, key=None):
        calls.append(body)
        return {"choices": [{"message": {"content": text}}]}
    return _c


class FakeStream:
    AGENT_MODEL = "test-model"


def run(name, reply, expect, **kw):
    import stream_api
    stream_api._chat_once = stub(reply)
    got = S.build_questions(kw.get("u", "今晚吃什么"), kw.get("a", "推荐火锅"), kw.get("ex"))
    ok = got == expect
    print(("PASS " if ok else "FAIL ") + name + " -> " + json.dumps(got, ensure_ascii=False))
    if not ok:
        print("      期望 " + json.dumps(expect, ensure_ascii=False))
    return ok


allok = True
allok &= run("标准三条", '["附近有什么好吃的火锅店？","火锅店要预约吗？","附近有什么便利店？"]',
             ["附近有什么好吃的火锅店？", "火锅店要预约吗？", "附近有什么便利店？"])
allok &= run("带 markdown 围栏也能解", '```json\n["a是什么？","b怎么做？","c多少钱？"]\n```',
             ["a是什么？", "b怎么做？", "c多少钱？"])
allok &= run("dict 包裹 questions", '{"questions":["x怎么用？","y在哪？","z为何？"]}',
             ["x怎么用？", "y在哪？", "z为何？"])
allok &= run("只1条→宁缺勿滥返空", '["只有一个问题？"]', [])
allok &= run("非问句被剔除", '["火锅很好吃","火锅店在哪？","排队久吗？"]',
             ["火锅店在哪？", "排队久吗？"])
allok &= run("重复项去重", '["同一问题？","同一问题？","另一个？"]', ["同一问题？", "另一个？"])
allok &= run("与exclude交集丢弃", '["今晚吃什么？","火锅好吃吗？","要不要买菜？"]',
             ["火锅好吃吗？", "要不要买菜？"], ex=["今晚吃什么？"])
allok &= run("超长截断", '["' + "超" * 40 + '？","第二个问题？","第三个问题？"]',
             ["超" * 24 + "？", "第二个问题？", "第三个问题？"])
allok &= run("空回复→空", '', [])
allok &= run("非JSON垃圾→空", '抱歉我无法回答', [])
allok &= run("全被exclude→空", '["今晚吃什么？"]', [], ex=["今晚吃什么？"])


def boom(body, url=None, key=None):
    raise RuntimeError("upstream 402")


import stream_api
stream_api._chat_once = boom
r = S.build_questions("a", "b")
print(("PASS " if r == [] else "FAIL ") + "上游异常→空数组 -> " + json.dumps(r))
allok &= (r == [])

r = S.build_questions("", "")
print(("PASS " if r == [] else "FAIL ") + "双空输入→空数组")
allok &= (r == [])

# prompt 里 reasoning 关闭 + exclude 真的带进去了
stream_api._chat_once = stub('["q1？","q2？"]')
S.build_questions("u", "a", ["旧问题一", "旧问题二"])
b = calls[-1]
print("reasoning_enabled =", b["model_options"]["reasoning"]["enabled"])
print("prompt 含 exclude =", "旧问题一" in b["messages"][0]["content"])
print("RESULT", "PASS" if allok else "FAIL")
sys.exit(0 if allok else 1)