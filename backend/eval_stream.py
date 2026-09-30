import sys, time
sys.path.insert(0, '/volume1/docker/hermes/微信文件/轻聊web/backend')
import stream_api
task_id='eval-stream-1'
st={'sessionId':'eval-stream-sess','model':'deepseek-v4-flash','messages':[{'role':'user','content':'帮我查一下本机内存占用'}],'content':'','status':'streaming','agentEnabled':True,'provider':'','createdAt':time.time(),'updatedAt':time.time()}
task={'state':st,'cancelled':False,'id':task_id,'lock':None}
with stream_api._tasks_lock:
    stream_api._tasks[task_id]=task
stream_api._worker(task_id, task)
print('worker invoked')
for i in range(12):
    time.sleep(5)
    t=stream_api._tasks.get(task_id)
    if t:
        s=t['state']
        print(f"[{i*5+5}s] status={s.get('status')} len={len(s.get('content',''))} head={s.get('content','')[:60]!r}")
        if s.get('status')=='done':
            print('FULL:', s.get('content','')[:300])
            break
