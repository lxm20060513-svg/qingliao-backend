import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import stream_api
st={'sessionId':'e2e-sess-0905','agentEnabled':True,'messages':[{'role':'user','content':'帮我查一下本机内存占用'}]}
task={'state':st,'cancelled':False,'id':'e2e-t1'}
stream_api._forward_qingliao('e2e-t1', task, '帮我查一下本机内存占用')
print('E2E forward dispatched')
