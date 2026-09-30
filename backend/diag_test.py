import urllib.request, json, socket
url='http://172.21.0.2:9130/chat'
try:
    s=socket.create_connection(('172.21.0.2',9130),timeout=5); s.close(); print('TCP 172.21.0.2:9130 OK')
except Exception as e: print('TCP ERR', repr(e))
body=json.dumps({'text':'ping','user_id':'diag','chat_id':'diag'}).encode()
req=urllib.request.Request(url, data=body, method='POST', headers={'Content-Type':'application/json','Authorization':'Bearer qingliao-token-9130'})
try:
    r=urllib.request.urlopen(req, timeout=10)
    print('HTTP OK', r.status, r.read().decode()[:200])
except urllib.error.HTTPError as e:
    print('HTTP ERROR', e.code, e.read().decode()[:200])
except Exception as e:
    print('ERR', repr(e))
