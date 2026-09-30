import urllib.request, json
url='http://172.21.0.2:9130/chat'
body=json.dumps({'text':'帮我查一下本机内存占用','user_id':'e2e-real','chat_id':'e2e-real'}).encode()
req=urllib.request.Request(url, data=body, method='POST', headers={'Content-Type':'application/json','Authorization':'Bearer qingliao-token-9130'})
try:
    r=urllib.request.urlopen(req, timeout=10)
    print('HTTP OK', r.status, r.read().decode()[:200])
except urllib.error.HTTPError as e:
    print('HTTP ERROR', e.code, e.read().decode()[:200])
except Exception as e:
    print('ERR', repr(e))
