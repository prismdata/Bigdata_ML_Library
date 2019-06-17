## parser.py
#-*- coding: utf-8 -*-
import requests

url = 'https://section.blog.naver.com/Search/Blog.nhn?pageNo=1&orderBy=sim&keyword=%EB%B0%B1%EC%88%98%EC%98%A4'
header = {'User-Agent' : 'Mozilla/5.0', 'referer' : 'http://naver.com'}

param = {
        'where' : 'post',
        'query' : 'mera'
        }

req = requests.get(url)
# html 소스 가져오기
html = req.text
print(html)

# http header
h = req.headers

# http status
status = req.status_code

# http 정상적으로 되었는지 ?
ok = req.ok

