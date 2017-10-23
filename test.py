# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 16:32:54 2017

@author: KimballWu
"""

import requests
from bs4 import BeautifulSoup as bs
import os
os.chdir('d:/coding/urlshort')
import time
from urlshort import *

# step 1: get ourself some random urls
ptt_hot=requests.get('https://www.ptt.cc/bbs/index.html', verify=False)
ptt_hot.encoding='utf8'
soup=bs(ptt_hot.text,'lxml')
ptt_hot=['https://www.ptt.cc'+x.get('href') for x in soup.select('.board')]


#load test
repeat=[10]
use_batch=True
b=10
s=12
for r in repeat:
    load_tester=UrlShortener_load_tester(keyfiles='project_keys.txt')
    load_tester.do_jobs(worktype='convert', target_urls=ptt_hot, use_batch=use_batch, batch_size=b, repeat=r, sleep=s)
    load_tester.task_summary(last=5, logging='load_test_unit.csv', console_summary=True)
    print 'please check the result to determine is it need to continue. Wait 3 minutes.......'
    time.sleep(180)