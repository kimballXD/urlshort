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
import pandas as pd

# step 1: get ourself some random urls
#ptt_hot=requests.get('https://www.ptt.cc/bbs/index.html', verify=False)
#ptt_hot.encoding='utf8'
#soup=bs(ptt_hot.text,'lxml')
#ptt_hot=['https://www.ptt.cc'+x.get('href') for x in soup.select('.board')]
#%%

ptt_hot=['https://www.ptt.cc/bbs/Gossiping/index.html',
 'https://www.ptt.cc/bbs/NBA/index.html',
 'https://www.ptt.cc/bbs/Baseball/index.html',
 'https://www.ptt.cc/bbs/Stock/index.html',
 'https://www.ptt.cc/bbs/LoL/index.html',
 'https://www.ptt.cc/bbs/C_Chat/index.html',
 'https://www.ptt.cc/bbs/sex/index.html',
 'https://www.ptt.cc/bbs/MobileComm/index.html',
 'https://www.ptt.cc/bbs/car/index.html',
 'https://www.ptt.cc/bbs/Japan_Travel/index.html',
 'https://www.ptt.cc/bbs/WomenTalk/index.html',
 'https://www.ptt.cc/bbs/movie/index.html',
 'https://www.ptt.cc/bbs/Tech_Job/index.html',
 'https://www.ptt.cc/bbs/Lifeismoney/index.html',
 'https://www.ptt.cc/bbs/BabyMother/index.html',
 'https://www.ptt.cc/bbs/e-shopping/index.html',
 'https://www.ptt.cc/bbs/joke/index.html',
 'https://www.ptt.cc/bbs/marvel/index.html',
 'https://www.ptt.cc/bbs/Beauty/index.html',
 'https://www.ptt.cc/bbs/Boy-Girl/index.html',
 'https://www.ptt.cc/bbs/Hearthstone/index.html',
 'https://www.ptt.cc/bbs/marriage/index.html',
 'https://www.ptt.cc/bbs/BuyTogether/index.html',
 'https://www.ptt.cc/bbs/ToS/index.html',
 'https://www.ptt.cc/bbs/MakeUp/index.html',
 'https://www.ptt.cc/bbs/Option/index.html',
 'https://www.ptt.cc/bbs/iOS/index.html',
 'https://www.ptt.cc/bbs/PlayStation/index.html',
 'https://www.ptt.cc/bbs/PC_Shopping/index.html',
 'https://www.ptt.cc/bbs/Elephants/index.html',
 'https://www.ptt.cc/bbs/TypeMoon/index.html',
 'https://www.ptt.cc/bbs/StupidClown/index.html',
 'https://www.ptt.cc/bbs/BeautySalon/index.html',
 'https://www.ptt.cc/bbs/KoreaStar/index.html',
 'https://www.ptt.cc/bbs/KR_Entertain/index.html',
 'https://www.ptt.cc/bbs/AllTogether/index.html',
 'https://www.ptt.cc/bbs/Kaohsiung/index.html',
 'https://www.ptt.cc/bbs/MLB/index.html',
 'https://www.ptt.cc/bbs/creditcard/index.html',
 'https://www.ptt.cc/bbs/Steam/index.html',
 'https://www.ptt.cc/bbs/home-sale/index.html',
 'https://www.ptt.cc/bbs/PokemonGO/index.html',
 'https://www.ptt.cc/bbs/SportLottery/index.html',
 'https://www.ptt.cc/bbs/KoreaDrama/index.html',
 'https://www.ptt.cc/bbs/Tainan/index.html',
 'https://www.ptt.cc/bbs/NBA_Film/index.html',
 'https://www.ptt.cc/bbs/Examination/index.html',
 'https://www.ptt.cc/bbs/Japandrama/index.html',
 'https://www.ptt.cc/bbs/TaichungBun/index.html',
 'https://www.ptt.cc/bbs/MuscleBeach/index.html',
 'https://www.ptt.cc/bbs/HardwareSale/index.html',
 'https://www.ptt.cc/bbs/RO/index.html',
 'https://www.ptt.cc/bbs/PublicServan/index.html',
 'https://www.ptt.cc/bbs/GetMarry/index.html',
 'https://www.ptt.cc/bbs/CFantasy/index.html',
 'https://www.ptt.cc/bbs/TWICE/index.html',
 'https://www.ptt.cc/bbs/Salary/index.html',
 'https://www.ptt.cc/bbs/ONE_PIECE/index.html',
 'https://www.ptt.cc/bbs/Palmar_Drama/index.html',
 'https://www.ptt.cc/bbs/NSwitch/index.html',
 'https://www.ptt.cc/bbs/WOW/index.html',
 'https://www.ptt.cc/bbs/Aviation/index.html',
 'https://www.ptt.cc/bbs/CVS/index.html',
 'https://www.ptt.cc/bbs/BTS/index.html',
 'https://www.ptt.cc/bbs/biker/index.html',
 'https://www.ptt.cc/bbs/japanavgirls/index.html',
 'https://www.ptt.cc/bbs/Hsinchu/index.html',
 'https://www.ptt.cc/bbs/Militarylife/index.html',
 'https://www.ptt.cc/bbs/mobilesales/index.html',
 'https://www.ptt.cc/bbs/part-time/index.html',
 'https://www.ptt.cc/bbs/HelpBuy/index.html',
 'https://www.ptt.cc/bbs/OverWatch/index.html',
 'https://www.ptt.cc/bbs/Food/index.html',
 'https://www.ptt.cc/bbs/KanColle/index.html',
 'https://www.ptt.cc/bbs/EAseries/index.html',
 'https://www.ptt.cc/bbs/cat/index.html',
 'https://www.ptt.cc/bbs/gay/index.html',
 'https://www.ptt.cc/bbs/PuzzleDragon/index.html',
 'https://www.ptt.cc/bbs/AC_In/index.html',
 'https://www.ptt.cc/bbs/Shadowverse/index.html',
 'https://www.ptt.cc/bbs/Headphone/index.html',
 'https://www.ptt.cc/bbs/Gamesale/index.html',
 'https://www.ptt.cc/bbs/FITNESS/index.html',
 'https://www.ptt.cc/bbs/TaiwanDrama/index.html',
 'https://www.ptt.cc/bbs/job/index.html',
 'https://www.ptt.cc/bbs/HatePolitics/index.html',
 'https://www.ptt.cc/bbs/CarShop/index.html',
 'https://www.ptt.cc/bbs/MacShop/index.html',
 'https://www.ptt.cc/bbs/lesbian/index.html',
 'https://www.ptt.cc/bbs/Soft_Job/index.html',
 'https://www.ptt.cc/bbs/Wanted/index.html',
 'https://www.ptt.cc/bbs/nb-shopping/index.html',
 'https://www.ptt.cc/bbs/KoreanPop/index.html',
 'https://www.ptt.cc/bbs/WannaOne/index.html',
 'https://www.ptt.cc/bbs/Lakers/index.html',
 'https://www.ptt.cc/bbs/RealmOfValor/index.html',
 'https://www.ptt.cc/bbs/DSLR/index.html',
 'https://www.ptt.cc/bbs/graduate/index.html',
 'https://www.ptt.cc/bbs/Tennis/index.html',
 'https://www.ptt.cc/bbs/NTU/index.html',
 'https://www.ptt.cc/bbs/Monkeys/index.html',
 'https://www.ptt.cc/bbs/Lions/index.html',
 'https://www.ptt.cc/bbs/BabyProducts/index.html',
 'https://www.ptt.cc/bbs/Celtics/index.html',
 'https://www.ptt.cc/bbs/cookclub/index.html',
 'https://www.ptt.cc/bbs/YuanChuang/index.html',
 'https://www.ptt.cc/bbs/TY_Research/index.html',
 'https://www.ptt.cc/bbs/LGBT_SEX/index.html',
 'https://www.ptt.cc/bbs/medstudent/index.html',
 'https://www.ptt.cc/bbs/Actuary/index.html',
 'https://www.ptt.cc/bbs/China-Drama/index.html',
 'https://www.ptt.cc/bbs/PUBG/index.html',
 'https://www.ptt.cc/bbs/DC_SALE/index.html',
 'https://www.ptt.cc/bbs/AKB48/index.html',
 'https://www.ptt.cc/bbs/GirlsFront/index.html',
 'https://www.ptt.cc/bbs/forsale/index.html',
 'https://www.ptt.cc/bbs/Korea_Travel/index.html',
 'https://www.ptt.cc/bbs/DMM_GAMES/index.html',
 'https://www.ptt.cc/bbs/GBF/index.html',
 'https://www.ptt.cc/bbs/BB_Online/index.html',
 'https://www.ptt.cc/bbs/facelift/index.html',
 'https://www.ptt.cc/bbs/Gov_owned/index.html',
 'https://www.ptt.cc/bbs/feminine_sex/index.html',
 'https://www.ptt.cc/bbs/Zastrology/index.html',
 'https://www.ptt.cc/bbs/Cavaliers/index.html',
 'https://www.ptt.cc/bbs/BB-Love/index.html',
 'https://www.ptt.cc/bbs/Bank_Service/index.html',
 'https://www.ptt.cc/bbs/basketballTW/index.html']



#load tester scheduling
repeat=[2]
res=[]
loggingPath=None
sleep_between_test=0

#job setting
use_batch=True
batch_size=40
pool=True

#throttle and backoff
sleep=60
backoff_hold=180
max_backoff=1
backoff_incre=10


for r in repeat:
    print 'start load test'
    load_tester=UrlShortener_load_tester(keyfiles='project_keys.txt')
    load_tester.do_jobs(worktype='convert', target_urls=ptt_hot, use_batch=use_batch, batch_size=batch_size, repeat=r, sleep=sleep, backoff_incre=backoff_incre, max_backoff=max_backoff,  pool=pool, backoff_hold=backoff_hold)
    res.append(load_tester)
    print ''
    load_tester.task_summary(last=5, logging=loggingPath, console_summary=True)
    success_count=load_tester.res_df.res.apply(lambda x:isinstance(x,unicode)).value_counts()
    print '\nsucess count: {}'.format(success_count)
    backoff=[x['backoff'] for x in load_tester.key_info.values()]
    if load_tester.terminate:
        print 'load test have trigger backoff limit! break load test at {} repeats'.format(r)
        break
    print 'please check the result to determine is it need to continue. Wait {} seconds'.format(sleep_between_test) 
    time.sleep(sleep_between_test)
