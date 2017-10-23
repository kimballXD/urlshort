# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 14:41:24 2017
    Google Url Shortener API, batch request Example
    Preparing Steps:
        1. Go to https://console.developers.google.com, and
        2. create an Application
        3. go to "library" panel, activate urlShortener API for your application
        4. from "crendentials" panel, generate an API key
        6. from "crendentials" panel, generate an Oauth Client ID
        7. download Oauth Client ID as a json file named "client_secret.json", save it to the local folder.
        5. fill in your API key in the 'apikey' variable
        6. install Google API python client library
            -- pip install --upgrade google-api-python-client
        7. make sure other 3rd-party library are installed
        8. run (NEEDS human intervention if you don't have an Oauth crendential file saved in the local folder beforehanded)!
    Reference:
        1. url shortener api, https://goo.gl/TZ4vHy
        2. url shortener guide, Google API python client library. https://goo.gl/tgz8xD
        3. how to send batch request using Google API. https://goo.gl/tsqFZe
        4. official example of url shortener api. https://goo.gl/NsoJ7R
            
@author: KimballWu@taiwanmobile.com
"""

import multiprocessing.dummy as mt
import httplib2
from googleapiclient import discovery
from oauth2client.file import Storage
import time
import datetime
import math
import pandas as pd
import os
#%%

class UrlShortener(object):    
    def __init__(self, keyfiles):
        with open(keyfiles,'rb') as infile:
            project_keys=infile.readlines()
            project_keys=[x.strip().split(',') for x in project_keys]
        self.keyfile=project_keys
        self.KEYS=len(project_keys)
        self.key_credentials=dict.fromkeys([x[0] for x in self.keyfile])          
        self.res=list()
        self.setting={'worktype':None, 'use_batch':None, 'batch_size':None}
        self.joblist=list()
        self.setting=dict()
        self.job_timer=list()
        self.excpetion=0

    def _get_http_con(self):
        return httplib2.Http(disable_ssl_certificate_validation=True, timeout=10000)

    def _get_url_generator(self, target_urls):
        if self.setting['use_batch']: 
            batch_size=self.setting['batch_size']
            partition_num=int(math.ceil(float(len(target_urls))/batch_size))
            partitioner=((i,target_urls[batch_size*i : batch_size*(1+i)]) for i in range(partition_num))
            return partitioner
        else:
            return enumerate(target_urls)

    def _get_submit_worker(self, apikey, assess_token_path):
        if not self.key_credentials.get(apikey):
            credentials=Storage(assess_token_path).get()
            credentials.refresh(self._get_http_con())
            Storage(assess_token_path).put(credentials)
            self.key_credentials[apikey]=credentials
        else:
            credentials=self.key_credentials[apikey]
        authorized_http_con=credentials.authorize(self._get_http_con())
        service=discovery.build('urlshortener', 'v1', developerKey=apikey, http=authorized_http_con)
        if self.setting['use_batch']: 
            return service, service.new_batch_http_request()
        else:
            return service
            
    def _gen_unit_job(self, url, service):
        if self.setting['worktype']=='convert':         
            req=service.url().insert(body={'longUrl':url}, fields='id')
        elif self.setting['worktype']=='analyze':
            req=service.url().get(shortUrl=url, projection='ANALYTICS_CLICKS', fields='analytics(allTime/shortUrlClicks)')
        return req     

    def _get_unit_job_list(self, target_urls):
        res=[]
        for idx, url in self._get_url_generator(target_urls):
            keyID, apikey, assess_token_path=self.keyfile[idx % self.KEYS]           
            service =self._get_submit_worker(apikey, assess_token_path)
            req=self._gen_unit_job(url, service)
            res.append((idx, keyID, url, req))
        self.joblist=res
        return res         

    def _gen_batch_job(self, idx, keyID, batch_urls, service, batch_worker):
        if self.setting['worktype']=='convert':         
            for url in batch_urls:
                req=service.url().insert(body={'longUrl':url}, fields='id')
                batch_worker.add(req, request_id='{};{};{}'.format(idx, keyID, url), callback=self._append_convert_res)
        elif self.setting['worktype']=='analyze':
            for url in batch_urls:
                req=service.url().get(shortUrl=url, projection='ANALYTICS_CLICKS', fields='analytics(allTime/shortUrlClicks)')
                batch_worker.add(req, request_id='{};{};{}'.format(idx, keyID, url), callback=self._append_analyze_res)
        return batch_worker

    def _get_batch_job_list(self, target_urls):       
        res=[]
        for idx, batch_urls in self._get_url_generator(target_urls):
            keyID, apikey, assess_token_path=self.keyfile[idx % self.KEYS]           
            service, batch_worker=self._get_submit_worker(apikey, assess_token_path)
            batch_jobs= self._gen_batch_job(idx, keyID, batch_urls, service, batch_worker)
            res.append((idx, batch_jobs))
        self.joblist=res
        return res
 
    def _append_analyze_res(self, req_id, req, exception):
        batch, keyID, shortUrl= req_id.split(';')
        batch, keyID=int(batch), int(keyID)
        if exception:
            self.res.append((batch, keyID, shortUrl, exception))        
        else:
            self.res.append((batch, keyID, shortUrl, req['analytics']['allTime']['shortUrlClicks']))
            
    def _append_convert_res(self, req_id, req, exception): 
        '''callback function for each urlShorten api call'''
        batch, keyID, longUrl= req_id.split(';')
        batch, keyID=int(batch), int(keyID)
        if exception:
            self.res.append((batch, keyID, longUrl, exception))
            return 1
        else:
            self.res.append((batch, keyID, longUrl, req['id']))
            return 0
      
    def _do_batch_job(self, batch_job, sleep, retry=0):
        batch_id, batch_req=batch_job

        try:
            batch_req.execute()
        except:
            batch_req.execute()
        finally:
            print 'batch {} call finished! sleep for {} seconds'.format(batch_id, sleep)
            time.sleep(sleep)
            self.job_timer.append((batch_id, datetime.datetime.now()))
            return 

    def _do_unit_job(self, job, sleep, retry=0):
        job_id, keyID, url, req = job
        exc=False
        try:
            rep=req.execute()
            if self.setting['worktype']=='convert':
                self.res.append((job_id, keyID, url, rep['id']))
            elif self.setting['worktype']=='analyze':
                self.res.append((job_id, keyID, url, rep['analytics']['allTime']['shortUrlClicks']))
        except Exception as e:
            if retry<1:
                print 'give a another shot!'
                exc=self._do_unit_job(job, sleep, retry=1)
            else:
                exc=True
        finally:
            if not exc and retry==0:
                print 'job call {}/{} finished! sleep for {} seconds.'.format(job_id+1, len(self.joblist), sleep)
                self.job_timer.append((job_id, datetime.datetime.now()))
            elif exc and retry>0:
                print 'job call {}/{} failed! Store exception'.format(job_id+1, len(self.joblist))
                self.job_timer.append((job_id, datetime.datetime.now()))
                self.res.append((job_id, keyID, url, e))
            time.sleep(sleep)
            return exc
    
    def _excpetion_monitor(self, rep):
        print rep
    
    def _main(self, target_urls, sleep):
        pool=mt.Pool(self.KEYS)
        if self.setting['use_batch']:
            self.job_timer.append((-1, datetime.datetime.now())) #start processing
            joblist=self._get_batch_job_list(target_urls)
            self.job_timer.append((-2, datetime.datetime.now())) #all http conneciotn authorization finished
            for job in joblist:
                pool.apply_async(self._do_batch_job, args=(job, sleep,), callback=self._excpetion_monitor)      

        else:
            joblist=self._get_unit_job_list(target_urls)
            self.job_timer.append((-1, datetime.datetime.now()))
            for job in joblist:
                pool.apply_async(self._do_unit_job, args=(job, sleep,))      
                
        #finallize
        pool.close()
        pool.join()          
      
    def do_jobs(self, worktype, target_urls, use_batch=False, batch_size=1, sleep=1):
        #setting
        self.setting={'worktype':worktype, 'use_batch':use_batch, 'batch_size':batch_size}
        self.res=list()
        #do main jobs
        self._main(target_urls, sleep)
    
    def dump_res(self, res_path):
        try:
            import pandas as pd
            res=pd.DataFrame(res, columns=['job_id','key_id','url','res'])
            res.to_excel(res_path,index=False, encoding='utf8')
        except ImportError as e:
            print '[WARNING] there is no pandas module to import, fall back to CSV file.'
            import csv
            with open(os.path.splitext(res_path)[0]+'.csv','wb') as out:
                        csv.writer
        
     

#%%

class UrlShortener_load_tester(UrlShortener): 
    def __init__(self, keyfiles):
        super(UrlShortener_load_tester, self).__init__(keyfiles)
        self.setting={'worktype':None, 'use_batch':None, 'batch_size':None, 'repeat':None}
        self.res_df=None
        self.summary=None
       
    def _get_url_generator(self, target_urls):
        if self.setting['repeat']==-1:
            return super(UrlShortener_load_tester, self)._get_url_generator(target_urls)
        else:           
            ## hack batc_size veriable for unit jobs
            if self.setting['use_batch']:
                batch_size=self.setting['batch_size']  
            else:
                batch_size=1
            
            #caculate
            repeat=self.setting['repeat']
            url_num=batch_size * repeat * self.KEYS
            mul=math.ceil(float(url_num)/len(target_urls))
            temp=target_urls * int(mul)
            temp=temp[:url_num]
           
            #res
            if self.setting['use_batch']:            
                partition_num=len(temp)/batch_size
                return ((i,temp[batch_size*i :batch_size*(1+i)]) for i in range(partition_num))
            else:
                return enumerate(temp)    
    
    def do_jobs(self, worktype, target_urls, use_batch=False, batch_size=1, sleep=1, repeat=-1):
        use_batch=False if batch_size==1 else use_batch
        self.setting={'worktype':worktype, 'use_batch':use_batch, 'batch_size':batch_size,'sleep':sleep,'repeat':repeat,}
        self.res=list()
        super(UrlShortener_load_tester, self)._main(target_urls, sleep)
     
    def task_summary(self, last=5, logging=None, console_summary=False):
        if self.summary is None:
            res=pd.DataFrame(self.res, columns=['job_id','key_id','url','res'])
            self.res_df=res
            
            #time processing
            job_timer=pd.DataFrame(self.job_timer, columns=['job_id','time'])
            job_timer['elapse']=(job_timer['time']-job_timer.ix[1,'time']).apply(lambda x:x.total_seconds())
                
            #pusedo_batch_processing
            if self.setting['use_batch']:
                res['batch_id']=res['job_id']
                job_timer['batch_id']=job_timer['job_id']
            else:
                res=res.sort_values(['key_id','job_id'])
                for i in range(len(res['key_id'].unique())):
                    key_id=sorted(res['key_id'].unique())[i]
                    batch_ids=[i*10+x for x in pd.cut(res[res.key_id==key_id]['job_id'],5,labels=range(5)).tolist()] # split jobs of specific key to 5 batch
                    res.ix[res.key_id==key_id, 'batch_id']=batch_ids
                job_timer=job_timer.merge(res[['job_id','batch_id']],on='job_id')
                job_timer=job_timer.groupby('batch_id').apply(lambda x:x.ix[x.index.max(),:])    
                           
            error=res.groupby(['batch_id']).apply(lambda x: pd.Series([
                len(x),
                x['res'].apply(lambda y:not isinstance(y, basestring)).sum(),
                x['res'].apply(lambda y:not isinstance(y, basestring)).sum()/float(len(x)),
                ]))
            error.columns=['batch_size','batch_err','batch_err_rate']        
            key_jobs=res[['key_id','batch_id']].drop_duplicates().reset_index(drop=True)
            key_jobs=key_jobs.sort_values(['key_id','batch_id'])
            key_jobs=key_jobs.merge(error, left_on='batch_id', right_index=True, how='left')
            key_jobs['cum_call']=key_jobs.groupby('key_id').apply(lambda x:x['batch_size'].cumsum()).reset_index(level='key_id', drop=True)
            key_jobs['cum_err_rate']=key_jobs.groupby('key_id').apply(lambda x:x['batch_err'].cumsum()/x['cum_call']).reset_index(level='key_id', drop=True)
            key_jobs=key_jobs.merge(job_timer[['batch_id','elapse']], on='batch_id', how='left')
            self.summary=key_jobs
            if not self.setting['use_batch']:                
                self.setting['batch_size']=error.batch_size.min()
                
        ## reporting
        fin_last=int(min([last, self.setting['batch_size']]))
        if not logging or console_summary:
            print '==== Google UrlShortener Load Test Report ===='
            print 'worktype: '+ self.setting['worktype']
            print 'jobsetting: {{use_batch: {use_batch}, key_batch_size: {batch_size}, sleep: {sleep}, repeat: {repeat}}},'.format(**self.setting)
            print ''
            print self.summary[['key_id','batch_err_rate','cum_call','cum_err_rate','elapse']].groupby('key_id').apply(lambda x:x.tail(fin_last))
        
        if logging:
            summary=self.summary.copy()
            for k,v in self.setting.iteritems():
                summary[k]=v
            summary=summary[['use_batch','batch_size','sleep','repeat','key_id','batch_err_rate','cum_call','cum_err_rate','elapse']]
            summary=summary.groupby('key_id').apply(lambda x:x.tail(fin_last)).reset_index(drop=True)
            if not os.path.isfile(logging):
                summary.to_csv(logging,encoding='utf8', index=False)                
            else:
                log=pd.read_csv(logging, encoding='utf8')
                log=log.append(summary)
                log.to_csv(logging,encoding='utf8', index=False)                

            
            
            