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
        7. download Oauth Client ID as a json file 
        5. add a keyfile in local folder, which formats: 'self_assigned_KeyID,API_key,Oath_Client_ID_Json_File_Path'
        6. install Google API python client library
            -- pip install --upgrade google-api-python-client
        7. make sure other 3rd-party library are installed
        8. run (NEEDS human intervention if you don't have an Oauth crendential file saved in the local folder beforehanded)!
    Reference:
        1. url shortener api, https://goo.gl/TZ4vHy
        2. url shortener guide, Google API python client library. https://goo.gl/tgz8xD
        3. how to send batch request using Google API. https://goo.gl/tsqFZe
        4. official example of url shortener api. https://goo.gl/NsoJ7R
            
@author: KimballWu
"""

import threading
from contextlib import contextmanager
import multiprocessing.dummy as mt
import httplib2
from googleapiclient import discovery
from googleapiclient import errors as goolgeApiError
from oauth2client.file import Storage
import time
import datetime
import math
import pandas as pd
import os
import sys
#%%




class BackoffLimitExceed(Exception):
    pass

class UrlShortener(object):    
    def __init__(self, keyfiles):
        ## api credentail infos
        with open(keyfiles,'rb') as infile:
            project_keys=infile.readlines()
            project_keys=[x.strip().split(',') for x in project_keys]
        self.keyfile=project_keys
              
        ##job settings
        self.KEYS=len(project_keys)
        self.key_info=dict()
        for keyID, access_token, crendential in self.keyfile:
            self.key_info[keyID]={'credential':None, 'service':None, 'keylock':threading.RLock() ,'backoff':0, 'backoff_holding':0}
        self.global_lock=threading.RLock()
        self.global_timer=datetime.datetime.now()
        self.setting=dict()
        self.terminate=False
        self.unit_job_list=list()
        self.batch_job_dict=dict()     
        
        ## result        
        self.job_timer=list()
        self.res=list()
   
    def _get_http_con(self):
        return httplib2.Http(disable_ssl_certificate_validation=True, timeout=10000)

    @contextmanager     
    def _request_key_lock(self, keyID):
        try:
            self.key_info[keyID]['keylock'].acquire()
            yield
        except Exception as e:
            raise e
        finally:
            self.key_info[keyID]['keylock'].release()
            
    @contextmanager     
    def _global_sleep_lock(self, itermit):    
        try:
            self.global_lock.acquire()
            if itermit:
                elapse=datetime.datetime.now()-self.global_timer
                diff=3-elapse.total_seconds()
                if diff>0:
                    time.sleep(itermit)
            self.global_timer=datetime.datetime.now()
            yield
        except Exception as e:
            raise e
        finally:
            self.global_lock.release()

    def _get_url_generator(self, target_urls):
        if self.setting['use_batch']: 
            batch_size=self.setting['batch_size']
            partition_num=int(math.ceil(float(len(target_urls))/batch_size))
            partitioner=((i,target_urls[batch_size*i : batch_size*(1+i)]) for i in range(partition_num))
            return partitioner
        else:
            return enumerate(target_urls)

    def _get_submit_worker(self, keyID, apikey, assess_token_path):        
        #get credentail
        if not self.key_info[keyID]['credential']:
            credential=Storage(assess_token_path).get()
            credential.refresh(self._get_http_con())
            Storage(assess_token_path).put(credential)
            self.key_info[keyID]['credential']=credential
        else:
            credential=self.key_info[keyID]['credential']

        #get service            
        if not self.key_info[keyID]['service']:
            authorized_http_con=credential.authorize(self._get_http_con())
            GET_SERVICE_RETRY=1
            while GET_SERVICE_RETRY>0:
                try:
                    service=discovery.build('urlshortener', 'v1', developerKey=apikey, http=authorized_http_con, cache_discovery=False)
                    break
                except:
                    print 'Urlshortener Service Building Failed, retry:{}'.format(GET_SERVICE_RETRY)
                    GET_SERVICE_RETRY-=1
                    if GET_SERVICE_RETRY<0:
                        sys.exit(1)                    
                    time.sleep(1)
            self.key_info[keyID]['service']=service
        else:
            service=self.key_info[keyID]['service']        
                
        #return        
        if self.setting['use_batch']: 
            return service, service.new_batch_http_request()
        else:
            return service
            
    def _gen_unit_job(self, url, keyID,  service):
        if self.setting['worktype']=='convert':         
            req=service.url().insert(body={'longUrl':url,'quotaUser':keyID}, fields='id')
        elif self.setting['worktype']=='analyze':
            req=service.url().get(shortUrl=url, projection='ANALYTICS_CLICKS', fields='analytics(allTime/shortUrlClicks)')
        return req     

    def _get_unit_job_list(self, target_urls):
        res=[]
        for idx, url in self._get_url_generator(target_urls):
            keyID, apikey, assess_token_path=self.keyfile[idx % self.KEYS]           
            service =self._get_submit_worker(keyID, apikey, assess_token_path)
            req=self._gen_unit_job(url, keyID, service)
            res.append((idx, keyID, url, req))
        self.unit_job_list=res
        return res         

    def _gen_batch_job(self, idx, keyID, batch_urls, service, batch_worker):
        if self.setting['worktype']=='convert':         
            for url in batch_urls:
                req=service.url().insert(body={'longUrl':url}, fields='id')
                batch_worker.add(req, request_id='{};{};{}'.format(idx, keyID, url), callback=self._append_batch_res)
        elif self.setting['worktype']=='analyze':
            for url in batch_urls:
                req=service.url().get(shortUrl=url, projection='ANALYTICS_CLICKS', fields='analytics(allTime/shortUrlClicks)')
                batch_worker.add(req, request_id='{};{};{}'.format(idx, keyID, url), callback=self._append_batch_res)
        return batch_worker

    def _get_batch_job_list(self, target_urls):       
        res=[]
        for idx, batch_urls in self._get_url_generator(target_urls):
            keyID, apikey, assess_token_path=self.keyfile[idx % self.KEYS]           
            service, batch_worker=self._get_submit_worker(keyID, apikey, assess_token_path)
            batch_job= self._gen_batch_job(idx, keyID, batch_urls, service, batch_worker)
            res.append((idx, batch_job))
            self.batch_job_dict[idx]={'batch_job':batch_job, 'batch_urls': batch_urls, 'keyID':keyID, 'execeedRateLimit':0}          
        return res
      
    def _append_batch_res(self, req_id, req, exception): 
        '''call back funciton of the unit batched job'''
        batch_id, keyID, res_url= req_id.split(';')
        batch_id= int(batch_id)
        if exception:
            self.res.append((batch_id, keyID, res_url, exception))
            if isinstance(exception, goolgeApiError.HttpError):
                self.key_info[keyID]['backoff']+=1
                self.batch_job_dict[batch_id]['execeedRateLimit']+=1
        else:
            if self.setting['worktype']=='convert':
                self.res.append((batch_id, keyID, res_url, req['id']))
            elif self.setting['worktype']=='analyze':
                self.res.append((batch_id, keyID, res_url, req['analytics']['allTime']['shortUrlClicks']))

    def _cal_sleep(self, keyID):    
        key_backoff=self.key_info[keyID]['backoff']
        key_backoff_holding=self.key_info[keyID]['backoff_holding']
      
        if self.key_info[keyID]['backoff'] > self.setting['max_backoff']:
            return (-1,-1)
        else:
            sleep=self.setting['sleep'] + key_backoff * self.setting['backoff_incre']
            if not key_backoff_holding:
                return (sleep, key_backoff * self.setting['backoff_hold'])
            else:
                return (sleep, 0)
                
    def _do_batch_job(self, batch_job, retry=0):
        batch_id, batch_req=batch_job
        keyID=self.batch_job_dict[batch_id]['keyID']
        #determing wether job needs to execute and the sleep time after execute
        if self.terminate:
            e=BackoffLimitExceed('Key {} exceeded the backoff limit!'.format(keyID))
            self._terminating_batch_job(batch_job, e)
            return 
        elif self.key_info[keyID]['backoff'] > self.setting['max_backoff']:
            print 'job call {}/{} request with key {} failed due to backoff limit exceeded!'.format(batch_id+1, len(self.batch_job_dict), keyID)
            print 'set all job of key {} to terminated!'.format(keyID)
            self.terminate=True
            e=BackoffLimitExceed('Key {} exceeded the backoff limit!'.format(keyID))
            self._terminating_batch_job(batch_job, e)
            return           
        else:
            sleep, backoff_sleep=self._cal_sleep(keyID)                    
            if backoff_sleep>0:
                print 'Hold job because of rate limit has been trigger! Sleep for {} seconds for Key {}.'.format(backoff_sleep, keyID)            
                time.sleep(backoff_sleep)      
                self.key_info[keyID]['backoff_holding']=True
        
        exc=False
        if retry==0:
            try:
                self._get_batch_job_res(keyID, batch_req)
            except:
                exc=self._do_batch_job(batch_job, retry=retry+1)
            finally:
                if not exc:
                    if retry>0:
                        print 'batch {}/{} request with key {} initially failed, success with retry.'.format(batch_id+1, len(self.batch_job_dict), keyID) 
                    print 'batch call {}/{} request with key {} finished! sleep for {} seconds.'.format(batch_id+1, len(self.batch_job_dict), keyID, sleep) 
                else:
                    print 'batch call {}/{} request with key {} failed! Store exception'.format(batch_id+1, len(self.batch_job_dict), keyID)                    
                time.sleep(sleep)
                self.job_timer.append((batch_id, datetime.datetime.now()))
                return 
    
        elif retry>0:
            try:
                self._get_batch_job_res(keyID, batch_req)
            except Exception as e:
                exc=True
                self._terminating_batch_job(batch_job, exc=e)
            finally:
                time.sleep(sleep)
                return exc    

    def _get_batch_job_res(self, keyID, batch_req):
        with self._request_key_lock(keyID):
            with self._global_sleep_lock(3):
                pass
            batch_req.execute()
            
    def _terminating_batch_job(self, batch_job, exc):
        batch_id=batch_job[0]
        keyID=self.batch_job_dict[batch_id]['keyID']
        batch_urls=self.batch_job_dict[batch_id]['batch_urls']
        for url in batch_urls:
            self.res.append((batch_id, keyID, url, exc))
        self.job_timer.append((batch_id, datetime.datetime.now()))       
        
              
    def _do_unit_job(self, job, retry=0):
        job_id, keyID, url, req = job        
        
        #determing wether job needs to execute and the sleep time after execute
        if self.terminate:
            self._terminating_job(job)
            return 
        elif self.key_info[keyID]['backoff'] > self.setting['max_backoff']:
            print 'job call {}/{} request with key {} failed due to backoff limit exceeded!'.format(job_id+1, len(self.unit_job_list), keyID)
            print 'set all job of key {} to terminated!'.format(keyID)
            self.terminate=True        
            self.terminating_job(job)
            return           
        else:
            sleep, backoff_sleep=self._cal_sleep(keyID)                    
            if backoff_sleep>0:
                print 'Hold job because of rate limit has been trigger! Sleep for {} seconds for Key {}.'.format(backoff_sleep, keyID)            
                time.sleep(backoff_sleep)      
                self.key_info[keyID]['backoff_holding']=True
             
        exc=False
        #initialtor logic
        if retry==0: 
            try:
                self._get_unit_job_res(job)
            except goolgeApiError.HttpError as e:
                new_retry=retry+1
                self.key_info[keyID]['backoff']+=1
                exc=self._do_unit_job(job, retry=new_retry) #final result of retry 
            except Exception as e:
                new_retry=retry+1
                exc=self._do_unit_job(job, retry=new_retry) #final result of retry 
            finally:              
                if not exc:
                    if retry>0:
                        print 'job {}/{} request with key {} initially failed, success with retry.'.format(job_id+1, len(self.unit_job_list), keyID) 
                    print 'job call {}/{} request with key {} finished! sleep for {} seconds.'.format(job_id+1, len(self.unit_job_list), keyID, sleep) 
                else:
                    print 'job call {}/{} request with key {} failed! Store exception'.format(job_id+1, len(self.unit_job_list), keyID)                    
                time.sleep(sleep)
                self.job_timer.append((job_id, datetime.datetime.now()))
        #retry logic                   
        elif retry>0:
            try:
                self._get_unit_job_res(job)
            except Exception as e:
                self.res.append((job_id, keyID, url, e))
                exc=True
                if isinstance(e, goolgeApiError.HttpError):
                    self.key_info[keyID]['backoff']+=1
            finally:
                time.sleep(sleep)
                return exc

    def _get_unit_job_res(self, job):
        job_id, keyID, url, req = job
        with self._request_key_lock(keyID):
            rep=req.execute()
        if self.setting['worktype']=='convert':
            self.res.append((job_id, keyID, url, rep['id']))
        elif self.setting['worktype']=='analyze':
            self.res.append((job_id, keyID, url, rep['analytics']['allTime']['shortUrlClicks']))
              
    def _terminating_job(self, job):
        job_id, keyID, url, req = job  
        e=BackoffLimitExceed('Key {} exceeded the backoff limit!'.format(keyID))
        self.res.append((job_id, keyID, url, e))
        self.job_timer.append((job_id, datetime.datetime.now()))
        return 

#%%    
    def _main(self, target_urls):
        pool=mt.Pool(self.KEYS)
        self.job_timer.append((-1, datetime.datetime.now())) #start processing
        if self.setting['use_batch']:
            joblist=self._get_batch_job_list(target_urls)
            self.job_timer.append((-2, datetime.datetime.now())) #all http conneciotn authorization finished
            for job in joblist:
                pool.apply_async(self._do_batch_job, args=(job,))      

        else:
            joblist=self._get_unit_job_list(target_urls)
            self.job_timer.append((-2, datetime.datetime.now())) #all http conneciotn authorization finished
            for job in joblist:
                pool.apply_async(self._do_unit_job, args=(job,))      
                
        #finallize
        pool.close()
        pool.join()          
      
    def do_jobs(self, worktype, target_urls, use_batch=False, batch_size=1, sleep=1, backoff_incre=1, max_backoff=3):
        #setting
        self.setting={'worktype':worktype, 'use_batch':use_batch, 'batch_size':batch_size,
                      'sleep':sleep, 'backoff_incre':backoff_incre, 'max_backoff':max_backoff}
        self.res=list()
        #do main jobs
        self._main(target_urls)
    
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
        self.res_df=None
        self.jobtimer_df=None
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

    def _main_not_pool(self, target_urls):
        self.job_timer.append((-1, datetime.datetime.now())) #start processing
        if self.setting['use_batch']:
            joblist=self._get_batch_job_list(target_urls)
            self.job_timer.append((-2, datetime.datetime.now())) #all http conneciotn authorization finished
            for job in joblist:
                self._do_batch_job(job)

        else:
            joblist=self._get_unit_job_list(target_urls)
            self.job_timer.append((-2, datetime.datetime.now())) #all http conneciotn authorization finished
            for job in joblist:
                self._do_unit_job(job)      
                
    def do_jobs(self, worktype, target_urls, use_batch=False, batch_size=1, repeat=-1, sleep=1, backoff_incre=1, max_backoff=2, pool=True, backoff_hold=60):
        use_batch=False if batch_size==1 else use_batch
        self.setting={'worktype':worktype, 'use_batch':use_batch, 'batch_size':batch_size, 'repeat':repeat,
                      'sleep':sleep, 'backoff_incre':backoff_incre, 'max_backoff':max_backoff, 'backoff_hold':backoff_hold}
        self.res=list()
        if not pool:
            self._main_not_pool(target_urls)
        else:
            super(UrlShortener_load_tester, self)._main(target_urls)
        
    def task_summary(self, last=5, logging=None, console_summary=False):
        if self.summary is None:
            res=pd.DataFrame(self.res, columns=['job_id','key_id','url','res'])
            self.res_df=res
            
            #time processing
            job_timer=pd.DataFrame(self.job_timer, columns=['job_id','time'])
            job_timer['elapse']=(job_timer['time']-job_timer.ix[1,'time']).apply(lambda x:x.total_seconds())
            self.jobtimer_df=job_timer
                
            #change id and pusedo_batch_processing
            if self.setting['use_batch']:
                res['batch_id']=res['job_id']
                job_timer['batch_id']=job_timer['job_id']
            else:
                res=res.sort_values(['key_id','job_id'])
                cut_part=len(res) if len(res)<5 else 5
                for i in range(self.KEYS):
                    key_id=[x[0] for x in self.keyfile][i]
                    batch_ids=[i*10+x for x in pd.cut(res[res.key_id==key_id]['job_id'], cut_part ,labels=range(cut_part)).tolist()] # split jobs of specific key to 5 batch
                    res.ix[res.key_id==key_id, 'batch_id']=batch_ids
                job_timer=job_timer.merge(res[['job_id','batch_id']],on='job_id')
                job_timer=job_timer.groupby('batch_id').apply(lambda x:x.ix[x.index.max(),:])    
                
            # formatting and combine                
            key_jobs=res[['key_id','batch_id']].drop_duplicates().reset_index(drop=True)
            key_jobs=key_jobs.sort_values(['key_id','batch_id'])
    
            error=res.groupby(['batch_id']).apply(lambda x: pd.Series([
                len(x),
                x['res'].apply(lambda y:not isinstance(y, basestring)).sum(),
                x['res'].apply(lambda y:not isinstance(y, basestring)).sum()/float(len(x)),
                ]))
            error.columns=['batch_size','batch_err','batch_err_rate']        
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
        
            print '\njob milestone:'
            init=(self.job_timer[1][1]-self.job_timer[0][1]).total_seconds()
            totals=(self.job_timer[-1][1]-self.job_timer[0][1]).total_seconds()
            print '----Finished unit/batch job authorization at: {}'.format(int(init))
            print '----Finished all unit/batch {}'.format(int(totals))
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

