#-*-encoding:utf-8-*-
__author__ = "rody800"

import Queue
import threading
import sys
import time
import urllib
import requests
from bs4 import BeautifulSoup
import re
from hashlib import md5
import MySQLdb

reload(sys)
sys.setdefaultencoding('utf-8')

class ConstData():
    @property
    def SLEEP_TIME1(self):
        return 0.01
    @property
    def SLEEP_TIME2(self):
        return 0.01

    @property
    def SLEEP_TIME3(self):
         return 10

    @property
    def DEAULT_THREAD_COUNT(self):
        return 5

CONST=ConstData()

class ThreadWork(threading.Thread):
    def __init__(self, jobQueue, resultQueue,timeOut=CONST.SLEEP_TIME3, **kwargs):
        threading.Thread.__init__(self, kwargs=kwargs)
        self.timeout = timeOut
        self.setDaemon(True)
        self.jobqueue = jobQueue
        self.resultqueue = resultQueue
        self.start()

    def run(self):
        while 1:
            if self.jobqueue.empty():
               time.sleep(CONST.SLEEP_TIME1)  
               continue
            try:
                callfunc, args, kwargs = self.jobqueue.get(timeout=self.timeout)
                 
                results = callfunc(args[0],args[1],args[2],args[3],args[4],args[5])
                self.resultqueue.put(results)     

            except:
                print "thread excute error %s" % self.getName()
                return

     
class ThreadPool:
    def __init__( self, threadCount=CONST.DEAULT_THREAD_COUNT):
        self.jobqueue = Queue.Queue()
        self.resultqueue = Queue.Queue()
        self.threads = []
        self.create_thread( threadCount )

    def create_thread( self, threadCount ):
        for i in range( threadCount ):
            threadhandle = ThreadWork( self.jobqueue, self.resultqueue )
            self.threads.append(threadhandle)

    def wait_over(self):
        while len(self.threads):
            thread = self.threads.pop()
            if thread.isAlive(): 
                thread.join()
    
    def add_job( self, callable, *args, **kwargs ):
        self.jobqueue.put( (callable,args,kwargs) )

def GetPageUrl(url):  
    s = requests.Session() 
    htmltext=s.get(url)
    rdata=htmltext.text
    soup = BeautifulSoup(rdata) 
    content = soup.findAll('a')
    urls=[]
    for hrefs in content:
        hvalue= hrefs.get("href",'') 
        v=hvalue.replace(" ","")
        if re.match("http://", v) == None:
            continue
        urls.append(v)
    return urls

def GetUrlData(url,conn):
    s = requests.Session() 
    try:
       htmltext=s.get(url)
       rdata=htmltext.text
       conn.ping(True)
       cursor=conn.cursor()  
       url_e=MySQLdb.escape_string(url)
       rdata_e=MySQLdb.escape_string(rdata)
       sql="insert into page_table(page_url,page_content) values('%s','%s')" % (url_e,rdata_e)
       cursor.execute(sql)
       cursor.close()
    except:
       return

       
def Spider( conn,urlstore,baseurl,depth,tno,sleeptime):
    time.sleep(sleeptime) 
    try:
        if depth == 1:  

            urls = GetPageUrl(baseurl)

            for url in urls:
                urlmd5=md5(url.encode('utf-8')).hexdigest()
                if not urlstore.has_key(urlmd5):
                    urlstore[urlmd5]=0
                    print "thread %d to get : %s" % (tno,url)
                    GetUrlData(url,conn)           
        else:  

            urls = GetPageUrl(baseurl)  
            if urls:  
                for url in urls:  
                    if 1 : 
                        Spider(conn,urlstore,baseurl,depth-1,tno,sleeptime ) 
                    else:
                        continue
        return ("ok",baseurl)                    
    except:
        print "spider error"
        return ("error",baseurl)   

def start(conn,urlstore,urls,depth,n):
    print 'start spider'
    tp = ThreadPool(n)
    for i in range(n):
        time.sleep(CONST.SLEEP_TIME1)
        tp.add_job( Spider,conn,urlstore,urls[i],depth,i+1,CONST.SLEEP_TIME2 )
        
    tp.wait_over()

    while tp.resultqueue.qsize():
        print tp.resultqueue.get()
    print 'end spider'     

def InitDb(conn):
    sql='''DROP TABLE IF EXISTS `page_table`;
CREATE TABLE IF NOT EXISTS `page_table` (
  `id` int(8) unsigned NOT NULL AUTO_INCREMENT,
  `page_url` varchar(250) NOT NULL DEFAULT '',
  `page_content` mediumtext NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;
'''
    cursor = conn.cursor()  
    n = cursor.execute(sql)  
    if  n:
        print "init db error"
    cursor.close() 
 
if __name__ == '__main__':
    #确保mysql已经创建spiderdb数据库
    conn=MySQLdb.connect(host="127.0.0.1",user="root",passwd="123456",db="spiderdb",charset="utf8")
    InitDb(conn)
    
    urls=[ "http://www.sohu.com/",
          "http://www.163.com/",
          "http://www.sina.com.cn/"  
        ]
    # 启用多个线程去spider url,网页深度2
    urlstore={}
    start(conn,urlstore,urls,2,len(urls))
