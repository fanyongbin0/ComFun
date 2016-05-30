#!/usr/bin/env /usr/bin/python26
# -*- coding: utf8 -*-
import time
import os,sys
import logging
from os.path import getsize

linux = True;
cwd = os.getcwd()
if cwd.startswith("/"):
    rootpath = "/data/opt/dataplatform"
    sys.path.append("/data/opt/dataplatform/pysrc")
else:
    rootpath = "c:\\workspace\\dataplatform"
    sys.path.append("c:\\workspace\\dataplatform\\pysrc")
    linux = False;

import cpc_wget_file
import util

log = logging.getLogger()

def merge(path1,file1):
    fout = open(path1+'/'+file1, 'w')
    for file in os.listdir(path1):
	pathfile = path1+"/"+file

	if os.path.isfile(pathfile) and file.startswith("jz") and file!=file1:
	    fin = open(pathfile,"r")
	    while True:
		s = fin.read(16*1024)
		if not s:
		    break
		fout.write(s)
	    fin.close
    fout.close

def startimport(daystr2):
    log.info("start startimport=======")
    
    jzdisp = {}
    jzdispdir = rootpath+"/data/jzdisphourmysql/"+daystr2
    jzclickdir = rootpath+"/data/jzclickhourmysql/"+daystr2
    jzdispfout = "jzdisphourmysql-"+daystr2
    jzclickfout = "jzclickhourmysql-"+daystr2
    merge(jzdispdir,jzdispfout)
    merge(jzclickdir,jzclickfout)
    jzdispfile = jzdispdir + "/" + jzdispfout
    jzclickfile = jzclickdir + "/" + jzclickfout
    if getsize(jzdispfile)==0:
        return
    if getsize(jzclickfile)==0:
        return

    jzdisp = util.readfiles2mapCpcNew(daystr2,jzdispfile,jzclickfile,{})
    
    log.info("jzdisp=%d"%(len(jzdisp.keys())))
    mysqlhelper = MySQLHelper()
    mysqlhelper.setstatconn()
    pagesize = 100
    
    #mysqlhelper.query("delete from t_jzcpcdisp_"+yyyymm+" where  dispday like '"+daystr2+"%'")
    #bench_insertdb(mysqlhelper,pagesize,jzdisp,sql,handlerValues_jzdisp)
    bench_insertdb(mysqlhelper,pagesize,jzdisp,daystr2)
    #mysqlhelper.close()
    if os.path.exists(jzdispfile):
	os.remove(jzdispfile)
    if os.path.exists(jzclickfile):
        os.remove(jzclickfile)
    print "over.time=%s"%(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))

def bench_insertdb(mysqlhelper,pagesize,data,daystr2):
    #sql数组MAP，分表，共对应三组表，24张表
    i = [[],[],[]]
    sql = [[],[],[]]
    values = [[],[],[]]
    
    for j in range(64):
        if j < 10:
            tbstr = "0" + str(j)
        else:
            tbstr = str(j);
        i[0].append(0)
        values[0].append([])
        sql[0].append("insert into ad_displog_day_" + tbstr +"(user_id,campaign_id,subscribe_id,entity_id,entity_type,creative_id,online_date,pv,click,total_consume,mobile_pv,mobile_click,mobile_consume,uclick0,uconsume0,uclick1,uconsume1,coupon_consume,seo_data) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        mysqlhelper.query("delete from ad_displog_day_" + tbstr +" where online_date='" + daystr2 + "'")
    
    for j in range(256):
        if j < 10:
            tbstr = "00" + str(j)
        elif j < 100:
            tbstr = "0" + str(j)
        else:
            tbstr = str(j)    
        i[1].append(0)
        values[1].append([])
        sql[1].append("insert into ad_displog_local_" + tbstr +"(user_id,campaign_id,subscribe_id,entity_id,entity_type,creative_id,online_date,local,pv,click,total_consume,mobile_pv,mobile_click,mobile_consume,uclick0,uconsume0,uclick1,uconsume1,coupon_consume) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        mysqlhelper.query("delete from ad_displog_local_" + tbstr +" where online_date='" + daystr2 + "'")

    for j in range(256):
        if j < 10:
            tbstr = "00" + str(j)
        elif j < 100:
            tbstr = "0" + str(j)
        else:
            tbstr = str(j)
        i[2].append(0)
        values[2].append([])
        sql[2].append("insert into ad_displog_param_" + tbstr +"(user_id,campaign_id,subscribe_id,entity_id,entity_type,creative_id,online_date,param,pv,click,total_consume,mobile_pv,mobile_click,mobile_consume,uclick0,uconsume0,uclick1,uconsume1,coupon_consume) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        mysqlhelper.query("delete from ad_displog_param_" + tbstr +" where online_date='" + daystr2 + "'")
        
    for key in data.keys():
        keys = key.split(",")
        tableid = util.getTableidByUser(long(keys[2]))
        tableidBySub = util.getTableidBySub(int(keys[4]))
        if keys[1]=="4":
            #continue
            seo_pv = data[key]['seo_pv0']+data[key]['seo_pv1']
            seo_click = data[key]['seo_click0']+data[key]['seo_click1']
            seo_consume = data[key]['seo_consume0']+data[key]['seo_consume1']

            pv = data[key]['pv0']+data[key]['pv1']
            click = data[key]['click0']+data[key]['click1']
            consume = data[key]['consume0']+data[key]['consume1']
            coupon_consume = data[key]['coupon_consume0']+data[key]['coupon_consume1']

            seo_data = "pv:"+str(seo_pv)+",ck:"+str(seo_click)+",cons:"+str(seo_consume)+",m_pv:"+str(data[key]['seo_pv1'])+",m_ck:"+str(data[key]['seo_click1'])+",m_cons:"+str(data[key]['seo_consume1'])
            values[0][tableid].append((keys[2],keys[3],keys[4],keys[5],keys[6],keys[7],daystr2,pv,click,consume,data[key]['pv1'],data[key]['click1'],data[key]['consume1'],data[key]['uclick0'],data[key]['uconsume0'],data[key]['uclick1'],data[key]['uconsume1'],coupon_consume,seo_data))
           
            i[0][tableid] += 1
            if i[0][tableid]==pagesize:
                log.info("tableid0=%s i=%d"%(str(tableid),i[0][tableid]))
                mysqlhelper.executemany(sql[0][tableid],values[0][tableid])
                values[0][tableid] = []
                i[0][tableid] = 0
        if keys[1]=="2":
            cityid = keys[9]
            if cityid=="-":
                cityid="0"
            pv = data[key]['pv0']+data[key]['pv1']
            click = data[key]['click0']+data[key]['click1']
            consume = data[key]['consume0']+data[key]['consume1']
            coupon_consume = data[key]['coupon_consume0']+data[key]['coupon_consume1']
            values[1][tableidBySub].append((keys[2],keys[3],keys[4],keys[5],keys[6],keys[7],daystr2,cityid,pv,click,consume,data[key]['pv1'],data[key]['click1'],data[key]['consume1'],data[key]['uclick0'],data[key]['uconsume0'],data[key]['uclick1'],data[key]['uconsume1'],coupon_consume))
            i[1][tableidBySub] += 1
            if i[1][tableidBySub]==pagesize:
                log.info("tableid1=%s i=%d"%(str(tableidBySub),i[1][tableidBySub]))
                mysqlhelper.executemany(sql[1][tableidBySub],values[1][tableidBySub])
                values[1][tableidBySub] = []
                i[1][tableidBySub] = 0
        if keys[1]=="3":
            param = keys[9]
            if param=="-" or param.startswith("c"):
                param="0"
            pv = data[key]['pv0']+data[key]['pv1']
            click = data[key]['click0']+data[key]['click1']
            consume = data[key]['consume0']+data[key]['consume1']
            coupon_consume = data[key]['coupon_consume0']+data[key]['coupon_consume1']
            
            values[2][tableidBySub].append((keys[2],keys[3],keys[4],keys[5],keys[6],keys[7],daystr2,param,pv,click,consume,data[key]['pv1'],data[key]['click1'],data[key]['consume1'],data[key]['uclick0'],data[key]['uconsume0'],data[key]['uclick1'],data[key]['uconsume1'],coupon_consume))
            i[2][tableidBySub] += 1
            if i[2][tableidBySub]==pagesize:
                log.info("tableid2=%s i=%d"%(str(tableidBySub),i[2][tableidBySub]))
                mysqlhelper.executemany(sql[2][tableidBySub],values[2][tableidBySub])
                values[2][tableidBySub] = []
                i[2][tableidBySub] = 0

    log.info("come here")
    for j in range(64):
        if i[0][j]>0:
            mysqlhelper.executemany(sql[0][j],values[0][j])
            log.info("j=%d"%(j))
    for j in range(256):
        if i[1][j]>0:
            mysqlhelper.executemany(sql[1][j],values[1][j])
            log.info("j=%d"%(j))
    for j in range(256):
        if i[2][j]>0:
            mysqlhelper.executemany(sql[2][j],values[2][j])
            log.info("j=%d"%(j))
    log.info("commit")
    mysqlhelper.commit()
    
    log.info("len=%d"%(len(data)))
    log.info("end")
        
if __name__=="__main__":
    if(len(sys.argv)>1):
        daystr2 = sys.argv[1]
    else:
        mytime = time.time()
        daystr2 = time.strftime("%Y%m%d", time.localtime(mytime))
        daystr2 = "20150606"

    if(len(sys.argv)>2):
        from  conf.offline.MySQLHelper import  MySQLHelper
        handler = logging.FileHandler(rootpath+"/log/cpc_file2mysql_newTable.log.offline."+daystr2)
        fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        log.addHandler(handler)
        log.setLevel(logging.NOTSET)
        log.info("offline")
    else:
        from  conf.online.MySQLHelper94 import MySQLHelper
        handler = logging.FileHandler(rootpath+"/log/cpc_file2mysql_newTable.log.online."+daystr2)
        fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        log.addHandler(handler)
        log.setLevel(logging.NOTSET)
        log.info("online")
    #sys.exit(0)
    if not linux:
        daystr2 = "20131109"
    
    log.info(daystr2)

    if linux:
        startimport(daystr2)
