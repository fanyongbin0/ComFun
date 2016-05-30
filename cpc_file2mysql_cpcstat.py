#!/usr/bin/env /usr/bin/python26
# -*-coding: utf8-*-

import time
import os, sys
import logging
from os.path import getsize
import cpc_wget_file
import util
import threading
import redis

ROOT_PATH = "/opt/script/dataplatform"
sys.path.append("/opt/script/dataplatform/pysrc")

CMC_LOCAL_PATH = "/opt/dataplatform/dataLogRealTimeProcessor/paramConfig/cmcLocal"
CMC_PROVINCE_PATH = "/opt/dataplatform/dataLogRealTimeProcessor/paramConfig/cmcProvince"
CMC_CITY_PROVINCE_PATH = "/opt/dataplatform/dataLogRealTimeProcessor/paramConfig/cmcCityProvince"

CMC_CATE_PATH = "/opt/dataplatform/dataLogRealTimeProcessor/paramConfig/cmcCate"
CMC_UNIT_PARAMETER_PATH = "/opt/dataplatform/dataLogRealTimeProcessor/paramConfig/cmcUnitParameter"

REDIS_HOST = "10.126.215.179"
REDIS_PORT = 6531
REDIS_PWD = "6f41dad73659783a"
REDIS_DB = 0

PAGE_SIZE = 1000

log = logging.getLogger()

"""
    ***************按天、分时入bidding_stat库， 地区、类目入bidding_cpcstat库****************
"""
def startimport(datestr2):
    log.info("start startimport, datestr2=" + datestr2)

    jzdispfile = ROOT_PATH + "/data/jzdisphourmysqlNew/jzdisphourmysql-" + datestr2
    if getsize(jzdispfile) == 0:
        return

    jzclickfile = ROOT_PATH + "/data/jzclickhourmysqlNew/jzclickhourmysql-" + datestr2
    if getsize(jzclickfile) == 0:
        return
	
    print "start readfiles2mapCpcAdvanced..."
    jzdata = util.readfiles2mapCpcAdvanced(datestr2, jzdispfile, jzclickfile, {})
    print "finished readfiles2mapCpcAdvanced."
    log.info("jzdata=%d" % (len(jzdata.keys())))
	
    threads = []
    t1 = threading.Thread(target=bench_insertdb_day_hour, args=(jzdata, datestr2))
    threads.append(t1)
    
    t2 = threading.Thread(target=bench_insertdb_local, args=(jzdata, datestr2))
    threads.append(t2)
    
    t3 = threading.Thread(target=bench_insertdb_param, args=(jzdata, datestr2))
    threads.append(t3)
    
    for th in threads:
        th.setDaemon(True)
        th.start()
        
    for th in threads:
        th.join()
        
    #bench_insertdb_day_hour(jzdata, datestr2)

"""
    ***************************入库分天和分时表*****************************
"""
def bench_insertdb_day_hour(jzdata, datestr2):
    print "start bench_insertdb_day_hour..."
    log.info("start bench_insertdb_day_hour...")
    timestart = time.time()
    
    # sql数组MAP，分表，共对应四组表
    i = [[], []]
    sql = [[], []]
    values = [[], []]
    
    mysqlhelper = MySQLHelper()
    mysqlhelper.setstatconn()

    # ad_displog_day表
    for j in range(64):
        if j < 10:
            tbstr = "0" + str(j)
        else:
            tbstr = str(j);

        i[0].append(0)
        values[0].append([])
        sql[0].append("insert into ad_displog_day_" + tbstr + "(user_id,campaign_id,subscribe_id,entity_id,entity_type,creative_id,online_date,pv,click,total_consume,mobile_pv,mobile_click,mobile_consume,uclick0,uconsume0,uclick1,uconsume1,coupon_consume,seo_data) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        mysqlhelper.query("delete from ad_displog_day_" + tbstr + " where online_date='" + datestr2 + "'")

    # ad_displog_hour表
    for j in range(256):
        if j < 10:
            tbstr = "00" + str(j)
        elif j < 100:
            tbstr = "0" + str(j)
        else:
            tbstr = str(j)

        i[1].append(0)
        values[1].append([])
        sql[1].append("insert into ad_displog_hour_" + tbstr + "(user_id,campaign_id,subscribe_id,entity_id,entity_type,creative_id,online_date,pv,click,total_consume,mobile_pv,mobile_click,mobile_consume,uclick0,uconsume0,uclick1,uconsume1,coupon_consume) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        mysqlhelper.query("delete from ad_displog_hour_" + tbstr + " where online_date>='" + datestr2 + "00' and online_date<='" + datestr2 + "23'")
        
    for key in jzdata.keys():
        keys = key.split(",")
        
        pv = jzdata[key]['pv0'] + jzdata[key]['pv1']
        click = jzdata[key]['click0'] + jzdata[key]['click1']
        consume = jzdata[key]['consume0'] + jzdata[key]['consume1']
        coupon_consume = jzdata[key]['coupon_consume0'] + jzdata[key]['coupon_consume1']
        
        # 按天入库
        if keys[1] == "1":
            tableid = util.getTableidByUser(long(keys[2]))
            seo_pv = jzdata[key]['seo_pv0'] + jzdata[key]['seo_pv1']
            seo_click = jzdata[key]['seo_click0'] + jzdata[key]['seo_click1']
            seo_consume = jzdata[key]['seo_consume0'] + jzdata[key]['seo_consume1']
            seo_data = "pv:" + str(seo_pv)+",ck:" + str(seo_click) + ",cons:" + str(seo_consume) + ",m_pv:" + str(jzdata[key]['seo_pv1']) + ",m_ck:" + str(jzdata[key]['seo_click1']) + ",m_cons:" + str(jzdata[key]['seo_consume1'])
            
            values[0][tableid].append((keys[2], keys[3], keys[4], keys[5], keys[6], keys[7], keys[8], pv, click, consume, jzdata[key]['pv1'], jzdata[key]['click1'], jzdata[key]['consume1'], jzdata[key]['uclick0'], jzdata[key]['uconsume0'], jzdata[key]['uclick1'], jzdata[key]['uconsume1'], coupon_consume, seo_data))
            
            # 如果条数等于pagesize则执行一次SQL
            i[0][tableid] += 1
            if i[0][tableid] == PAGE_SIZE:
                log.info("ad_displog_day tableId=%d i=%d" % (tableid, i[0][tableid]))
                mysqlhelper.executemany(sql[0][tableid], values[0][tableid])
                
                values[0][tableid] = []
                i[0][tableid] = 0
            
        # 分时入库
        if keys[1] == "4":
            tableidByHour = util.getHourTableidByUser(long(keys[2]))
            values[1][tableidByHour].append((keys[2], keys[3], keys[4], keys[5], keys[6], keys[7], keys[8], pv, click, consume, jzdata[key]['pv1'], jzdata[key]['click1'], jzdata[key]['consume1'], jzdata[key]['uclick0'], jzdata[key]['uconsume0'], jzdata[key]['uclick1'], jzdata[key]['uconsume1'], coupon_consume))
            
            i[1][tableidByHour] += 1
            if i[1][tableidByHour] == PAGE_SIZE:
                log.info("ad_displog_hour tableId=%d i=%d" % (tableidByHour, i[1][tableidByHour]))
                mysqlhelper.executemany(sql[1][tableidByHour], values[1][tableidByHour])
        		
                values[1][tableidByHour] = []
                i[1][tableidByHour] = 0
        
    log.info("executemany ad_displog_day final data start...")
    for j in range(64):
        if i[0][j] > 0:
            mysqlhelper.executemany(sql[0][j], values[0][j])
            log.info("tableId=%d" % (j))

    log.info("executemany ad_displog_hour final data start...")
    for j in range(256):
        if i[1][j] > 0:
            mysqlhelper.executemany(sql[1][j], values[1][j])
            log.info("tableId=%d" % (j))

    
    mysqlhelper.commit()
    timefinish = time.time()
    timecost = timefinish - timestart
    print "insert db of day and hour done, time cost: " + str(timecost)
    log.info("insert db of day and hour done, time cost: " + str(timecost))

""" 
    ******************************************** 入地区表 ************************************************
"""
def bench_insertdb_local(jzdata, datestr2):
    print "start bench_insertdb_local"
    log.info("start to insert local data, datestr2=" + datestr2)
    timestart = time.time()
    
    promotion_mysqlhelper = MySQLHelper()
    promotion_mysqlhelper.setpromotionconn()
    
    cmc_province_file = CMC_PROVINCE_PATH + "/cmcProvince." + datestr2
    province_map = getCpcProvince(cmc_province_file, promotion_mysqlhelper, {})
    
    cmc_city_province_file = CMC_CITY_PROVINCE_PATH + "/cmcCityProvince." + datestr2
    city_province_map = getCpcCityProvinceMap(cmc_city_province_file, {})
    area_map = getCpcAreaConfigMap(promotion_mysqlhelper, {})
    
    # 获取local_path
    cmc_local_file = CMC_LOCAL_PATH + "/cmcLocal." + datestr2
    local_map = getCpcLocalPathMap(cmc_local_file, {})
    
    cpcstat_mysqlhelper = MySQLHelper()
    cpcstat_mysqlhelper.setcpcstatconn()
    
    old_user_result = {}
    old_userid_result = {}
    new_user_result = {}
    new_userid_result = {}
    map_userid_tableid = {}
    
    """
       ------------------------------------- 选取地区数据，筛选出老用户与新用户 -----------------------------------------
    """
    for key in jzdata.keys():
        keys = key.split(",")
        # 只处理地区，其他的忽略
        if keys[1] == "1" or keys[1] == "3" or keys[1] == "4":
            continue
        
        userid = int(keys[2])
        pv = jzdata[key]['pv0'] + jzdata[key]['pv1']
        click = jzdata[key]['click0'] + jzdata[key]['click1']
        consume = jzdata[key]['consume0'] + jzdata[key]['consume1']
        coupon_consume = jzdata[key]['coupon_consume0'] + jzdata[key]['coupon_consume1']
        
        cityid = keys[9]
        if cityid == "-":
            cityid = "0"
        
        data = [keys[3], keys[4], keys[5], keys[6], keys[7], keys[8], cityid, pv, click, consume, jzdata[key]['pv1'], jzdata[key]['click1'], jzdata[key]['consume1'], jzdata[key]['uclick0'], jzdata[key]['uconsume0'], jzdata[key]['uclick1'], jzdata[key]['uconsume1'], coupon_consume]
        
        # 如果map_userid_tableid中存在userid,则为老用户
        if userid in map_userid_tableid:
            old_user_result[userid].append(data)
            old_userid_result[userid] += 1
        else:
            sql = "SELECT table_id FROM ad_user_map_local WHERE user_id=" + str(userid)
            result = cpcstat_mysqlhelper.queryRow(sql)
            # 老用户
            if result != None:
                if userid not in old_user_result:
                    old_user_result[userid] = [data]
                else:
                    old_user_result[userid].append(data)
                
                table_id = result[0]
                if userid not in map_userid_tableid:
                    map_userid_tableid[userid] = table_id
                   
                if userid not in old_userid_result:
                    old_userid_result[userid] = 1
                else:
                    old_userid_result[userid] += 1
            # 新用户
            else:
                if userid not in new_user_result:
                    new_user_result[userid] = [data]
                else:
                    new_user_result[userid].append(data)
                
                if userid not in new_userid_result:
                    new_userid_result[userid] = 1
                else:
                    new_userid_result[userid] += 1
    
    # sql用于保存对应表的插入语句，values保存对应表的数据，size用于对应表的当前values大小的计数，当达到pages大小时执行sql
    sql = []
    values = []
    size = []
    
    # 初始化表
    for i in range(64):
        if i < 10:
            tbstr = "00" + str(i)
        else:
            tbstr = "0" + str(i)
        
        size.append(0)
        values.append([])
        sql.append("INSERT INTO ad_displog_local_" + tbstr + "(campaign_id,subscribe_id,entity_id,entity_type,creative_id,user_id,online_date,local,province,local_path,pv,click,total_consume,mobile_pv,mobile_click,mobile_consume,uclick0,uconsume0,uclick1,uconsume1,coupon_consume) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)") 
        cpcstat_mysqlhelper.query("DELETE FROM ad_displog_local_" + tbstr + " WHERE online_date='" + datestr2 + "'")
    
    """
       ----------------------------------------------- 开始入老用户 ------------------------------------------------
    """
    log.info("start to commit local old user,len=" + str(len(old_userid_result)))
    for userid in old_userid_result:
        tableid = map_userid_tableid[userid]
        
        for i in old_user_result[userid]:
            local = str(i[6])
            province = 0
            local_path = 0
            if local != "0":
                local_path = local_map[local]
                city = local_path.split(",")[0]
                if city in city_province_map:
                    cmc_province = city_province_map[city]
                    if cmc_province in province_map:
                        province = province_map[cmc_province]
                else:
                    # 余姚、改则等地区在cmc文件中没有，去promotion库的t_area_config找
                    if city in area_map:
                        province = area_map[city]
                
            values[tableid].append((i[0],i[1],i[2],i[3],i[4],userid,i[5],i[6],province,local_path,i[7],i[8],i[9],i[10],i[11],i[12],i[13],i[14],i[15],i[16],i[17]))
            size[tableid] += 1
            if size[tableid] == PAGE_SIZE:
                cpcstat_mysqlhelper.executemany(sql[tableid], values[tableid])
                log.info("ad_displog_local tableId=%s size=%d" % (str(tableid), size[tableid]))
                
                # 重置计数和数组
                values[tableid] = []
                size[tableid] = 0
                
                
    log.info("executemany ad_displog_local final old user data start...")
    for tableid in range(64):
        if size[tableid] > 0:
            cpcstat_mysqlhelper.executemany(sql[tableid], values[tableid])
            log.info("tableId=%d" % (tableid))
            values[tableid] = []
            size[tableid] = 0
       
    """
       -----------------------------开始入新用户，统计每张表的数据量，统计每个新用户的数据量，大用户先入小表----------------------------
    """
    table_result = {}
    for tableid in range(64):
        if tableid < 10:
            tbstr = "00" + str(tableid)
        else:
            tbstr = "0" + str(tableid)
        
        # 获取每张表的数量
        sqlstr = "SELECT COUNT(*) FROM ad_displog_local_" + tbstr
        tmp = cpcstat_mysqlhelper.queryRow(sqlstr)
        if tbstr not in table_result:
            table_result[tbstr] = tmp[0]
    
    # 表数量按升序排序
    table_result_list = sorted(table_result.iteritems(), key=lambda d:d[1], reverse=False)
    # 新用户数据量按降序排序
    new_userid_result_list = sorted(new_userid_result.iteritems(), key=lambda d:d[1], reverse=True)
    
    log.info("start to commit new user,len=" + str(len(new_userid_result_list)))
    # 开始入库，大用户入小表
    for ur in new_userid_result_list:
        userid = ur[0]
        tableid = int(table_result_list[0][0])
        tablecount = table_result_list[0][1]

        # 插入userid与tableid的映射表
        sqlstr = "INSERT INTO ad_user_map_local (user_id,table_id) VALUES (%s,%d)" % (userid,tableid)
        cpcstat_mysqlhelper.query(sqlstr)
        
        for i in new_user_result[userid]:
            local = str(i[6])
            province = 0
            local_path = 0
            if local == "0":
                log.error("local is 0, tableid=" + tbstr)
            else:
                local_path = local_map[local]
                city = local_path.split(",")[0]
                if city in city_province_map:
                    cmc_province = city_province_map[city]
                    if cmc_province in province_map:
                        province = province_map[cmc_province]
                else:
                    # 余姚、改则等地区在cmc文件中没有，去promotion库的t_area_config找
                    if city in area_map:
                        province = area_map[city]
                        
            values[tableid].append((i[0],i[1],i[2],i[3],i[4],userid,i[5],i[6],province,local_path,i[7],i[8],i[9],i[10],i[11],i[12],i[13],i[14],i[15],i[16],i[17]))
            size[tableid] += 1
            if size[tableid] == PAGE_SIZE:
                cpcstat_mysqlhelper.executemany(sql[tableid], values[tableid])
                log.info("ad_displog_local tableId=%s size=%d" % (str(tableid), size[tableid]))
                tablecount += PAGE_SIZE
                # 重置计数和数组
                values[tableid] = []
                size[tableid] = 0
        
        # 计算插入后的表大小，重新排序
        tablecount += len(values[tableid])
        # 元组不能直接修改值，所以先删后加
        del table_result_list[0]
        table_result_list.append((tableid, tablecount))
        
        table_result_list.sort(key=lambda d:d[1], reverse=False)
    
    log.info("executemany ad_displog_local final new user data start...")
    for tableid in range(64):
        if size[tableid] > 0:
            cpcstat_mysqlhelper.executemany(sql[tableid], values[tableid])
            log.info("tableId=%d" % (tableid))
            values[tableid] = []
            size[tableid] = 0
              
    cpcstat_mysqlhelper.commit()
    log.info("local committed.")
    
    # 将user_id与table_id映射表载入Redis中
    loadAdUserMapLocalToRedis(cpcstat_mysqlhelper)
    log.info("ad_user_map_local loaded in redis.")
    
    cpcstat_mysqlhelper.close()
    
    old_user_result = None
    old_userid_result = None
    new_user_result = None
    new_userid_result = None
    map_userid_tableid = None
    table_result = None
    table_result_list = None
    new_userid_result_list = None
    
    timefinish = time.time()
    timecost = timefinish - timestart
    print "insert db of local data done, time cost: " + str(timecost)
    log.info("insert db of local data done, time cost: " + str(timecost))

"""
    ***************************************** 入类目表 **********************************************
"""
def bench_insertdb_param(jzdata, datestr2):
    print "start bench_insertdb_param"
    log.info("start to insert param data, datestr2=" + datestr2)
    timestart = time.time()

    old_user_result = {}
    old_userid_result = {}
    new_user_result = {}
    new_userid_result = {}
    map_userid_tableid = {}
    
    # 获取param_path
    cmc_cate_file = CMC_CATE_PATH + "/cmcCate." + datestr2
    cate_map = getCpcCatePathMap(cmc_cate_file, {})
    
    cmc_unit_parameter_file = CMC_UNIT_PARAMETER_PATH + "/cmcUnitParameter." + datestr2
    unit_param_map = getCpcUnitParameterMap(cmc_unit_parameter_file, {})
    
    cpcstat_mysqlhelper = MySQLHelper()
    cpcstat_mysqlhelper.setcpcstatconn()
    
    for key in jzdata.keys():
        keys = key.split(",")
        # 只处理地区，其他的忽略
        if keys[1] == "1" or keys[1] == "2" or keys[1] == "4":
            continue
        
        userid = int(keys[2])
        pv = jzdata[key]['pv0'] + jzdata[key]['pv1']
        click = jzdata[key]['click0'] + jzdata[key]['click1']
        consume = jzdata[key]['consume0'] + jzdata[key]['consume1']
        coupon_consume = jzdata[key]['coupon_consume0'] + jzdata[key]['coupon_consume1']
       
        param = keys[9]
        if param == "-" or param.startswith("c"):
            param = "0"
        
        data = [keys[3], keys[4], keys[5], keys[6], keys[7], keys[8], param, pv, click, consume, jzdata[key]['pv1'], jzdata[key]['click1'], jzdata[key]['consume1'], jzdata[key]['uclick0'], jzdata[key]['uconsume0'], jzdata[key]['uclick1'], jzdata[key]['uconsume1'], coupon_consume]
        
        # 如果map_userid_tableid中存在userid,则为老用户
        if userid in map_userid_tableid:
            old_user_result[userid].append(data)
            old_userid_result[userid] += 1
        else:
            sql = "SELECT table_id FROM ad_user_map_param WHERE user_id=" + str(userid)
            result = cpcstat_mysqlhelper.queryRow(sql)
            # 老用户
            if result != None:
                if userid not in old_user_result:
                    old_user_result[userid] = [data]
                else:
                    old_user_result[userid].append(data)
                
                table_id = result[0]
                if userid not in map_userid_tableid:
                    map_userid_tableid[userid] = table_id
                   
                if userid not in old_userid_result:
                    old_userid_result[userid] = 1
                else:
                    old_userid_result[userid] += 1
            # 新用户
            else:
                if userid not in new_user_result:
                    new_user_result[userid] = [data]
                else:
                    new_user_result[userid].append(data)
                
                if userid not in new_userid_result:
                    new_userid_result[userid] = 1
                else:
                    new_userid_result[userid] += 1
    
    # sql用于保存对应表的插入语句，values保存对应表的数据，size用于对应表的当前values大小的计数，当达到pages大小时执行sql
    sql = []
    values = []
    size = []
    
    # 初始化表
    for i in range(64):
        if i < 10:
            tbstr = "00" + str(i)
        else:
            tbstr = "0" + str(i)
        size.append(0)
        values.append([])
        sql.append("INSERT INTO ad_displog_param_" + tbstr + "(campaign_id,subscribe_id,entity_id,entity_type,creative_id,user_id,online_date,param,param_path,pv,click,total_consume,mobile_pv,mobile_click,mobile_consume,uclick0,uconsume0,uclick1,uconsume1,coupon_consume) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")    
        cpcstat_mysqlhelper.query("DELETE FROM ad_displog_param_" + tbstr + " WHERE online_date='" + datestr2 + "'")
    
    promotion_mysqlhelper = MySQLHelper()
    promotion_mysqlhelper.setpromotionconn()
    
    """
       ----------------------------------------------- 开始入老用户 ------------------------------------------------
    """
    log.info("start to commit old user,len=" + str(len(old_userid_result)))
    for userid in old_userid_result:
        tableid = map_userid_tableid[userid]
        
        for i in old_user_result[userid]:
            param = i[6]
            cate_path = "0"
            
            # param=0表示广告展现在二级类首页，需subscribe表中获取对应的channel_id即为二级类id
            if param == "0" or "f" in param:
                subtable_id = (userid % 10000) / 1250
                p_sql = "SELECT channel_id FROM subscribe_" + str(subtable_id) + " WHERE subscribe_id=" + str(i[1])
                p_result = promotion_mysqlhelper.queryRow(p_sql)
                if p_result != None:
                    channel_id= str(p_result[0])
                    if channel_id == "0":
                        log.info("channel_id is 0, subscribe_id=" + str(i[1]))
                    else:
                        cate_path = cate_map[channel_id]
                else:
                    log.info("channel_id is None, subscribe_id=" + str(i[1]))
            
            elif "t" in param:
                cate_path = cate_map[param[1:]]
            else:
                cate_id = ""
                if "v" in param:
                    paramId = param[0:param.index("v")]
                    cate_id = unit_param_map[paramId]
                else:
                    cate_id = unit_param_map[param]
                    
                key = str(cate_id) + "_1"
                disp_cate = cate_map[key]
                cate_path = cate_map[disp_cate]
            
            values[tableid].append((i[0],i[1],i[2],i[3],i[4],userid,i[5],i[6],cate_path,i[7],i[8],i[9],i[10],i[11],i[12],i[13],i[14],i[15],i[16],i[17]))
            size[tableid] += 1
            if size[tableid] == PAGE_SIZE:
                cpcstat_mysqlhelper.executemany(sql[tableid], values[tableid])
                log.info("ad_displog_param tableId=%d size=%d" % (tableid, size[tableid]))
                
                # 重置计数和数组
                values[tableid] = []
                size[tableid] = 0
                
                
    log.info("executemany ad_displog_param final old user data start...")
    for tableid in range(64):
        if size[tableid] > 0:
            cpcstat_mysqlhelper.executemany(sql[tableid], values[tableid])
            log.info("tableId=%d" % (tableid))
            values[tableid] = []
            size[tableid] = 0
        
    """
       ----------------------------------------------- 开始入新用户 ------------------------------------------------
    """
    table_result = {}
    for j in range(64):
        if j < 10:
            tbstr = "00" + str(j)
        else:
            tbstr = "0" + str(j)
        
        # 获取每张表的数量
        sqlstr = "SELECT COUNT(*) FROM ad_displog_param_" + tbstr
        tmp = cpcstat_mysqlhelper.queryRow(sqlstr)
        if tbstr not in table_result:
            table_result[tbstr] = tmp[0]
    
    # 表数量按升序排序
    table_result_list = sorted(table_result.iteritems(), key=lambda d:d[1], reverse=False)
    # 新用户数据量按降序排序
    new_userid_result_list = sorted(new_userid_result.iteritems(), key=lambda d:d[1], reverse=True)
    
    # 开始入库，大用户入小表
    log.info("start to commit new user,len=" + str(len(new_userid_result_list)))
    for ur in new_userid_result_list:
        userid = ur[0]
        tableid = int(table_result_list[0][0])
        tablecount = table_result_list[0][1]
        
        # 插入userid与tableid的映射表
        sqlstr = "INSERT INTO ad_user_map_param (user_id,table_id) VALUES (%s,%d)" % (userid,tableid)
        cpcstat_mysqlhelper.query(sqlstr)
        
        for i in new_user_result[userid]:
            param = i[6]
            cate_path = "0"
            
            # param=0表示广告展现在二级类首页，需subscribe表中获取对应的channel_id即为二级类id
            if param == "0" or "f" in param:
                subtable_id = (userid % 10000) / 1250
                p_sql = "SELECT channel_id FROM subscribe_" + str(subtable_id) + " WHERE subscribe_id=" + str(i[1])
                p_result = promotion_mysqlhelper.queryRow(p_sql)
                if p_result != None:
                    channel_id= str(p_result[0])
                    if channel_id == "0":
                        log.info("channel_id is 0, subscribe_id=" + str(i[1]))
                    else:
                        cate_path = cate_map[channel_id]
                else:
                    log.info("channel_id is None, subscribe_id=" + str(i[1]))
                    
            elif "t" in param:
                cate_path = cate_map[param[1:]]
            else:
                cate_id = ""
                if "v" in param:
                    paramId = param[0:param.index("v")]
                    cate_id = unit_param_map[paramId]
                else:
                    cate_id = unit_param_map[param]
                    
                key = str(cate_id) + "_1"
                disp_cate = cate_map[key]
                cate_path = cate_map[disp_cate]
            
            values[tableid].append((i[0],i[1],i[2],i[3],i[4],userid,i[5],i[6],cate_path,i[7],i[8],i[9],i[10],i[11],i[12],i[13],i[14],i[15],i[16],i[17]))
            size[tableid] += 1
            if size[tableid] == PAGE_SIZE:
                cpcstat_mysqlhelper.executemany(sql[tableid], values[tableid])
                log.info("ad_displog_param tableId=%d size=%d" % (tableid, size[tableid]))
                tablecount += PAGE_SIZE
                # 重置计数和数组
                values[tableid] = []
                size[tableid] = 0
        
        # 计算插入后的表大小，重新排序
        tablecount += len(values[tableid])
        # 元组不能直接修改值，所以先删后加
        del table_result_list[0]
        table_result_list.append((tableid, tablecount))
        
        table_result_list.sort(key=lambda d:d[1], reverse=False)
    
    log.info("executemany ad_displog_param final new user data start...")
    for tableid in range(64):
        if size[tableid] > 0:
            cpcstat_mysqlhelper.executemany(sql[tableid], values[tableid])
            log.info("tableId=%d" % (tableid))
            values[tableid] = []
            size[tableid] = 0
               
    cpcstat_mysqlhelper.commit()
    log.info("param committed.") 
    
    # 将user_id与table_id映射表载入Redis中
    loadAdUserMapParamToRedis(cpcstat_mysqlhelper)
    log.info("ad_user_map_param loaded in redis.")
    
    cpcstat_mysqlhelper.close()
    promotion_mysqlhelper.close()
    
    old_user_result = None
    old_userid_result = None
    new_user_result = None
    new_userid_result = None
    map_userid_tableid = None
    table_result = None
    table_result_list = None
    new_userid_result_list = None
    
    timefinish = time.time()
    timecost = timefinish - timestart
    print "insert db of param data done, time cost: " + str(timecost)
    log.info("insert db of param data done, time cost: " + str(timecost))

"""
    存储<DispLcoalID,FullPath>
"""
def getCpcLocalPathMap(cmcLocalfile, rs):
    if not os.path.exists(cmcLocalfile):
        print "cmcLocalfile not exist! cmcLocalfile: %s" % cmcLocalfile
        return
    
    fr = open(cmcLocalfile, "r")
    key = ""
    
    for line in fr.readlines():
        logs = line.strip().split("\t")
        key = logs[0]
        if key not in rs:
            rs[key] = logs[4]
            
    fr.close()
    return rs

def getCpcAreaConfigMap(promotion_mysqlhelper, rs):
    sql = "SELECT pid,area_id FROM t_area_config WHERE `level`=3" 
    result = promotion_mysqlhelper.queryAll(sql)
    if result != None:
        for i in result:
            pid = i["pid"]
            area_id = i["area_id"]
            if area_id not in rs:
                rs[area_id] = pid
    
    return rs
                
def getCpcCityProvinceMap(cmcCityProvinceFile, rs):
    if not os.path.exists(cmcCityProvinceFile):
        print "cmcCityProvinceFile not exist! cmcCityProvinceFile: %s" % cmcCityProvinceFile
        return
    
    fr = open(cmcCityProvinceFile, "r")
    key = ""
    
    for line in fr.readlines():
        logs = line.strip().split("\t")
        key = logs[1]
        if key not in rs:
            rs[key] = logs[2]
            
    fr.close()
    return rs
    

def getCpcProvince(cmcProvinceFile, promotion_mysqlhelper, rs):
    if not os.path.exists(cmcProvinceFile):
        print "cmcProvinceFile not exist! cmcProvinceFile: %s" % cmcProvinceFile
        return
    
    fr = open(cmcProvinceFile, "r")
    key = ""
    
    for line in fr.readlines():
        logs = line.strip().split("\t")
        key = logs[0]

        nameLen = len(logs[1])
        if nameLen == 9:
            name = logs[1][0:6]
        elif nameLen == 12:
            name = logs[1][0:9]
        elif nameLen > 12:
            if "内蒙古" in logs[1]:
                name = logs[1][0:9]
            else:
                name = logs[1][0:6]
        else:
            name = logs[1]
            
         # 处理直辖市
        if name == "北京":
            if key not in rs:
                rs[key] = "1"
                continue
        elif name == "上海":
            if key not in rs:
                rs[key] = "2"
                continue
        elif name == "天津":
            if key not in rs:
                rs[key] = "18"
                continue
        elif name == "重庆":
            if key not in rs:
                rs[key] = "37"
                continue
        elif name == "香港":
            if key not in rs:
                rs[key] = "2050"
                continue
        elif name == "澳门":
            if key not in rs:
                rs[key] = "9399"
                continue
        elif name == "台湾":
            if key not in rs:
                rs[key] = "2051"
                continue
            
        sql = "SELECT area_id FROM t_area_config WHERE `name`='" + name + "' AND level=2"
        result = promotion_mysqlhelper.queryRow(sql)
        if result != None:
            area_id = str(result[0])
            if key not in rs:
                rs[key] = area_id
            
    fr.close()
    return rs

"""
    存储 <DispCategoryID,FullPath>
"""
def getCpcCatePathMap(cmcCateFile, rs):
    if not os.path.exists(cmcCateFile):
        print "cateCateFile not exist! cateCateFile: %s" % cateCateFile
        return
    
    fr = open(cmcCateFile, "r")
    key = ""
    
    for line in fr.readlines():
        logs = line.strip().split("\t")
        key = logs[0]
        if key not in rs:
            rs[key] = logs[5]
           
        # 存放二级类,"<cateID_depth, dispCategory>"，用以单元参数通过cateID能找到对应的二级类
        depth = str(logs[4])
        if depth == "1":
            key = str(logs[7]) + "_" + depth
            if key not in rs:
                rs[key] = logs[0]
            
    fr.close()
    return rs

"""
    存储<ParameterID, CateID>
"""
def getCpcUnitParameterMap(cmcUnitParameterFile, rs):
    if not os.path.exists(cmcUnitParameterFile):
        print "cmcUnitParameterFile not exist! cmcUnitParameterFile: %s" % cmcUnitParameterFile
        return
    
    fr = open(cmcUnitParameterFile, "r")
    key = ""
    
    for line in fr.readlines():
        logs = line.strip().split("\t")
        key = logs[0]
        if key not in rs:
            rs[key] = logs[2]
            
    fr.close()
    return rs

def loadAdUserMapLocalToRedis(cpcstat_mysqlhelper):
    print "start to save ad_user_map_local data to redis."
    
    redis_cli = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD, db=REDIS_DB)
    pipeline = redis_cli.pipeline()
    
    sql = "SELECT * FROM ad_user_map_local"
    result = cpcstat_mysqlhelper.queryAll(sql)
    print "ad_user_map_local count:" + str(len(result))
    
    if result == None:
        return
    
    for i in result:
        key = "l_" + i["user_id"]
        pipeline.set(key, i["table_id"])
    
    pipeline.execute()
    print "finished to save ad_user_map_local data to redis."
        
def loadAdUserMapParamToRedis(cpcstat_mysqlhelper):
    print "start to save ad_user_map_param data to redis."
    
    redis_cli = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD, db=REDIS_DB)
    pipeline = redis_cli.pipeline()
    
    sql = "SELECT * FROM ad_user_map_param"
    result = cpcstat_mysqlhelper.queryAll(sql)
    print "ad_user_map_param count:" + str(len(result))
    
    if result == None:
        return
    
    for i in result:
        key = "p_" + i["user_id"]
        pipeline.set(key, i["table_id"])
        
    pipeline.execute()
    print "finished to save ad_user_map_param data to redis."

if __name__ == "__main__":
    if(len(sys.argv) > 1):
        datestr2 = sys.argv[1]
    else:
        mytime = time.time() - (3600 * 24)
        datestr2 = time.strftime("%Y%m%d", time.localtime(mytime))
        
    if(len(sys.argv) > 2):	
        from  conf.offline.MySQLHelper import  MySQLHelper
        handler = logging.FileHandler(ROOT_PATH + "/log/cpc_file2mysql_cpcstat.log.offline." + datestr2)
        fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        log.addHandler(handler)
        log.setLevel(logging.NOTSET)
        log.info("offline")
    else:							
        from  conf.online.MySQLHelper import  MySQLHelper
        handler = logging.FileHandler(ROOT_PATH + "/log/cpc_file2mysql_cpcstat.log.online." + datestr2)
        fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'			
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        log.addHandler(handler)
        log.setLevel(logging.NOTSET)
        log.info("online")
    
    dispfile = ROOT_PATH + "/data/jzdisphourmysqlNew/jzdisphourmysql-" + datestr2
    clickfile = ROOT_PATH + "/data/jzclickhourmysqlNew/jzclickhourmysql-" + datestr2
    
    cpc_wget_file.get_cpc_log("/home/hdp_lbg_ectech/resultdata/tuiguang/jzcpc/jzdispdaymysqlNew/v1/" + datestr2 + "/part-r-00000", dispfile)
    cpc_wget_file.get_cpc_log("/home/hdp_lbg_ectech/resultdata/tuiguang/jzcpc/jzclickdaymysqlNew/v1/" + datestr2 + "/part-r-00000", clickfile)
    startimport(datestr2)
