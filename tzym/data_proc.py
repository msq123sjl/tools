#! /usr/bin/python
#-*-coding:utf-8 -*-

'''
Author: msq
Date: 20180126
Description:
    1、yum install python-devel.x86_64
    2、yum install gcc-c++
    3、install pysubnettree-0.26.tar.gz.       python setup.py install
    4、python ./outnetreplace.py 待处理日志目录 生成目标日志目录
'''
from logging.handlers import RotatingFileHandler
import SubnetTree
import socket
import os
import os.path
import Queue
from multiprocessing import Pool, TimeoutError
import time
import gzip
import re
import sys
import ConfigParser
import time
import random
import logging

   
script_path = sys.path[0]
config_file = "%s/%s" % (script_path, sys.argv[1])
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append("%s/common_help/" % script_path)
import uspeed_py_common_helper as uhelp
   
global Sqlite_in
global Sqlite
global logging


global sqlite_insertcount
sqlite_insertcount = 0


#global hlt_web_top_yc_dict
#hlt_web_top_yc_dict = dict()
def close_sqlite():
    global Sqlite_in
    Sqlite_in.commit()
    logging.info("sqlite插入[%d] 条数据" % sqlite_insertcount)
    Sqlite_in.close()
    logging.info("关闭Sqlite数据库")
    logging.info("退出")


def insert_sqlite(table, sql):
    global sqlite_insertcount

    logging.debug("insert into %s values(%s);" % (table, sql))
    Sqlite_in.insert(table, sql)
    sqlite_insertcount += 1
    
    if sqlite_insertcount >= commitcount:
        logging.info("插入[%d] 条数据" % sqlite_insertcount)
        Sqlite_in.commit()
        sqlite_insertcount = 0

    
def clean_sqlite_hour_data(Sqlite_in):
    Sqlite_in.delete1('Hour_w00000', "GetTime >= '%s' and GetTime <= '%s'" % (starttime, endtime))    
    Sqlite_in.commit()    

def clean_sqlite_day_data(Sqlite_in):
    Sqlite_in.delete1('Day_w00000', "GetTime >= '%s' and GetTime <= '%s'" % (day_starttime, day_endtime))    
    Sqlite_in.commit() 	
 

def check_file():

    logging.info("检查相关文件")
    
    if uhelp.file_is_readable(alldb) == False:
        print "文件[%s] 不存在或不可读" % alldb
        logging.error("文件[%s] 不存在或不可读" % alldb)
        exit(uhelp.CONFIG_ERROR)

def insert_data_hour():
    for key_a in hour_dict:
        try:
            monitor_time = hour_dict[key_a]['time']
            rtd_avg     = round(uhelp.div1(hour_dict[key_a]['RTD'][0],hour_dict[key_a]['RTD'][1]),2)
            rtd_min     = hour_dict[key_a]['RTD_MIN']
            rtd_max     = hour_dict[key_a]['RTD_MAX']
            
            last_hour   = uhelp.get_hour_lasthour(monitor_time)
            try:
                total_min   = hour_dict[last_hour]['TOTAL_MAX']
            except Exception, e:
                total_min   = hour_dict[key_a]['TOTAL_MIN']
                
            total_max   = hour_dict[key_a]['TOTAL_MAX']
            cou         = total_max - total_min
            #logging.info("last_hour[%s],hour[%s],total_min[%s],min[%s],total_max[%s]" % (last_hour,monitor_time,total_min,hour_dict[key_a]['TOTAL_MIN'],total_max))
            if cou < 0:
                cou = 0
                logging.warning("monitor_time[%s],total_min[%d],total_max[%d]" % (monitor_time,total_min,total_max))
            
            sql = uhelp.column_join(monitor_time,rtd_max,rtd_min,rtd_avg,cou)
            #logging.info("monitor_time[%s]  total_min[%s] total_max[%s]" % (monitor_time,total_min,total_max))
            if monitor_time >= starttime and monitor_time <= endtime:
                insert_sqlite('Hour_w00000', sql)
                #logging.info("Hour_w00000 数据 %s" % sql)
            else:
                logging.info("not insert [Hour_w00000 数据 %s] " % sql)
            
        except Exception, e:
            logging.warning("hour_dict monitor_time %s %s" % (key_a,e))

def insert_data_day():
    for key_a in day_dict:
        try:
            monitor_time = day_dict[key_a]['time']
            rtd_avg     = round(uhelp.div1(day_dict[key_a]['RTD'][0],day_dict[key_a]['RTD'][1]),2)
            rtd_min     = day_dict[key_a]['RTD_MIN']
            rtd_max     = day_dict[key_a]['RTD_MAX']
            
            last_day    = uhelp.get_day_Yesterday(monitor_time)
            try:
                total_min   = day_dict[last_day]['TOTAL_MAX']
            except Exception, e:
                total_min   = day_dict[key_a]['TOTAL_MIN']
                logging.warning("day %s" % (monitor_time))
                
            total_max   = day_dict[key_a]['TOTAL_MAX']
            cou         = total_max - total_min
            #logging.info("last_day[%s],day[%s],total_min[%s],min[%s],total_max[%s]" % (last_day,monitor_time,total_min,day_dict[key_a]['TOTAL_MIN'],total_max))
            if cou < 0:
                cou = 0
                logging.warning("monitor_time[%s],total_min[%d],total_max[%d]" % (monitor_time,total_min,total_max))
            
            sql = uhelp.column_join(monitor_time,rtd_max,rtd_min,rtd_avg,cou)
            #logging.info("monitor_time[%s]  total_min[%s] total_max[%s]" % (monitor_time,total_min,total_max))
            if monitor_time >= day_starttime and monitor_time <= day_endtime:
                insert_sqlite('Day_w00000', sql)
                #logging.info("Day_w00000 数据 %s" % sql)
            else:
                logging.info("not insert [Day_w00000 数据 %s] " % sql)
            
        except Exception, e:
            logging.warning("day_dict monitor_time %s %s" % (key_a,e))            
        
def calc_DATA_HOUR(cols):
    monitor_time        = cols[TIME_INDEX][:19]
    total               = float(cols[TOTAL_INDEX])
    rtd                 = float(cols[RTD_INDEX])
    if rtd < 0:
        rtd = 0
    if total > 0:
        hour                = cols[TIME_INDEX][:14] + '00:00'
        #logging.info("last_hour[%s],hour[%s],next_hour[%s],monitor_time[%s]" % (last_hour,hour,next_hour,monitor_time))
        
        uhelp.set_2xdict(hour_dict, hour, 'time', hour)
        uhelp.calc_2xdict_list2_add(hour_dict, hour, 'RTD', [rtd,1])
        uhelp.calc_2xdict_val_min(hour_dict, hour, 'RTD_MIN', rtd)
        uhelp.calc_2xdict_val_max(hour_dict, hour,'RTD_MAX',rtd)
        uhelp.calc_2xdict_val_min(hour_dict, hour, 'TOTAL_MIN', total)
        uhelp.calc_2xdict_val_max(hour_dict, hour, 'TOTAL_MAX', total)
    else:
        logging.warning("monitor_time[%s],total[%d]" % (monitor_time,total))

def calc_DATA_DAY(cols):
    monitor_time        = cols[TIME_INDEX][:19]
    total               = float(cols[TOTAL_INDEX])
    rtd                 = float(cols[RTD_INDEX])
    if rtd < 0:
        rtd = 0
    if total > 0:
        day                = cols[TIME_INDEX][:11] + '00:00:00'
        #logging.info("last_day[%s],hour[%s],next_day[%s],monitor_time[%s]" % (last_day,hour,next_day,monitor_time))
        
        uhelp.set_2xdict(day_dict, day, 'time', day)
        uhelp.calc_2xdict_list2_add(day_dict, day, 'RTD', [rtd,1])
        uhelp.calc_2xdict_val_min(day_dict, day, 'RTD_MIN', rtd)
        uhelp.calc_2xdict_val_max(day_dict, day,'RTD_MAX',rtd)
        uhelp.calc_2xdict_val_min(day_dict, day, 'TOTAL_MIN', total)
        uhelp.calc_2xdict_val_max(day_dict, day, 'TOTAL_MAX', total)
    else:
        logging.warning("monitor_time[%s],total[%d]" % (monitor_time,total))        
    
def read_log():
    #files = []
    file_cnt = 0
    for parent, dirname, filenames in os.walk(logdir):
        for filename in filenames:
            #files.append(os.path.join(parent, filename))
            file = os.path.join(parent, filename)
            #for file in files:
            file_cnt += 1
            logging.info("%s: processing %s start" % (time.strftime("%a %b %d %H:%M:%S %Y", time.localtime()), file))
            if file.endswith(".gz"):
                f = gzip.open(file,'r')
            else:
                f = open(file, 'r')
            line_cnt = 0
            for line in f:
            
                try:
                    line = line.encode("utf-8")
                except Exception:
                    line = line.decode("gbk").encode("utf-8")
                    
                line_cnt += 1
                #跳过标题行
                if line_cnt == 1 and title != 0:
                    continue
                    
                line=line.strip()
                cols = line.split(separate);
                if (len(cols) != column):
                    logging.warning("列数不足 ,%d 列,忽略",len(cols))
                    continue
                try:    
                    monitor_time        = cols[TIME_INDEX][:19]
                    struct_time         = time.strptime(monitor_time, "%Y-%m-%d %H:%M:%S")
       
                    total               = float(cols[TOTAL_INDEX])
                    rtd                 = float(cols[RTD_INDEX])
                    
                except Exception, e:
                    logging.warning("日志 %s,  %d 行,数据错误,忽略" % (file,line_cnt))
                    continue
                
                calc_DATA_HOUR(cols)
                calc_DATA_DAY(cols)
                #calc_OV_FLOW_THIRD_D(cols)
    logging.info("处理日志数 %d",file_cnt)
        
config = ConfigParser.ConfigParser()
config.read(config_file)      
try:   
    starttime       = config.get("global", "starttime")
    endtime         = config.get("global", "endtime")
    day_starttime   = config.get("global", "day_starttime")
    day_endtime     = config.get("global", "day_endtime")
    
    alldb           = config.get("global", "alldb")
    logdir          = config.get("global", "logdir")
    
    separate        = config.get("rtdlog", "separate")             
    title           = config.getint("rtdlog", "title")
    column          = config.getint("rtdlog", "column")
    TIME_INDEX      = config.getint("rtdlog", "监测时间")
    TOTAL_INDEX     = config.getint("rtdlog", "表显累计量")
    RTD_INDEX       = config.getint("rtdlog", "RTD")

    commitcount     = 20000
    
    log_level       = config.get("log", "level")
    log_name        = config.get("log", "path")
    log_size        = config.get("log", "size")
 
    
except Exception, e:
    print e
    exit(1)

#iprex     = re.compile(r'^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$')
            
if __name__ == "__main__":
    
    logging = uhelp.init_log(log_name, log_level, log_size)
    logging.info("开始工作")    

    check_file()
    
    Sqlite_in = uhelp.SqliteHelper(alldb)
    logging.info("连接Sqlite数据库")
    
    #clean_day_data(Mysql, statdate)
    clean_sqlite_hour_data(Sqlite_in)
    clean_sqlite_day_data(Sqlite_in)
    logging.info("删除数据")
    
    hour_dict = dict()
    day_dict  = dict()
    read_log()
    insert_data_hour()
    insert_data_day()
    close_sqlite()
    
    logging.info("exiting Main Thread")