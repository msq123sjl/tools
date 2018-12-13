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
    Sqlite_in.delete1('Hour_w01018', "GetTime >= '%s' and GetTime <= '%s'" % (starttime, endtime)) 
    Sqlite_in.delete1('Hour_w21003', "GetTime >= '%s' and GetTime <= '%s'" % (starttime, endtime))
    Sqlite_in.delete1('Hour_w01001', "GetTime >= '%s' and GetTime <= '%s'" % (starttime, endtime))
    Sqlite_in.delete1('Hour_w01012', "GetTime >= '%s' and GetTime <= '%s'" % (starttime, endtime))    
    Sqlite_in.commit()    

def clean_sqlite_day_data(Sqlite_in):  
    Sqlite_in.delete1('Day_w01018', "GetTime >= '%s' and GetTime <= '%s'" % (day_starttime, day_endtime)) 
    Sqlite_in.delete1('Day_w21003', "GetTime >= '%s' and GetTime <= '%s'" % (day_starttime, day_endtime))
    Sqlite_in.delete1('Day_w01001', "GetTime >= '%s' and GetTime <= '%s'" % (day_starttime, day_endtime))
    Sqlite_in.delete1('Day_w01012', "GetTime >= '%s' and GetTime <= '%s'" % (day_starttime, day_endtime))     
    Sqlite_in.commit() 	
 

def check_file():

    logging.info("检查相关文件")
    
    if uhelp.file_is_readable(alldb) == False:
        print "文件[%s] 不存在或不可读" % alldb
        logging.error("文件[%s] 不存在或不可读" % alldb)
        exit(uhelp.CONFIG_ERROR)          
        
def calc_DATA_HOUR(cols):
    monitor_time        = cols[TIME_INDEX][:19]
    wartercou           = float(cols[WARTERCOU_INDEX])
    id                  = int(cols[ID_INDEX])
    min                 = float(cols[MIN_INDEX])
    avg                 = float(cols[AVG_INDEX])
    max                 = float(cols[MAX_INDEX])
    if min < 0:
        min = 0
    if avg < 0:
        avg = 0
    if max < 0:
        max = 0
    if wartercou < 0:
        wartercou = 0
    cou  = round(wartercou*avg*0.001,2)
    hour = cols[TIME_INDEX][:14] + '00:00'
    sql = uhelp.column_join(hour,max,min,avg,cou)
    #logging.info("monitor_time[%s]  total_min[%s] total_max[%s]" % (monitor_time,total_min,total_max))
    if monitor_time >= starttime and monitor_time <= endtime:
        if id == 2:
            #cod
            insert_sqlite('Hour_w01018', sql)
        elif id == 3:
            #氨氮
            insert_sqlite('Hour_w21003', sql)
        elif id == 4:
            #PH值
            insert_sqlite('Hour_w01001', sql)
        elif id == 5:
            #悬浮物
            insert_sqlite('Hour_w01012', sql)
        else:
            logging.info("Hour id 数据 %s" % sql)
    else:
        logging.info("not insert [hour 数据 %s] " % sql)

def calc_DATA_DAY(cols):
    monitor_time        = cols[TIME_INDEX][:19]
    wartercou           = float(cols[WARTERCOU_INDEX])
    id                  = int(cols[ID_INDEX])
    min                 = float(cols[MIN_INDEX])
    avg                 = float(cols[AVG_INDEX])
    max                 = float(cols[MAX_INDEX])
    if min < 0:
        min = 0
    if avg < 0:
        avg = 0
    if max < 0:
        max = 0
    if wartercou < 0:
        wartercou = 0
    cou  = round(wartercou*avg*0.001,2)
    hour = cols[TIME_INDEX][:11] + '00:00:00'
    sql = uhelp.column_join(hour,max,min,avg,cou)
    #logging.info("monitor_time[%s]  total_min[%s] total_max[%s]" % (monitor_time,total_min,total_max))
    if monitor_time >= day_starttime and monitor_time <= day_endtime:
        if id == 2:
            #cod
            insert_sqlite('Day_w01018', sql)
        elif id == 3:
            #氨氮
            insert_sqlite('Day_w21003', sql)
        elif id == 4:
            #PH值
            insert_sqlite('Day_w01001', sql)
        elif id == 5:
            #悬浮物
            insert_sqlite('Day_w01012', sql)
        else:
            logging.info("Day id 数据 %s" % sql)
    else:
        logging.info("not insert [Day 数据 %s] " % sql)
            
    
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
       
                    wartercou           = float(cols[WARTERCOU_INDEX])
                    id                  = int(cols[ID_INDEX])
                    min                 = float(cols[MIN_INDEX])
                    avg                 = float(cols[AVG_INDEX])
                    max                 = float(cols[MAX_INDEX])
                    
                except Exception, e:
                    logging.warning("日志 %s,  %d 行,数据错误,忽略" % (file,line_cnt))
                    continue
                
                calc_DATA_HOUR(cols)
                #calc_DATA_DAY(cols)
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
    WARTERCOU_INDEX = config.getint("rtdlog", "wartercou")
    ID_INDEX        = config.getint("rtdlog", "污染物编码ID")
    #COU_INDEX       = config.getint("rtdlog", "COU")
    MIN_INDEX       = config.getint("rtdlog", "MIN")
    AVG_INDEX       = config.getint("rtdlog", "AVG")
    MAX_INDEX       = config.getint("rtdlog", "MAX")

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
    #clean_sqlite_day_data(Sqlite_in)
    logging.info("删除数据")
    
    read_log()
    close_sqlite()
    
    logging.info("exiting Main Thread")