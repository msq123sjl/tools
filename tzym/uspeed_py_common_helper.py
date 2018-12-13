#! /usr/bin/python
#-*-coding:utf-8 -*-

'''
Author: Chu Xiaoqiang
Date: 20170321
Description:
    公共函数
'''

from __future__ import division
from logging.handlers import RotatingFileHandler

import commands
import ConfigParser
import os
import sys 
import logging 
import socket
import time
import sqlite3
import MySQLdb
import chardet
import datetime
import calendar
from decimal import Decimal

script_path = sys.path[0]

reload(sys)
sys.setdefaultencoding('utf-8')

#错误代码，如果需要新增，请勿覆盖原有错误代码，保证错误代码一致性
EXIT_SUCCESS = 0
CONFIG_ERROR = 1
SQLITE_ERROR = 2
MYSQL_ERROR  = 3

#使得 sqlite 数据库查询结果为字典格式
#官方文档地址 https://docs.python.org/2/library/sqlite3.html#sqlite3.Connection.row_factory
def dict_factory(cursor, row):  
    d = {}
    for idx, col in enumerate(cursor.description):  
        d[col[0]] = row[idx]  
    return d

def load_db_result_as_dict(dbpath, sql):
    sqlite = SqliteHelper(dbpath)
    dbdict = sqlite.select(sql)
    dbdict.close()
    return dbdict
    
def load_sqlite_result_as_str(path, sql):
    row = ""
    con = sqlite3.connect(path)
    cursor = con.execute(sql)
    for row in cursor:
        pass
    con.close()
    return row

#计算除法 分子/分母
def div(num1, num2):
    if num2 == 0:
        return 0
    else:
        return (float(num1)/float(num2))

#计算除法 分子/分母
def div1(num1, num2):
    if num1 < 0 or num2 <= 0:
        return 0
    else:
        return (float(num1)/float(num2))
        
def find_str(strshort, strlong):
    if -1 != strlong.find(strshort):
        return True
    else:
        return False

class MySQLHelper:
    def __init__(self, host, port, user, password, charset):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.charset = charset
        try:  
            self.conn = MySQLdb.connect(host = self.host, port = self.port, user = self.user, passwd = self.password)  
            self.conn.set_character_set(self.charset)
            self.cur = self.conn.cursor()
        except MySQLdb.Error as e:
            logging.error("Mysql 错误 : %s" % (e.args[0]))
            print("Mysql 错误 : %s" % (e.args[0]))
            exit(MYSQL_ERROR)

    def useDb(self,db):
        try:  
            self.conn.select_db(db)  
        except MySQLdb.Error as e:
            logging.error("Mysql 错误 %d: %s" % (e.args[0], e.args[1]))
            print("Mysql 错误 %d: %s" % (e.args[0], e.args[1]))
            exit(MYSQL_ERROR)
    
    def delete(self, table_name, condition):
        try:  
            n = self.cur.execute("delete from %s where %s;" % (table_name, condition))
            return n  
        except MySQLdb.Error as e:
            logging.error("Mysql 错误 %d: %s" % (e.args[0], e.args[1]))
            print("Mysql 错误 %d: %s" % (e.args[0], e.args[1]))
            exit(MYSQL_ERROR)
    
    def execute(self, sql):
        try:
            n = self.cur.execute(sql)
            return n  
        except MySQLdb.Error as e:
            logging.error("Mysql 错误:%s\nSQL:%s" %(e, sql))
            print("Mysql 错误:%s\nSQL:%s" %(e, sql))
        
    def select(self, sql):
        self.cur.execute(sql)
        return self.cur.fetchall()
    
    def replace(self, p_table_name, p_data):
        real_sql = "REPLACE INTO " + p_table_name + " VALUES (" + p_data + ")"
        return self.execute(real_sql)
        
    def insert(self, p_table_name, p_data):
        real_sql = "INSERT INTO " + p_table_name + " VALUES (" + p_data + ")"
        return self.execute(real_sql)
        
    def commit(self):  
        self.conn.commit()
  
    def close(self):  
        self.cur.close()  
        self.conn.close()

class SqliteHelper:
    def __init__(self, dbname): 
        self.name = dbname
        self.commit_nb = 0
        try:  
            self.conn = sqlite3.connect(self.name)
            self.conn.row_factory = dict_factory
            self.cur = self.conn.cursor()
        except sqlite3.Error as e: 
            print e
            logging.error("Sqlite3 错误 : %s" % (e.args[0]))
            print("Sqlite3 错误 : %s" % (e.args[0]))
            exit(SQLITE_ERROR)
        
    def execute(self, sql):
        try:  
            n = self.cur.execute(sql)
            return n
        except sqlite3.Error as e:  
            logging.error("Sqlite3 错误:%s\nSQL:%s" %(e,sql))
            print("Sqlite3 错误:%s\nSQL:%s" %(e,sql))

    def selectone(self, stmt):
        try:
            self.cur.execute(stmt)
            row = self.cur.fetchone()
        except Exception, e:
            logging.error("执行查询语句[%s]失败[%s]" %(stmt, e))
            print("执行查询语句[%s]失败[%s]" %(stmt, e))
            return None
        return row
        
    def delete(self, table_name):
        try:  
            n = self.cur.execute("delete from %s;" % (table_name))
            return n  
        except MySQLdb.Error as e:
            logging.error("sqlite 错误 %d: %s" % (e.args[0], e.args[1]))
            print("sqlite 错误 %d: %s" % (e.args[0], e.args[1]))
            exit(SQLITE_ERROR)
    
    def delete1(self, table_name, condition):
        try:  
            n = self.cur.execute("delete from %s where %s;" % (table_name, condition))
            return n  
        except MySQLdb.Error as e:
            logging.error("sqlite 错误 %d: %s" % (e.args[0], e.args[1]))
            print("sqlite 错误 %d: %s" % (e.args[0], e.args[1]))
            exit(SQLITE_ERROR)  
            
    def insert(self, table_name, data):
        sql = "INSERT INTO " + table_name + " VALUES (" + data + ");"  
        self.commit_nb += 1
        return self.execute(sql)
    
    def select(self, sql):
        self.execute(sql)
        dbdict = dict()
        try:
            dbdict = self.cur.fetchall()
        #except Exception:
        except sqlite3.Error as e:
            logging.error("Sqlite3 错误:%s\nSQL:%s" %(e,sql))
            pass
        finally:
            #self.close()
            return dbdict
            
    def commit(self):  
        self.conn.commit()
        self.commit_nb = 0
        
    def close(self):  
        self.cur.close()  
        self.conn.close()  

#将两个字典相同的key相加
def dict_plus(dict1, dict2, key):
    if dict1.has_key(key):
        dict1[key] += dict2[key]
    else:
        dict1[key] = dict2[key]    

#判断文件是不是utf-8格式
def file_is_utf8(path):
    f = file(path, "r")
    data = f.read(2000)
    if chardet.detect(data)['encoding'] == "utf-8":
        return True
    else:
        return False

#判断文件是否存在并且是否可读
def file_is_readable(path):
    if os.access(path, os.F_OK) and os.access(path, os.R_OK):
        return True

    return False

def init_log(log_name, log_level, log_size):

    # 设置文件级别
    level = {
        'CRITICAL': logging.CRITICAL,
        'ERROR'   : logging.ERROR,
        'WARNING' : logging.WARNING,
        'INFO'    : logging.INFO,
        'DEBUG'   : logging.DEBUG,
        'NOTSET'  : logging.NOTSET
    }[log_level]

    # 设置日志最大大小
    try:
        if 'K' == log_size[-1]:
            size = int(log_size[:-1]) * 1024
        elif 'M' == log_size[-1]:
            size = int(log_size[:-1]) * 1024 * 1024
        else:
            size = int(log_size)
    except Exception, e:
        print "get log size error"
        exit(1)

    logger = logging.getLogger(__name__)
    logger.setLevel(level)

    handler = logging.handlers.RotatingFileHandler(log_name, maxBytes = size, backupCount = 5)
    formatter = logging.Formatter('[%(asctime)s %(process)d %(threadName)s] [%(levelname)s] : %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

def load_conf(conf_file):
    
    if file_is_readable(conf_file) == False:
        print "配置文件[%s] 不存在或不可读" % conf_file
        return None

    config = ConfigParser.ConfigParser()
    config.read(conf_file)
    return config

def column_join(*tupleArg):

    sql = ""
    for element in tupleArg:
        #print type(element)
        if isinstance(element, int):
            sql += "%d," % element
        elif isinstance(element, str) or isinstance(element, unicode):
            sql += ("'%s'," % element)
        elif isinstance(element, float) or isinstance(element, long) or isinstance(element, Decimal):
            sql += "%.3f," % element
    return sql[:-1]

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass
 
    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
 
    return False

def get_day_month(statdate):
    month_list = list()
    today = time.strptime(statdate,'%Y%m%d')
    first_day = datetime.date(today.tm_year, today.tm_mon, 1)
    monthdays = calendar.monthrange(today.tm_year, today.tm_mon)[1] 
    last_day  = first_day + datetime.timedelta(days = monthdays - 1)   
    for day in range(int(first_day.strftime('%Y%m%d')),int(last_day.strftime('%Y%m%d')) + 1):
        month_list.append(str(day))
    return month_list

def get_day_week(statdate):
    week_list = list()
    today = time.strptime(statdate,'%Y%m%d')
    today = datetime.date(today.tm_year, today.tm_mon, today.tm_mday)
    weekday = calendar.weekday(today.year, today.month, today.day)
    
    first_day = today - datetime.timedelta(days = weekday)
    for index in range(0,7):
        day = first_day + datetime.timedelta(days = index)
        week_list.append(day.strftime('%Y%m%d'))
    return week_list

def get_day_Yesterday(statdate):
    today = time.strptime(statdate,'%Y-%m-%d %H:%M:%S')
    today = datetime.date(today.tm_year, today.tm_mon, today.tm_mday)
    Yesterday = today - datetime.timedelta(days = 1)
    return Yesterday.strftime('%Y-%m-%d %H:%M:%S')

def get_hour_lasthour(statdate):
    #today = time.strptime(statdate,'%Y-%m-%d %H:%M:%S')
    today = datetime.datetime.strptime(statdate,'%Y-%m-%d %H:%M:%S')
    #today = datetime.date(today.tm_year, today.tm_mon, today.tm_mday)
    lasthour = today - datetime.timedelta(hours = 1)
    return lasthour.strftime('%Y-%m-%d %H:%M:%S')[:14] + '00:00'

def get_hour_nexthour(statdate):
    #today = time.strptime(statdate,'%Y-%m-%d %H:%M:%S')
    today = datetime.datetime.strptime(statdate,'%Y-%m-%d %H:%M:%S')
    #today = datetime.date(today.tm_year, today.tm_mon, today.tm_mday)
    #print today
    nexthour = today + datetime.timedelta(hours = 1)
    return nexthour.strftime('%Y-%m-%d %H:%M:%S')[:14] + '00:00'
    
def calc_2xdict_val_max(thedict, key_a, key_b, val):
    if key_a in thedict:
        if key_b in thedict[key_a]:
            if thedict[key_a][key_b] < val:
                thedict[key_a][key_b] = val
            return 1
        else:
            thedict[key_a].update({key_b: val})
            return 0
    else:
        thedict.update({key_a:{key_b: val}})
        return 0

def calc_2xdict_val_min(thedict, key_a, key_b, val):
    if key_a in thedict:
        if key_b in thedict[key_a]:
            if thedict[key_a][key_b] > val or thedict[key_a][key_b] == -1:
                thedict[key_a][key_b] = val
            return 1
        else:
            thedict[key_a].update({key_b: val})
            return 0
    else:
        thedict.update({key_a:{key_b: val}})
        return 0
'''
value[0]:累加的数值
value[1]:累加的次数
'''
def calc_2xdict_list2_add(thedict, key_a, key_b, val):
    if key_a in thedict:
        if key_b in thedict[key_a]:
            if val[0] != -1:
                if -1 == thedict[key_a][key_b][0]:
                    thedict[key_a][key_b][0] = 0
                thedict[key_a][key_b][0] += val[0]
                thedict[key_a][key_b][1] += val[1]
            return 1
        else:
            if val[0] != -1:
                thedict[key_a].update({key_b: val})
            else:
                thedict[key_a].update({key_b: [-1,0]})
            return 0
    else:
        if val[0] != -1:
            thedict.update({key_a:{key_b: val}})
        else:
            thedict.update({key_a:{key_b: [-1,0]}})
        return 0
        
def calc_2xdict_val_add(thedict, key_a, key_b, val):
    if key_a in thedict:
        if key_b in thedict[key_a]:
            thedict[key_a][key_b] += val
            return 1
        else:
            thedict[key_a].update({key_b: val})
            return 0
    else:
        thedict.update({key_a:{key_b: val}})
        return 0
    
def set_2xdict(thedict, key_a, key_b, val):
    if key_a in thedict:
        thedict[key_a].update({key_b: val})
    else:
        thedict.update({key_a:{key_b: val}})  

def list_to_csv(thelist):
    newlist = list()
    for ele in thelist:
        newlist.append('"'+ele+'"')
    return newlist
    
def fill_log_data(cols):
    new_cols = cols
    for i, val in enumerate(cols):
        if val == None or val.strip() == "":
            new_cols[i] = "-1"
        else:
            new_cols[i] = val.strip()
    return new_cols


'''
对配置文件的行做统一格式化
'''
def format_conf_line(line):
    newline = line.split('#')[0]
    newline = newline.strip(' ;\t\n')
    if newline:
        return newline
    return None

        
global logging
