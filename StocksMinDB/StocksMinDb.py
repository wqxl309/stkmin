
import configparser as cp
import datetime as dt
import h5py
import mysql.connector
import logging
import os
import scipy.io as scio
import time

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.types import INTEGER,FLOAT

from StocksMinDB.Constants import LogMark,TableCol

class StocksMinDB:

    def __init__(self,configpath):
        cfp = cp.ConfigParser()
        cfp.read(os.path.join(configpath,'loginfo.ini'))
        self._loginfo = dict(cfp.items('login'))
        self._currdb = None # 当前连接的数据库
        cfp.read(os.path.join(configpath,'datainfo.ini'))
        self._updtpath = cfp.get('datasource','update')
        self._histpath = cfp.get('datasource','history')
        ######## create logger # 按实际调用日期写日志  ########
        logfile = os.path.join('logs','Stocks_Data_Min_{0}.log'.format(dt.datetime.today().strftime('%Y%m%d')))
        if not os.path.exists(logfile):
            os.system('type NUL > {0}'.format(logfile))
        self._logger = logging.getLogger(name=__name__)
        self._logger.setLevel(level=logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
        fh = logging.FileHandler(logfile, mode='a') # 输出到file
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        ch = logging.StreamHandler() # 输出到屏幕
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        self._logger.addHandler(fh)
        self._logger.addHandler(ch)

    def _db_connect(self,dbtype='by_day'):
        """ 连接到数据库"""
        dbname  = '_'.join(['stocks_data_min',dbtype])
        if (self._currdb is not None) and (self._currdb==dbtype):
            self._logger.info('{0}already connected to database : {1}'.format(LogMark.info,self._currdb))
        elif (self._currdb is not None) and (self._currdb!=dbtype):
            self._switch_db(dbtype=dbtype)
        else:
            try:
                self.conn = mysql.connector.connect(**self._loginfo)
                self.cursor = self.conn.cursor()
                self.cursor.execute('USE {0};'.format(dbname))
                self.cursor.execute('SELECT DATABASE()')
                self._currdb = self.cursor.fetchone()[0]
                self._logger.info('{0}connected to database : {1}'.format(LogMark.info,self._currdb))
                # egnstr = r'mysql+mysqlconnector://root:{pwd}@{host}/{schema}?charset=utf8'.format(pwd=self._loginfo['password'],
                #                                                                                  host=self._loginfo['host'],
                #                                                                                  #port=self._loginfo['port'],
                #                                                                                  schema=dbname)
                # self._engine = create_engine(egnstr, echo=False)
            except mysql.connector.Error as e:
                self._logger.error('{0}connect fails : {1}'.format(LogMark.error,str(e)))

    def _switch_db(self,dbtype='by_day'):
        """ 切换数据库"""
        currtype = '_'.join(self._currdb.split('_')[-2:])
        if dbtype==currtype:
            self._logger.info('{0}already connected to database : {1}'.format(LogMark.info,self._currdb))
        else:
            dbname  = '_'.join(['stocks_data_min',dbtype])
            try:
                self.cursor.execute('USE {0};'.format(dbname))
                self.cursor.execute('SELECT DATABASE()')
                self._currdb = self.cursor.fetchone()[0]
                self._logger.info('{0}connected to database : {1}'.format(LogMark.info,self._currdb))
                # egnstr = r'mysql+mysqlconnector://root:{pwd}@{host}/{schema}?charset=utf8'.format(pwd=self._loginfo['password'],
                #                                                                              host=self._loginfo['host'],
                #                                                                              #port=self._loginfo['port'],
                #                                                                              schema=dbname)
                # self._engine = create_engine(egnstr, echo=False)
            except mysql.connector.Error as e:
                self._logger.error('{0}connect fails : {1}'.format(LogMark.error,str(e)))

    def _get_db_tables(self,dbtype='by_day'):
        """获取指定数据库的所有表格"""
        if self._currdb==dbtype:
            self._switch_db(dbtype=dbtype)
        self.cursor.execute('SHOW TABLES;')
        temptbs = self.cursor.fetchall()
        return [tb[0] for tb in temptbs if tb[0]!='trddates'] if temptbs else temptbs

    def _get_trddates(self,cuts=None):
        if cuts:
            pass

    # def update_trddates(self,dbtype='by_day'):
    #     self._db_connect(dbtype=dbtype)
    #     dates = scio.loadmat(r'C:\Users\Jiapeng\Desktop\trddates.mat')['trddates'][:,0]
    #     for dt in dates:
    #         exeline = 'INSERT INTO trddates (date) VALUES ({0})'.format(dt)
    #         self.cursor.execute(exeline)
    #     self.conn.commit()

    def update_db(self,data,tablename,colinfo,prmkey=None,dbtype='by_day',if_exist='nothing'):
        """ 将 单张表格 数据更新至 指定数据库
            data : np.array of size obsnum*colnum
            colinfo : 列名：列类型的dict
        """
        obsnum = data.shape[0]
        colnames = list(colinfo.keys())
        colnum = len(colnames)
        savedtables = self._get_db_tables()
        hastable = tablename in savedtables
        if hastable and (if_exist=='nothing'): # 数据表已存在且不会替换
            self._logger.info('{0}table {1} already in database {2}'.format(LogMark.info,tablename,self._currdb))
            insertdata = False
        elif (hastable and if_exist=='replace') or (not hastable): # 需要创建新表格
            if hastable: # 需要先删除原表格
                self.cursor.execute('DROP TABLE {0}'.format(tablename))
                self._logger.info('{0}table {1} dropped from database {2}'.format(LogMark.info,tablename,self._currdb))
            ############# 创建表格 #############
            colstr = '('+','.join(['{0} {1}'.format(cn,colinfo[cn]) for cn in colinfo])
            prmkey = ',PRIMARY KEY (' + ','.join(prmkey) + '))' if prmkey else ')'
            egn = 'ENGINE=InnoDB DEFAULT CHARSET=utf8'
            createline = ' '.join(['CREATE TABLE {0} '.format(tablename),colstr,prmkey,egn])
            try:
                self.cursor.execute(createline)
                self._logger.info('{0}create table {1} successfully in database {2}'.format(LogMark.info,tablename,self._currdb))
            except mysql.connector.Error as e:
                self._logger.error('{0}create table {1} failed in database {2},err : {3}'.format(LogMark.error,tablename,self._currdb,str(e)))
                raise e
            insertdata = True
        elif hastable and if_exist=='append':
            insertdata = True
        else:
            raise BaseException('if_exist value {0} error'.format(if_exist))
        ############# 插入表格 #############
        if insertdata:
            insertline  = 'INSERT INTO {0} ('.format(tablename) + ','.join(colnames) + ') VALUES '
            try:
                st = time.time()
                for row in range(obsnum):
                    rowdata = data[row,:]
                    exeline = ''.join([insertline,'('+','.join(['{'+'{0}'.format(i)+'}' for i in range(colnum)])+')']).format(*rowdata)
                    self.cursor.execute(exeline)
                self.conn.commit()
                self._logger.info('{0}table updated {1} successfully in database {2} with {3} seconds'.format(LogMark.info,tablename,self._currdb,time.time()-st))
            except mysql.connector.Error as e:
                if (hastable and if_exist=='replace') or (not hastable): # 需要创建新表格的情况下
                    self.cursor.execute('DROP TABLE {0}'.format(tablename))  # 如果更新失败需要确保表格删除
                    self._logger.info('{0}table {1} dropped from database {2}'.format(LogMark.info,tablename,self._currdb))
                self._logger.error('{0}update table {1} failed in database {2}, line No.{3} ,err : {4}'.format(LogMark.error,tablename,self._currdb,row,str(e)))
                raise e

    def update_data_by_day(self):
        """ 按日度更新数据，目前为.mat格式 """
        self._db_connect(dbtype='by_day')
        datelst = [date.split('.')[0] for date in os.listdir(self._updtpath)]
        newdates = sorted(set(datelst) - set([tb.split('_')[1] for tb in self._get_db_tables()]))
        if not newdates:
            self._logger.info('{0}no new table to update for database {1}'.format(LogMark.info,self._currdb))
            return
        else:
            self._logger.info('{0}{1} tables to update for database {2}'.format(LogMark.info,len(newdates),self._currdb))
        colinfo = {
            TableCol.stkcd:'INT UNSIGNED NOT NULL',
            TableCol.time:'INT UNSIGNED NOT NULL',
            TableCol.open:'FLOAT',
            TableCol.high:'FLOAT',
            TableCol.low:'FLOAT',
            TableCol.close:'FLOAT',
            TableCol.volume:'DOUBLE',
            TableCol.amount:'DOUBLE',
            TableCol.stkid:'INT UNSIGNED NOT NULL'
        }
        prmkey = [TableCol.stkcd,TableCol.time]
        for newdt in newdates:
            tablename = 'stkmin_'+newdt
            print(tablename)
            newdata = np.transpose(h5py.File(os.path.join(self._updtpath,'{0}.mat'.format(newdt)))['sdata'])
            self.update_db(data=newdata,tablename=tablename,colinfo=colinfo,prmkey=prmkey,dbtype='by_day',if_exist='replace')
            dateupdt = 'INSERT INTO trddates (date) VALUES ({0})'.format(newdt)
            self.cursor.execute(dateupdt)
            self.conn.commit()

    def update_data_by_day1(self):
        """ 按日度更新数据，目前为.mat格式 """
        datelst = [date.split('.')[0] for date in os.listdir(self._updtpath)]
        newdates = sorted(set(datelst) - set([tb.split('_')[1] for tb in self._get_db_tables()]))
        if not newdates:
            self._logger.info('{0}no new table to update for database {1}'.format(LogMark.info,self._currdb))
            return
        else:
            self._logger.info('{0}{1} tables to update for database {2}'.format(LogMark.info,len(newdates),self._currdb))
        for newdt in newdates:
            tablename = 'stkmin_'+newdt
            print(tablename)
            ############### create table ##########################
            createline = ['CREATE TABLE {0} '.format(tablename)+
                          '({0} INT UNSIGNED NOT NULL'.format(TableCol.stkcd),
                          '{0} INT UNSIGNED NOT NULL'.format(TableCol.time),
                          '{0} FLOAT'.format(TableCol.open),
                          '{0} FLOAT'.format(TableCol.high),
                          '{0} FLOAT'.format(TableCol.low),
                          '{0} FLOAT'.format(TableCol.close),
                          '{0} DOUBLE'.format(TableCol.volume),
                          '{0} DOUBLE'.format(TableCol.amount),
                          '{0} INT UNSIGNED NOT NULL'.format(TableCol.stkid),
                          'PRIMARY KEY ({0},{1})) '.format(TableCol.stkid,TableCol.time),
                          ]
            egn = 'ENGINE=InnoDB DEFAULT CHARSET=utf8'
            try:
                self.cursor.execute(','.join(createline)+egn)
                self._logger.info('{0}create table {1} successfully in database {2}'.format(LogMark.info,tablename,self._currdb))
            except mysql.connector.Error as e:
                self._logger.error('{0}create table {1} failed in database {2},err : {3}'.format(LogMark.error,tablename,self._currdb,str(e)))
                raise e
            ################# load data ######################
            newdata = h5py.File(os.path.join(self._updtpath,'{0}.mat'.format(newdt)))['sdata']
            ################# insert data ######################
            insertline = ','.join(['INSERT INTO {0} '.format(tablename)+
                                   '({0}'.format(TableCol.stkcd),
                                   '{0}'.format(TableCol.time),
                                   '{0}'.format(TableCol.open),
                                   '{0}'.format(TableCol.high),
                                   '{0}'.format(TableCol.low),
                                   '{0}'.format(TableCol.close),
                                   '{0}'.format(TableCol.volume),
                                   '{0}'.format(TableCol.amount),
                                   '{0})'.format(TableCol.stkid)+
                                   ' VALUES '])
            try:
                st = time.time()
                for row in range(newdata.shape[1]):
                    rowdata = newdata[:,row]
                    exeline = ''.join([insertline,'({0},{1},{2},{3},{4},{5},{6},{7},{8})'.format(*rowdata)])
                    self.cursor.execute(exeline)
                self.conn.commit()
                self._logger.info('{0}table updated {1} successfully in database {2} with {3} seconds'.format(LogMark.info,tablename,self._currdb,time.time()-st))
            except mysql.connector.Error as e:
                self.cursor.execute('DROP TABLE {0}'.format(tablename))
                self._logger.info('{0}table {1} dropped from database {2}').format(LogMark.info,tablename,self._currdb)
                self._logger.error('{0}update table {1} failed in database {2}, line No.{3} ,err : {4}'.format(LogMark.error,tablename,row,self._currdb,str(e)))
                raise e

    def update_data_by_stock(self,tempfolder):
        """ 按股票更新（历史）数据，目前为CSV格式"""
        self._db_connect(dbtype='by_stock')
        filepath = os.path.join(self._histpath,tempfolder)
        filelst = os.listdir(filepath)
        colnames = ['date','time','open','high','low','close','volume','amount']
        colinfo = {
            TableCol.date:'INT UNSIGNED NOT NULL',
            TableCol.time:'INT UNSIGNED NOT NULL',
            TableCol.open:'FLOAT',
            TableCol.high:'FLOAT',
            TableCol.low:'FLOAT',
            TableCol.close:'FLOAT',
            TableCol.volume:'DOUBLE',
            TableCol.amount:'DOUBLE',
            TableCol.stkcd:'INT UNSIGNED NOT NULL'
        }
        prmkey = [TableCol.date,TableCol.time]
        for fl in filelst:
            flname = fl.split('.')[0]
            cond1 = not flname[2:].isnumeric()
            cond2 = flname[0:2]=='SH' and (flname[2] not in ('6'))
            cond3 = flname[0:2]=='SZ' and (flname[2] not in ('0','3'))
            cond4 = flname[0:2]=='SZ' and (flname[2:5]=='399')
            if cond1 or cond2 or cond3 or cond4:
                continue
            tablename = 'stkmin_' + flname.lower()
            print(tablename)
            fldata = pd.read_csv(os.path.join(filepath,fl),names=colnames)
            fldata['stkcd'] = int(flname[2:8])
            fldata['date'] = fldata['date'].str.replace('/','').map(int)
            fldata['time'] = fldata['time'].str.replace(':','').map(int)
            self.update_db(data=fldata.values,tablename=tablename,colinfo=colinfo,prmkey=prmkey,dbtype='by_stock',if_exist='append')

    def byday2bystk(self):
        if self._currdb=='by_day':
            self._switch_db(dbtype='by_stock')


    def bystk2byday(self):
        if self._currdb=='by_stock':
            self._switch_db(dbtype='by_day')



if __name__=='__main__':
    c = r'E:\stocks_data_min\StocksMinDB\configs'
    obj = StocksMinDB(c)
    obj.update_data_by_stock('200007-12')