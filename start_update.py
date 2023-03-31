# -*- coding:utf-8 -*-
from time import sleep
import pymysql
from functools import wraps
import os
# import urllib . request
# import chardet

import configparser

## 配置文件名称
configFileName = "config.ini"


config = configparser.ConfigParser()
config.read(configFileName)

mysqlConnectInfo = {
    "host": config["mysql_connect"]["host"],
    "user": config["mysql_connect"]["user"],
    "port": config["mysql_connect"].getint('port'),
    "password": config["mysql_connect"]["password"],
    # "database": "test_db",
    "charset": config["mysql_connect"]["charset"]
}

OldDatabaseName = config["mysql_connect"]["dbName"]
NewDatabaseName = OldDatabaseName+"_new"

sqlDir = config["other_config"]["sqlDir"]


def start_connect_mysql(DataBase):
    if DataBase != None:
        conn = pymysql.connect(database=DataBase, **mysqlConnectInfo)
    else:
        conn = pymysql.connect(**mysqlConnectInfo)
    return conn, conn.cursor()


def close_connect_mysql(conn, cursor):
    cursor.close()
    conn.close()


indexType = {
    0: "ADD UNIQUE INDEX",
    1: "ADD INDEX"
}

def write_exec_sql(sqlL):
    fileRef = open('exec.sql', 'w+', encoding='utf8')
    for sql in sqlL:
        fileRef.write(sql + "\n")
    fileRef.flush()
    fileRef.close()


def exec_sql(conn, cursor, sqlL):
    for sql in sqlL:
        cursor.execute(sql)
        conn.commit()


def executeScriptsFromFile(filename, cursor):
    fd = open(filename, 'r', encoding='utf-8')
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(';')
    for command in sqlCommands:
        try:
            cursor.execute(command)
        except Exception as msg:
            print(msg)


def update_new_sql(Cursor):
    List = os.listdir(sqlDir)
    for fileName in List:
        if fileName == '.svn':
            continue
        tmpPath = os.path.join(sqlDir, fileName)
        if os.path.isfile(tmpPath) and fileName.endswith('.sql'):
            executeScriptsFromFile(tmpPath, Cursor)


def StartCompare(a_func):
    @wraps(a_func)
    def wrapCompare(*args, **kwargs):
        Newconn, Newcursor = start_connect_mysql(None)
        sql = "DROP DATABASE IF EXISTS {0};".format(NewDatabaseName)
        Newcursor.execute(sql)
        sql = "CREATE DATABASE if not exists {0} DEFAULT CHARACTER SET {1} COLLATE {1}_bin;".format(NewDatabaseName, mysqlConnectInfo["charset"])
        Newcursor.execute(sql)
        Newcursor.execute("use {0};".format(NewDatabaseName))
        update_new_sql(Newcursor)
        Oldconn, Oldcursor = start_connect_mysql(OldDatabaseName)
        NewTableObjDir = GenerateTabObj(Newcursor)
        OldTableObjDir = GenerateTabObj(Oldcursor)
        SqlL = a_func(NewTableObjDir, OldTableObjDir)
        print(SqlL)
        # exec_sql(Oldconn, Oldcursor, SqlL)
        write_exec_sql(SqlL)
        close_connect_mysql(Newconn, Newcursor)
        close_connect_mysql(Oldconn, Oldcursor)
        return
    return wrapCompare


def handle_select_tableInfo(cursor, dbName, tableName):
    sql = "SHOW INDEX FROM " + tableName + ";"
    cursor.execute(sql)
    indexL = list(cursor.fetchall())
    sql = 'SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA="{0}" AND table_name="{1}";'.format(
        dbName, tableName)
    cursor.execute(sql)
    fieldL = list(cursor.fetchall())
    sql = "SHOW CREATE TABLE {0};".format(tableName)
    cursor.execute(sql)
    creatSqlL = list(cursor.fetchall())
    return DbTable(tableName, fieldL, indexL, creatSqlL[0][1]+";")


def GenerateTabObj(Cursor):
    sql = "SHOW TABLES;"
    Cursor.execute(sql) 
    indexL = list(Cursor.fetchall())
    sql = "select database();"
    Cursor.execute(sql)
    dbName = Cursor.fetchone()[0]
    returnDic = {}
    for tableName, in indexL:
        returnDic[tableName] = handle_select_tableInfo(
            Cursor, dbName, tableName)
    return returnDic


class DbTable:
    def __init__(self, tableName, fieldList, indexList, creatSql):
        self.creatSql = creatSql
        self.tableName = tableName
        self.Fields = []
        self.FieldsMap = {}
        self.IndexInfoDic = {}
        tmp = None
        for filedInfo in fieldList:
            tmpField = DbTableField(filedInfo, tmp, tableName)
            tmp = tmpField
            self.Fields.append(tmpField)
            self.FieldsMap[tmpField.fieldName] = tmpField

        for indexInfo in indexList:
            # if indexInfo[2] == "PRIMARY":
            #     continue
            if indexInfo[2] in self.IndexInfoDic:
                self.IndexInfoDic[indexInfo[2]].add_Column_name(
                    (indexInfo[3], indexInfo[4]))
            else:
                tmpDbTableIndex = DbTableIndex(indexInfo)
                self.IndexInfoDic[indexInfo[2]] = tmpDbTableIndex

    def CREAT_TABLE(self):
        return self.creatSql

    def DELETE_TABLE(self):
        return "DROP TABLE {0};".format(self.tableName)

    def Compare(self, Other):
        add_Field_sql = []
        delete_Field_sql = []
        update_Field_sql = []
        # 查询删除
        for k, v in self.FieldsMap.items():
            if not k in Other.FieldsMap:
                # 我的字段在other中没找到 我应该删除
                print("{0} delete Field {1}".format(self.tableName, k))
                delete_Field_sql.append(v.DELETE_FIELD())
            else:  # 查询修改
                if k in self.FieldsMap and not v.Compare(Other.FieldsMap[k]):
                    print("{0} update Field {1}".format(self.tableName, k))
                    update_Field_sql.append(
                        v.generateUpdateSql(Other.FieldsMap[k]))
        # 查询添加
        for k, v in Other.FieldsMap.items():
            if not k in self.FieldsMap:
                print("{0} add Field {1}".format(self.tableName, k))
                add_Field_sql.append(v.CREAT_FIELD())

        creat_index_sql = []
        delete_index_sql = []
        update_index_sql = []

        for k, indexInfo in self.IndexInfoDic.items():
            if not k in Other.IndexInfoDic:
                delete_index_sql.append(indexInfo.DROP_INDEX())
            else:  # 比较
                if not indexInfo.Compare(Other.IndexInfoDic[k]):
                    update_index_sql += indexInfo.generateUpdateSql(
                        Other.IndexInfoDic[k])

        for k, indexInfo in Other.IndexInfoDic.items():
            if not k in self.IndexInfoDic:
                creat_index_sql.append(indexInfo.CREAT_INDEX())

        return add_Field_sql + delete_Field_sql + update_Field_sql + creat_index_sql + delete_index_sql + update_index_sql

# MODIFY COLUMN `value4` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL AFTER `value1`;


class DbTableField:  # todo 缺少是否自增
    def __init__(self, fieldInfo, LastField, tableName):
        self.tableName = tableName    # 表明
        self.LastField = LastField    # 上一个字段
        self.fieldName = fieldInfo[3]  # 字段表示的是列名
        self.fieldType = fieldInfo[15]  # 字段表示的是列的数据类型
        self.fieldIsCanNull = fieldInfo[6]  # 字段表示这个列是否能取空值
        # 在mysql中key 和index 是一样的意思，这个Key列可能会看到有如下的值：PRI(主键)、MUL(普通的b-tree索引)、UNI(唯一索引)
        self.fieldIsPrimaryKey = fieldInfo[4]
        self.fieldDefault = fieldInfo[5]  # 列的默认值
        self.COMMENT_INFO = 'COMMENT "{0}"'.format(fieldInfo[19])        # 注释信息
        self.EXTRA = fieldInfo[17]  # 是否是自增

    def IsNull(self):
        if self.fieldIsCanNull == 'NO':
            return "NOT NULL"
        return ""

    # 删除语句
    def DELETE_FIELD(self):
        return "ALTER TABLE `{0}` DROP COLUMN `{1}`;".format(self.tableName, self.fieldName)

    # 获取上个字段
    def getLastField(self):
        if self.LastField != None:
            return "AFTER `{0}`".format(self.LastField.fieldName)
        return "FIRST"

    # 创建语句
    # ADD COLUMN `value5` varchar(255) NOT NULL COMMENT '156415' AFTER `value3`;
    def CREAT_FIELD(self):
        return "ALTER TABLE `{0}`  ADD COLUMN `{1}` {2} {3} {4} {5} {6};".format(self.tableName, self.fieldName, self.fieldType, self.IsNull(), self.EXTRA, self.COMMENT_INFO, self.getLastField())

    # 生成更新语句
    # MODIFY COLUMN `value4` int(10) NOT NULL AUTO_INCREMENT COMMENT '123456' AFTER `value1`,
    def generateUpdateSql(self, other):
        return "ALTER TABLE `{0}` MODIFY COLUMN `{1}` {2} {3} {4} {5} {6};".format(self.tableName, self.fieldName, other.fieldType, other.IsNull(), other.EXTRA, other.COMMENT_INFO, other.getLastField())

    def Compare(self, other):
        # self.fieldIsPrimaryKey == other.fieldIsPrimaryKey and \ 主键索引等处理
        return self.fieldType == other.fieldType and \
            self.fieldIsCanNull == other.fieldIsCanNull and \
            self.fieldDefault == other.fieldDefault and \
            self.COMMENT_INFO == other.COMMENT_INFO and \
            self.EXTRA == other.EXTRA


class DbTableIndex:
    def __init__(self, IndexInfo):
        self.Table = IndexInfo[0]  # 表示创建索引的数据表名，这里是 tb_stu_info2 数据表。
        # 表示该索引是否是唯一索引。若不是唯一索引，则该列的值为 1；若是唯一索引，则该列的值为 0。
        self.Non_unique = IndexInfo[1]
        self.Key_name = IndexInfo[2]  # 表示索引的名称。
        # 表示该列在索引中的位置，如果索引是单列的，则该列的值为 1；如果索引是组合索引，则该列的值为每列在索引定义中的顺序。
        self.Seq_in_index = IndexInfo[3]
        self.Column_name = [(self.Seq_in_index, IndexInfo[4])]  # 表示定义索引的列字段。
        # self.Collation = IndexInfo[5] ## 表示列以何种顺序存储在索引中。在 MySQL 中，升序显示值“A”（升序），若显示为 NULL，则表示无分类。
        # self.Cardinality = IndexInfo[6] ## 索引中唯一值数目的估计值。基数根据被存储为整数的统计数据计数，所以即使对于小型表，该值也没有必要是精确的。基数越大，当进行联合时，MySQL 使用该索引的机会就越大。
        # self.Sub_part = IndexInfo[7] ## 表示列中被编入索引的字符的数量。若列只是部分被编入索引，则该列的值为被编入索引的字符的数目；若整列被编入索引，则该列的值为 NULL。
        # self.Packed = IndexInfo[8] ## 指示关键字如何被压缩。若没有被压缩，值为 NULL
        # self.Null = IndexInfo[9] ## 用于显示索引列中是否包含 NULL。若列含有 NULL，该列的值为 YES。若没有，则该列的值为 NO。
        # 显示索引使用的类型和方法（BTREE、FULLTEXT、HASH、RTREE）。
        self.Index_type = IndexInfo[10]
        self.Comment = IndexInfo[11]  # 显示评注。

    def add_Column_name(self, ColumnName):
        self.Column_name.append(ColumnName)

    def generate_column_name(self):
        self.Column_name.sort(key=lambda e: e[0])
        tmpL = []
        for index, columnName in self.Column_name:
            tmpL.append("`"+columnName+"`")
        return ",".join(tmpL)

    # 拼写删除语句
    def DROP_INDEX(self):
        if not self.Key_name == "PRIMARY":
            return "DROP INDEX " + self.Key_name + " ON " + self.Table + ";"
        return "ALTER TABLE `{0}` DROP PRIMARY KEY;".format(self.Table)

    # 拼写创建语句只提供 NORMAL 与 UNIQUE 两种索引类型
    def CREAT_INDEX(self):
        if not self.Key_name == "PRIMARY":
            return "ALTER TABLE `{0}` {1} {2} ({3}) USING {4} COMMENT '{5}';".format(self.Table, indexType[self.Non_unique], self.Key_name, self.generate_column_name(), self.Index_type, self.Comment)
        return "ALTER TABLE {0} add primary key({1}) USING {2};".format(self.Table, self.generate_column_name(), self.Index_type)

    # 比较两个索引是否相同
    def Compare(self, Other):
        return Other.Table == self.Table and \
            Other.Key_name == self.Key_name and \
            Other.Column_name == self.Column_name and\
            Other.Index_type == self.Index_type and\
            Other.Comment == self.Comment

    # 索引名称一样其他不一样时当前的这个字段应该修改
    def generateUpdateSql(self, Other):
        # 主键对比单独抽出
        if not self.Key_name == "PRIMARY":
            return [self.DROP_INDEX()] + [Other.CREAT_INDEX()]
        return ["ALTER TABLE `{0}` DROP PRIMARY KEY, add primary key({1}) USING {2} ".format(self.Table, Other.generate_column_name(), Other.Index_type)]

@StartCompare
def start_func(NewTableObjDic, OldTableObjDic):
    delete_table = []
    add_table = []
    update_table = []
    for tableName, OldTable in OldTableObjDic.items():
        if not tableName in NewTableObjDic:
            delete_table.append(OldTable.DELETE_TABLE())
        else:
            update_table += OldTable.Compare(NewTableObjDic[tableName])
    for tableName, NewTable in NewTableObjDic.items():
        if not tableName in OldTableObjDic:
            add_table.append(NewTable.CREAT_TABLE())
    return delete_table + add_table + update_table


start_func()

# test_table()
