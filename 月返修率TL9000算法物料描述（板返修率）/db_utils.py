#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2025.07.31
# @Author : 王沁桐(3636617336@qq.com)
# @File : db_utils.py
# @Description : 

import pymysql
from pymysql import MySQLError

def create_db_connection(host, user, password, database):
    """
    创建并返回MySQL数据库连接对象

    参数:
        host (str): 数据库主机地址
        user (str): 数据库用户名
        password (str): 数据库密码
        database (str): 数据库名称

    返回:
        pymysql.connections.Connection: 数据库连接对象，失败则返回None
    """
    try:
        conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4"
        )
        print("数据库连接成功")
        return conn
    except MySQLError as e:
        print(f"连接失败: {e}")
        return None
    


def execute_query(conn, query):
    """
    执行SQL查询并返回游标（用于获取查询结果）
    参数:
        conn: 数据库连接对象（已建立的连接）
        query (str): 要执行的SQL查询语句
    返回:
        cursor: 执行查询后的游标对象（含查询结果），若失败则返回None
    """
    try:
        # 创建游标对象（用于执行查询和获取结果）
        cursor = conn.cursor()
        # 执行SQL查询
        cursor.execute(query)
        # 返回游标（后续可通过cursor.fetchall()获取数据）
        return cursor
    except Exception as e:
        print(f"查询执行失败: {e}")
        return None

def close_db_connection(conn):
    """
    关闭数据库连接

    参数:
        conn (pymysql.connections.Connection): 数据库连接对象
    """
    if conn:
        conn.close()
        print("数据库连接已关闭")