#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2025.07.30
# @Author : 王沁桐(3636617336@qq.com)
# @File : db_utils.py
# @Description : 


"""数据库操作通用工具模块
包含数据库连接、关闭、表创建、数据插入等通用功能
"""
import pymysql
from typing import Tuple, Optional


def get_db_connection(db_config: dict) -> Tuple[Optional[pymysql.connections.Connection], 
                                              Optional[pymysql.cursors.Cursor]]:
    """
    获取数据库连接和游标
    
    参数:
        db_config: 数据库配置字典，包含host, user, password, database等键
        
    返回:
        元组 (连接对象, 游标对象)，失败时返回 (None, None)
    """
    try:
        conn = pymysql.connect(**db_config, charset="utf8mb4")
        cursor = conn.cursor()
        print("数据库连接成功")
        return conn, cursor
    except pymysql.MySQLError as e:
        print(f"数据库连接失败: {str(e)}")
        return None, None


def close_db_connection(conn: Optional[pymysql.connections.Connection], 
                       cursor: Optional[pymysql.cursors.Cursor]) -> None:
    """
    关闭数据库连接和游标
    
    参数:
        conn: 数据库连接对象
        cursor: 游标对象
    """
    try:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("数据库连接已关闭")
    except pymysql.MySQLError as e:
        print(f"关闭数据库连接出错: {str(e)}")


def create_table(conn: pymysql.connections.Connection, 
                cursor: pymysql.cursors.Cursor, 
                table_name: str, 
                create_sql: str) -> bool:
    """
    创建数据库表（若不存在）
    
    参数:
        conn: 数据库连接对象
        cursor: 游标对象
        table_name: 表名
        create_sql: 创建表的SQL语句
        
    返回:
        创建成功返回True，失败返回False
    """
    try:
        cursor.execute(create_sql)
        conn.commit()
        print(f"表 `{table_name}` 创建（或已存在）")
        return True
    except pymysql.MySQLError as e:
        conn.rollback()
        print(f"创建表 `{table_name}` 失败: {str(e)}")
        return False


def batch_insert_data(conn: pymysql.connections.Connection, 
                     cursor: pymysql.cursors.Cursor, 
                     insert_sql: str, 
                     data_list: list) -> Tuple[int, int]:
    """
    批量插入数据
    
    参数:
        conn: 数据库连接对象
        cursor: 游标对象
        insert_sql: 插入数据的SQL语句
        data_list: 待插入的数据列表（元组组成的列表）
        
    返回:
        元组 (成功条数, 失败条数)
    """
    if not data_list:
        return 0, 0
        
    try:
        cursor.executemany(insert_sql, data_list)
        conn.commit()
        return len(data_list), 0
    except pymysql.MySQLError as e:
        conn.rollback()
        print(f"批量插入失败: {str(e)}")
        return 0, len(data_list)


def execute_query(conn: pymysql.connections.Connection, query_sql: str) -> Optional[pymysql.cursors.DictCursor]:
    """
    执行查询语句
    
    参数:
        conn: 数据库连接对象
        query_sql: 查询SQL语句
        
    返回:
        查询结果游标，失败返回None
    """
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query_sql)
        return cursor
    except pymysql.MySQLError as e:
        print(f"查询执行失败: {str(e)}")
        return None