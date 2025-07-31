#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2025.07.31
# @Author : 王沁桐(3636617336@qq.com)
# @File : 入库物料代码和物料描述和转换代码.py
# @Description : 

import pandas as pd
from pymysql import MySQLError
from db_utils import create_db_connection, close_db_connection
from config import DB_CONFIG, EXCEL_CONFIG

def create_material_table(conn, table_name):
    """
    创建物料信息表（material_stats）

    参数:
        conn (pymysql.connections.Connection): 数据库连接对象
        table_name (str): 表名
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `material_code` VARCHAR(255) PRIMARY KEY COMMENT '物料代码（Excel A列）',
        `material_desc` VARCHAR(255) COMMENT '物料描述（Excel B列）',
        `board_code`   VARCHAR(255) COMMENT '单板料号（Excel C列）',
        import_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
        print(f"表 `{table_name}` 已创建（若不存在）")
    except MySQLError as e:
        print(f"建表失败: {e}")

def insert_material_data(conn, table_name, excel_path, sheet_name):
    """
    从Excel读取物料数据并插入到数据库

    参数:
        conn (pymysql.connections.Connection): 数据库连接对象
        table_name (str): 目标表名
        excel_path (str): Excel文件路径
        sheet_name (str): 工作表名称
    """
    try:
        # 读取Excel指定范围（A2~C14，共13行）
        df = pd.read_excel(
            excel_path,
            sheet_name=sheet_name,
            header=0,      # 第1行作为表头
            nrows=13,      # 读取13行数据
            usecols="A:C"  # 仅保留A、B、C列
        )
        
        # 映射Excel表头到数据库字段
        df = df.rename(columns={
            '物料代码': 'material_code',
            '物料描述（生产入库数据）': 'material_desc',
            '单板料号': 'board_code'
        })
        
        print("读取并映射后的物料数据：")
        print(df)
        
        if df.empty:
            print("无有效物料数据，跳过插入")
            return
        
        # 转换为插入数据格式
        records = [tuple(row) for row in df.values]
        
        # 批量插入（主键重复会报错）
        insert_sql = f"""
        INSERT INTO `{table_name}` (material_code, material_desc, board_code)
        VALUES (%s, %s, %s)
        """
        with conn.cursor() as cursor:
            cursor.executemany(insert_sql, records)
            conn.commit()
        print(f"成功插入 {cursor.rowcount} 条物料数据")
    
    except MySQLError as e:
        print(f"插入失败: {e}（提示：material_code不可重复）")
        conn.rollback()
    except Exception as e:
        print(f"Excel处理失败: {e}")

def main():
    """物料数据处理主函数"""
    # 建立数据库连接
    conn = create_db_connection(
    DB_CONFIG["host"],
    DB_CONFIG["user"],
    DB_CONFIG["password"],
    DB_CONFIG["database"]
)

    if not conn:
        return
    
    try:
        # 创建物料表
        create_material_table(conn, DB_CONFIG["material_table"])
        # 插入物料数据
        insert_material_data(
            conn,
            DB_CONFIG["material_table"],
            EXCEL_CONFIG["path"],
            EXCEL_CONFIG["material_sheet"]
        )
    finally:
        # 关闭连接
        close_db_connection(conn)

if __name__ == "__main__":
    main()