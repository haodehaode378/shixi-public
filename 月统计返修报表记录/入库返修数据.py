#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2025.07.31
# @Author : 王沁桐(3636617336@qq.com)
# @File : 入库返修数据.py
# @Description : 

import pandas as pd
from pymysql import MySQLError
from db_utils import create_db_connection, close_db_connection
from config import DB_CONFIG, EXCEL_CONFIG

def create_repair_table(conn, table_name):
    """
    创建返修数据表（repair_stats）

    参数:
        conn (pymysql.connections.Connection): 数据库连接对象
        table_name (str): 表名
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
        `board_code` VARCHAR(255) COMMENT '对应Excel第23列',
        `count` INT COMMENT '对应Excel第12列（个数）',
        `year` INT COMMENT '对应Excel第16列（年份）',
        `month` INT COMMENT '对应Excel第17列（月份）',
        import_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
        print(f"表 `{table_name}` 已创建（若不存在）")
    except MySQLError as e:
        print(f"建表失败: {e}")

def get_valid_board_codes(conn):
    """
    从物料表获取有效board_code列表

    参数:
        conn (pymysql.connections.Connection): 数据库连接对象

    返回:
        list: 有效board_code字符串列表（去重去空）
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT board_code FROM material_stats")
            # 转换为字符串并去空格，确保格式统一
            valid_codes = [str(row[0]).strip() for row in cursor.fetchall()]
            print(f"从物料表获取到{len(valid_codes)}个有效board_code")
            return valid_codes
    except MySQLError as e:
        print(f"读取物料表失败: {e}")
        return []

def safe_int_convert(value):
    """
    安全转换值为整数

    参数:
        value: 待转换的值

    返回:
        int: 转换后的整数，失败则返回None
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def insert_repair_data(conn, table_name, excel_path, sheet_name, valid_codes):
    """
    处理并插入返修数据（含数据清洗）

    参数:
        conn (pymysql.connections.Connection): 数据库连接对象
        table_name (str): 目标表名
        excel_path (str): Excel文件路径
        sheet_name (str): 工作表名称
        valid_codes (list): 有效board_code列表
    """
    try:
        # 读取Excel指定列（第12、16、17、23列，索引11、15、16、22）
        df = pd.read_excel(
            excel_path,
            sheet_name=sheet_name,
            usecols=[11, 15, 16, 22],
            header=0
        )
        df.columns = ["count", "year", "month", "board_code"]
        total_rows = len(df)
        print(f"读取到返修数据共{total_rows}行")

        # 数据清洗步骤
        df = df.dropna()  # 过滤空值行
        print(f"过滤空值后剩余{len(df)}行")
        if df.empty:
            print("无有效数据，终止处理")
            return

        # 处理board_code格式
        df["board_code"] = df["board_code"].astype(str).str.strip()
        df = df[df["board_code"] != ""]  # 过滤空字符串
        print(f"过滤空board_code后剩余{len(df)}行")
        if df.empty:
            print("无有效board_code数据，终止处理")
            return

        # 转换数值列并过滤无效值
        df["count"] = df["count"].apply(safe_int_convert)
        df["year"] = df["year"].apply(safe_int_convert)
        df["month"] = df["month"].apply(safe_int_convert)
        df = df.dropna(subset=["count", "year", "month"])
        print(f"过滤无效数值后剩余{len(df)}行")
        if df.empty:
            print("无有效数值数据，终止处理")
            return

        # 匹配有效board_code
        df = df[df["board_code"].isin(valid_codes)]
        print(f"匹配物料表后剩余{len(df)}行有效数据")
        if df.empty:
            print("无匹配物料表的数据，终止处理")
            return

        # 批量插入数据库
        records = [tuple(row) for row in df[["board_code", "count", "year", "month"]].values]
        insert_sql = f"""
        INSERT INTO `{table_name}` (board_code, count, year, month)
        VALUES (%s, %s, %s, %s)
        """
        with conn.cursor() as cursor:
            cursor.executemany(insert_sql, records)
            conn.commit()
        print(f"成功插入{len(records)}条数据到{table_name}表")

    except MySQLError as e:
        print(f"数据库操作失败: {e}")
        conn.rollback()
    except Exception as e:
        print(f"数据处理错误: {e}")

def main():
    """返修数据处理主函数"""
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
        # 创建返修表
        create_repair_table(conn, DB_CONFIG["repair_table"])
        
        # 获取有效board_code列表
        valid_codes = get_valid_board_codes(conn)
        if not valid_codes:
            print("物料表无有效数据，无法继续")
            return

        # 插入返修数据
        insert_repair_data(
            conn,
            DB_CONFIG["repair_table"],
            EXCEL_CONFIG["path"],
            EXCEL_CONFIG["repair_sheet"],
            valid_codes
        )
    finally:
        # 关闭连接
        close_db_connection(conn)

if __name__ == "__main__":
    main()