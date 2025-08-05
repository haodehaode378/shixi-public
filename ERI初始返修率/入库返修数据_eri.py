#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime, timedelta
from pymysql import MySQLError
from db_utils import create_db_connection, close_db_connection
from config import DB_CONFIG, EXCEL_CONFIG

def create_repair_table(conn, table_name):
    """创建返修数据表（包含repair_date和result字段）"""
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
        `board_code` VARCHAR(255) COMMENT '对应Excel第23列',
        `count` INT COMMENT '对应Excel第12列（个数）',
        `year` INT COMMENT '对应Excel第16列（年份）',
        `month` INT COMMENT '对应Excel第17列（月份）',
        `repair_date` VARCHAR(255) COMMENT '对应Excel第O列（返修日期）',
        import_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
    except MySQLError as e:
        print(f"建表失败: {e}")

def get_valid_board_codes(conn):
    """从物料表获取有效board_code列表"""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT board_code FROM material_stats_eri")
            valid_codes = [str(row[0]).strip() for row in cursor.fetchall() if row[0] is not None]
            print(f"从物料表获取到{len(valid_codes)}个有效board_code")
            return valid_codes
    except MySQLError as e:
        print(f"读取物料表失败: {e}")
        return []

def safe_int_convert(value):
    """安全转换值为整数"""
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def parse_date(date_str):
    """解析日期字符串，支持多种格式"""
    if not date_str or str(date_str).strip() == "":
        return None
    
    date_formats = ['%Y-%m-%d', '%Y/%m/%d', '%Y年%m月%d日', '%Y%m%d']
    for fmt in date_formats:
        try:
            return datetime.strptime(str(date_str).strip(), fmt)
        except ValueError:
            continue
    return None


def insert_repair_data(conn, table_name, excel_path, sheet_name, valid_codes):
    """处理并插入返修数据（包含repair_date）"""
    try:

        # 读取Excel时包含第O列（索引14）作为repair_date
        df = pd.read_excel(
            excel_path,
            sheet_name=sheet_name,
            usecols=[11,  12, 13,14, 22],  # 包含O列（索引14）
            header=0
        )
        # 列名对应：count（数量）、repair_date（O列）、year（年份）、month（月份）、board_code（编码）
        df.columns = ["count", "year", "month", "repair_date_str", "board_code"]
        total_rows = len(df)
        print(f"读取到返修数据共{total_rows}行（含O列数据）")

        # 数据清洗步骤
        df = df.dropna(subset=["board_code"])  # 过滤board_code为空的行
        print(f"过滤空board_code后剩余{len(df)}行")
        if df.empty:
            print("无有效board_code数据，终止处理")
            return

        # 处理board_code格式
        df["board_code"] = df["board_code"].astype(str).str.strip()

        # 转换数值列并过滤无效值
        df["count"] = df["count"].apply(safe_int_convert) # 填充 0
        df["year"] = df["year"].apply(safe_int_convert) # 填充默认年份
        df["month"] = df["month"].apply(safe_int_convert)  # 填充默认月份
        df = df.dropna(how="all", subset=["count", "year", "month"])
        print(f"过滤无效数值后剩余{len(df)}行")
        if df.empty:
            print("无有效数值数据，终止处理")
            return

        # 解析repair_date
        df["repair_date"] = df["repair_date_str"].apply(parse_date)
        # 保留原始字符串用于存储
        df["repair_date_str"] = df["repair_date_str"].astype(str).str.strip()


        # 匹配有效board_code
        df = df[df["board_code"].isin(valid_codes)]
        print(f"匹配物料表后剩余{len(df)}行有效数据")
        if df.empty:
            print("无匹配物料表的数据，终止处理")
            return

        # 批量插入数据库（包含repair_date）
        records = [
            (row["board_code"], row["count"], row["year"], row["month"], row["repair_date_str"])
            for _, row in df.iterrows()
        ]
        insert_sql = f"""
        INSERT INTO `{table_name}` (board_code, count, year, month, repair_date)
        VALUES (%s, %s, %s, %s, %s)
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
    """主函数：建表→读取数据→入库"""
    conn = create_db_connection(
        DB_CONFIG["host"],
        DB_CONFIG["user"],
        DB_CONFIG["password"],
        DB_CONFIG["database"]
    )
    if not conn:
        return
    
    try:
        create_repair_table(conn, DB_CONFIG["repair_table"])
        valid_codes = get_valid_board_codes(conn)
        if not valid_codes:
            print("物料表无有效数据，无法继续")
            return

        insert_repair_data(
            conn,
            DB_CONFIG["repair_table"],
            EXCEL_CONFIG["path"],
            EXCEL_CONFIG["repair_sheet"],
            valid_codes
        )
    finally:
        close_db_connection(conn)

if __name__ == "__main__":
    main()
