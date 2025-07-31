import pandas as pd
import pymysql
from pymysql import MySQLError

def create_db_connection(host, user, password, database):
    """创建MySQL连接（pymysql）"""
    try:
        conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4"  # 支持中文
        )
        print("数据库连接成功")
        return conn
    except MySQLError as e:
        print(f"连接失败: {e}")
        return None

def create_table_with_semantic_cols(conn, table_name):
    """创建表，列名直接用业务字段（material_code为主键）"""
    cursor = conn.cursor()
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `material_code` VARCHAR(255) PRIMARY KEY COMMENT '物料代码（Excel A列）',
        `material_desc` VARCHAR(255) COMMENT '物料描述（Excel B列）',
        `board_code`   VARCHAR(255) COMMENT '单板料号（Excel C列）'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        cursor.execute(create_sql)
        print(f"表 `{table_name}` 已创建（列名：material_code/material_desc/board_code）")
    except MySQLError as e:
        print(f"建表失败: {e}")
    finally:
        cursor.close()

def insert_excel_data_with_mapping(conn, table_name, excel_path, sheet_name):
    """读取Excel并映射列名后插入数据库"""
    try:
        # 读取Excel：仅取A2~C14（行2-14，共13行；列A-C）
        df = pd.read_excel(
            excel_path,
            sheet_name=sheet_name,
            header=0,      # Excel第1行作为表头（如“物料代码”）
            nrows=13,      # 读取13行（对应Excel行2~14）
            usecols="A:C"  # 仅保留A、B、C列
        )
        
        # 重命名列：将Excel原始表头映射为数据库列名（解决特殊字符问题）
        df = df.rename(columns={
            '物料代码': 'material_code',
            '物料描述（生产入库数据）': 'material_desc',
            '单板料号': 'board_code'
        })
        
        print("读取并映射后的Excel数据：")
        print(df)
        
        if df.empty:
            print("无有效数据，跳过插入")
            return
        
        # 转换为插入所需的元组列表
        records = [tuple(row) for row in df.values]
        
        # 批量插入（material_code为主键，重复会报错）
        cursor = conn.cursor()
        insert_sql = f"""
        INSERT INTO `{table_name}` (material_code, material_desc, board_code)
        VALUES (%s, %s, %s)
        """
        cursor.executemany(insert_sql, records)
        conn.commit()
        print(f"成功插入 {cursor.rowcount} 条数据")
    except MySQLError as e:
        print(f"插入失败: {e}（提示：material_code不可重复）")
        conn.rollback()
    except Exception as e:
        print(f"Excel处理失败: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()

def main():
    # ------------------- 配置区（必须修改！） -------------------
    DB_CONFIG = {
        "host": "localhost",        # 数据库IP
        "user": "root",             # 用户名
        "password": "123456",     # 密码
        "database": "三江",      # 数据库名（需提前创建）
        "table_name": "material_stats",  # 表名
    }
    EXCEL_CONFIG = {
        "path": r"C:\Users\admin\Desktop\三江\核心产品返修率-20250611外发wqt.xlsx",
        "sheet": "改善统计"        # 工作表名
    }
    # ------------------- 执行流程 -------------------
    conn = create_db_connection(
        DB_CONFIG["host"],
        DB_CONFIG["user"],
        DB_CONFIG["password"],
        DB_CONFIG["database"]
    )
    if not conn:
        return
    
    try:
        create_table_with_semantic_cols(conn, DB_CONFIG["table_name"])
        insert_excel_data_with_mapping(conn, DB_CONFIG["table_name"], EXCEL_CONFIG["path"], EXCEL_CONFIG["sheet"])
    finally:
        if conn:
            conn.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    main()