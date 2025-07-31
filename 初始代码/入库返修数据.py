import pandas as pd
import pymysql
from pymysql import MySQLError

def create_db_connection(host, user, password, database):
    """创建数据库连接"""
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

def create_repair_table(conn, table_name):
    """自动创建repair_stats表（若不存在）"""
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `id` INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
        `board_code` VARCHAR(255) COMMENT '对应Excel第23列',
        `count` INT COMMENT '对应Excel第12列（个数）',
        `year` INT COMMENT '对应Excel第16列（年份）',
        `month` INT COMMENT '对应Excel第17列（月份）'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
        print(f"表 `{table_name}` 已创建（若不存在）")
    except MySQLError as e:
        print(f"建表失败: {e}")

def get_valid_board_codes(conn):
    """从参考表获取有效board_code（字符串类型）"""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT board_code FROM material_stats")
            # 转为字符串并去空格，确保格式统一
            valid_codes = [str(row[0]).strip() for row in cursor.fetchall()]
            print(f"从参考表获取到{len(valid_codes)}个有效board_code")
            return valid_codes
    except MySQLError as e:
        print(f"读取参考表失败: {e}")
        return []

def safe_int_convert(value):
    """安全转换为整数，失败返回None"""
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def insert_repair_data(conn, table_name, excel_path, sheet_name, valid_codes):
    """处理并插入返修数据（过滤空值、无效类型和不匹配数据）"""
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
        print(f"读取到Excel数据共{total_rows}行")

        # 1. 过滤所有含空值的行
        df = df.dropna()
        print(f"过滤空值后剩余{len(df)}行")
        if df.empty:
            print("无有效数据，终止处理")
            return

        # 2. 处理board_code：转为字符串并去空格
        df["board_code"] = df["board_code"].astype(str).str.strip()
        # 过滤board_code为空字符串的行
        df = df[df["board_code"] != ""]
        print(f"过滤空board_code后剩余{len(df)}行")
        if df.empty:
            print("无有效board_code数据，终止处理")
            return

        # 3. 安全转换数值列（count/year/month）为整数
        df["count"] = df["count"].apply(safe_int_convert)
        df["year"] = df["year"].apply(safe_int_convert)
        df["month"] = df["month"].apply(safe_int_convert)
        # 过滤转换失败的行
        df = df.dropna(subset=["count", "year", "month"])
        print(f"过滤无效数值后剩余{len(df)}行")
        if df.empty:
            print("无有效数值数据，终止处理")
            return

        # 4. 过滤不在参考表中的board_code
        df = df[df["board_code"].isin(valid_codes)]
        print(f"匹配参考表后剩余{len(df)}行有效数据")
        if df.empty:
            print("无匹配参考表的数据，终止处理")
            return

        # 5. 转换为插入格式并批量插入
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
    # 配置信息（需根据实际环境修改）
    DB_CONFIG = {
        "host": "localhost",
        "user": "root",
        "password": "123456",
        "database": "三江",
        "repair_table": "repair_stats"
    }
    EXCEL_CONFIG = {
        "path": r"C:\Users\admin\Desktop\三江\核心产品返修率-20250611外发wqt.xlsx",
        "sheet": "返修"
    }

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
        # 自动创建表（若不存在）
        create_repair_table(conn, DB_CONFIG["repair_table"])
        
        # 获取参考表中的有效board_code
        valid_codes = get_valid_board_codes(conn)
        if not valid_codes:
            print("参考表无有效数据，无法继续")
            return

        # 处理并插入返修数据
        insert_repair_data(
            conn,
            DB_CONFIG["repair_table"],
            EXCEL_CONFIG["path"],
            EXCEL_CONFIG["sheet"],
            valid_codes
        )

    finally:
        # 关闭数据库连接
        if conn:
            conn.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    main()
    