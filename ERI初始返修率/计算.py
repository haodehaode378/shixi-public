import pymysql
from datetime import datetime, timedelta

# 数据库连接配置（原数据库，包含 repair_stats_eri 表）
src_db_config = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "三江"
}

# 数据库连接配置（新数据库，用于存储结果）
dest_db_config = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "三江"
}

# 新表创建语句
create_table_sql = """
CREATE TABLE IF NOT EXISTS new_repair_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    board_code VARCHAR(255),
    time_calculated DATETIME,  -- 这里的“后面定义的时间”，你可根据实际需求调整字段名和类型
    diff_result VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# 连接原数据库，查询数据并处理
def process_and_create_new_table():
    # 连接原数据库
    src_conn = pymysql.connect(**src_db_config)
    src_cursor = src_conn.cursor(pymysql.cursors.DictCursor)
    
    # 连接新数据库
    dest_conn = pymysql.connect(**dest_db_config)
    dest_cursor = dest_conn.cursor()
    
    try:
        # 在新数据库创建表
        dest_cursor.execute(create_table_sql)
        dest_conn.commit()
        
        # 查询原表数据
        query_sql = "SELECT id, board_code, year, month, repair_date FROM repair_stats_eri;"
        src_cursor.execute(query_sql)
        rows = src_cursor.fetchall()
        
        for row in rows:
            id_val = row["id"]
            board_code = row["board_code"]
            year = row["year"]
            month = row["month"]
            repair_date = row["repair_date"]
            
            # 构造 year 和 month 对应的当月 1 号的日期
            
            target_date = datetime(year, month, 1)
            
                
            # 计算时间差（天数），repair_date 需是 datetime 类型，若原数据是字符串需先转换
            if target_date and isinstance(repair_date, datetime):
                diff_days = (target_date - repair_date).days
                # 根据时间差判断结果
                if diff_days > 540:
                    diff_result = "LTR"
                elif diff_days > 180:
                    diff_result = "YRR"
                elif diff_days > 0:
                    diff_result = "ERI"
                else:
                    diff_result = "NA"
            else:
                diff_result = "NA"
            
            
            time_calculated = target_date
            
            # 插入新数据库表
            insert_sql = """
            INSERT INTO new_repair_stats (id, board_code, time_calculated, diff_result)
            VALUES (%s, %s, %s, %s);
            """

        
        dest_conn.commit()
        print("数据处理并插入新表完成")
        
    except Exception as e:
        print(f"处理过程中发生错误: {e}")
        dest_conn.rollback()
    finally:
        src_cursor.close()
        src_conn.close()
        dest_cursor.close()
        dest_conn.close()

if __name__ == "__main__":
    process_and_create_new_table()