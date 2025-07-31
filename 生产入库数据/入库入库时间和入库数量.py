#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2025.07.30
# @Author : 王沁桐(3636617336@qq.com)
# @File : 入哭入库时间和入库数量.py
# @Description : 


"""入库信息导入程序
从Excel读取入库时间和数量信息，导入数据库material_stock表
"""
from db_utils import get_db_connection, close_db_connection, create_table
from excel_utils import check_file_exists, load_excel_workbook, get_excel_sheet, read_cell_value


EXCEL_FILE = r"C:\Users\admin\Desktop\三江\核心产品返修率-20250611外发wqt.xlsx"
SHEET_NAME = "板子入库"
DB_CONFIG = {
    'host': 'localhost',    
    'user': 'root',         
    'password': '123456',   
    'database': '三江'      
}
target_table = "material_stock" 


def main():
    """
    主函数：执行入库信息导入流程
    """
    # 1. 检查Excel文件
    if not check_file_exists(EXCEL_FILE):
        return

    # 2. 加载Excel
    workbook = load_excel_workbook(EXCEL_FILE)
    if not workbook:
        return

    sheet = get_excel_sheet(workbook, SHEET_NAME)
    if not sheet:
        workbook.close()
        return
    print("Excel文件加载成功")

    # 3. 连接数据库
    conn, cursor = get_db_connection(DB_CONFIG)
    if not conn or not cursor:
        workbook.close()
        return

    try:
        # 4. 创建表
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{target_table}` (
            `material_code` VARCHAR(50),
            `seq` VARCHAR(20),
            `date` DATE,
            `quantity` FLOAT,
            PRIMARY KEY (`material_code`, `seq`, `date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        if not create_table(conn, cursor, target_table, create_table_sql):
            return

        # 5. 遍历数据并插入
        success_count = 0
        fail_count = 0
        
        # 行范围：2到182行
        for row in range(2, 183):
            # 提取物料代码（A列）和序号（C列）
            material_code = read_cell_value(sheet, row, 1)
            seq = read_cell_value(sheet, row, 3)
            
            # 遍历时间列（H→AJ，列8到36）
            for col in range(8, 37):
                quantity = sheet.cell(row=row, column=col).value
                date_str = read_cell_value(sheet, 1, col)
                
                try:
                    sql = f"""
                    INSERT INTO `{target_table}` 
                    (material_code, seq, date, quantity)
                    VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(sql, (material_code, seq, date_str, quantity))
                    conn.commit()
                    success_count += 1
                except Exception as e:
                    conn.rollback()
                    fail_count += 1

    except Exception as e:
        print(f"执行过程出错：{e}")
    finally:
        close_db_connection(conn, cursor)
        workbook.close()
        print("资源已释放")


if __name__ == "__main__":
    main()