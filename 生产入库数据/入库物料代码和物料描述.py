#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2025.07.30
# @Author : 王沁桐(3636617336@qq.com)
# @File : 入库物料代码和物料描述.py
# @Description : 


"""物料信息导入程序
从Excel读取物料代码和描述，清洗后导入数据库material_info表
"""
import os
from db_utils import get_db_connection, close_db_connection, create_table, batch_insert_data
from excel_utils import check_file_exists, load_excel_workbook, get_excel_sheet, read_cell_value
from utils import clean_description


excel_path = r"C:\Users\admin\Desktop\三江\核心产品返修率-20250611外发wqt.xlsx"  
sheet_name = "板子入库"         
CODE_COLUMN = 1               
DESC_COLUMN = 2              
db_config = {                  
    "host": "localhost",       
    "user": "root",            
    "password": "123456",      
    "database": "三江",        
    "port": 3306               
}
target_table = "material_info" # 目标表名


def main():
    """
    主函数：执行物料信息导入流程
    """
    # 1. 检查Excel文件存在性
    if not check_file_exists(excel_path):
        return

    # 2. 读取Excel文件
    workbook = load_excel_workbook(excel_path)
    if not workbook:
        return

    sheet = get_excel_sheet(workbook, sheet_name)
    if not sheet:
        workbook.close()
        return
    print(f"成功：加载Excel → 共 {sheet.max_row} 行数据")
    
    # 验证表头
    header = list(sheet.rows)[0]
    if not (header[CODE_COLUMN-1].value and header[DESC_COLUMN-1].value):
        print(f"警告：表头第{CODE_COLUMN}列或第{DESC_COLUMN}列为空！请检查列索引配置。")

    # 3. 提取并处理数据（从第2行开始）
    material_data = []
    for row_idx in range(2, sheet.max_row + 1):
        # 读取单元格数据
        raw_code = read_cell_value(sheet, row_idx, CODE_COLUMN)
        raw_desc = read_cell_value(sheet, row_idx, DESC_COLUMN)
        
        
        # 过滤空代码
        if not raw_code:
            print(f"  跳过：行{row_idx} → 物料代码为空")
            continue
        
        # 清洗描述
        cleaned_desc = clean_description(raw_desc)
        if not cleaned_desc:
            print(f"  跳过：行{row_idx} → 描述清洗后为空")
            continue
        
        material_data.append((raw_code, cleaned_desc))

    # 4. 数据去重
    unique_materials = {}
    for code, desc in material_data:
        if code not in unique_materials:
            unique_materials[code] = desc
    final_data = list(unique_materials.items())
    print(f"信息：去重后 → 有效数据共 {len(final_data)} 条")

    # 5. 数据库操作
    conn, cursor = get_db_connection(db_config)
    if not conn or not cursor:
        workbook.close()
        return

    try:
        # 创建表
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{target_table}` (
            `material_code` VARCHAR(50) PRIMARY KEY,
            `material_desc` VARCHAR(255) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        if not create_table(conn, cursor, target_table, create_table_sql):
            return

        # 插入数据
        if final_data:
            insert_sql = f"""
            INSERT INTO `{target_table}` (material_code, material_desc)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE material_desc = VALUES(material_desc);
            """
            success, fail = batch_insert_data(conn, cursor, insert_sql, final_data)
            
        else:
            print("警告：无有效数据可插入！")

    except Exception as e:
        print(f"错误：执行过程出错 → {str(e)}")
    finally:
        close_db_connection(conn, cursor)
        workbook.close()
        print("资源已释放")


if __name__ == "__main__":
    main()
    print("=== 操作执行完毕 ===")