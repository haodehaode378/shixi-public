#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2025.07.31
# @Author : 王沁桐(3636617336@qq.com)
# @File : config.py
# @Description : 

# 修改 config.py
"""项目配置参数"""

# 数据库配置（合并连接参数和表名）
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "三江",
    "material_table": "material_stats_eri",
    "repair_table": "repair_stats_eri"
}

DB_CONFIG1 = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "三江",
}

# Excel文件配置
EXCEL_CONFIG = {
    "path": r"C:\Users\admin\Desktop\三江\核心产品返修率-20250611外发wqt.xlsx",
    "material_sheet": "改善统计",  # 物料数据所在工作表
    "repair_sheet": "返修"  # 返修数据所在工作表
}