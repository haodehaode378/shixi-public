#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2025.07.31
# @Author : 王沁桐(3636617336@qq.com)
# @File : 输出数量.py
# @Description : 

import pandas as pd
import os
from datetime import datetime
from db_utils import create_db_connection, close_db_connection
from config import DB_CONFIG

def get_windows_desktop():
    """
    获取Windows系统桌面路径

    返回:
        str: 桌面路径字符串
    """
    return os.path.join(os.environ["USERPROFILE"], "Desktop")

def load_database_data():
    """
    从数据库加载物料和返修数据（过滤2023-01以前的数据）
    返回:
        tuple: (material_df, repair_df)
            material_df: 物料信息DataFrame
            repair_df: 返修数据DataFrame（仅包含2023-01及以后）
            若失败则返回(None, None)
    """
    try:
        conn = create_db_connection(**{k: DB_CONFIG[k] for k in ['host','user','password','database']})
        material_df = pd.read_sql("SELECT material_code, material_desc, board_code FROM material_stats", conn)
        repair_df = pd.read_sql("SELECT board_code, count, year, month FROM repair_stats", conn)
        
        # 过滤掉2023-01以前的数据
        repair_df = repair_df[
            (repair_df['year'] > 2023) |  # 年份大于2023的全部保留
            ((repair_df['year'] == 2023) & (repair_df['month'] >= 1))  # 2023年只保留1月及以后
        ]
        
        close_db_connection(conn)
        return material_df, repair_df
    except Exception as e:
        print(f"数据库读取失败: {e}")
        return None, None

def generate_pivot_report(material_df, repair_df):
    """
    生成透视表报表

    参数:
        material_df (pd.DataFrame): 物料信息DataFrame
        repair_df (pd.DataFrame): 返修数据DataFrame

    返回:
        pd.DataFrame: 透视表报表
    """
    if material_df is None or repair_df is None or repair_df.empty:
        return None
    
    # 1. 合并数据，生成 "YYYY-MM" 格式月份（自动补全月份为两位数）
    merged_data = pd.merge(
        material_df, 
        repair_df.assign(
            month_str=repair_df['year'].astype(str) + '-' + repair_df['month'].astype(str).str.zfill(2)
        ), 
        on='board_code', 
        how='left'
    )

    # 2. 动态提取所有 ≥2023-01 的月份，并按时间排序
    if not merged_data.empty and 'month_str' in merged_data.columns:
        # 提取所有非空月份
        all_months = merged_data['month_str'].dropna().unique()  
        # 过滤出 2023-01 及以后的月份
        valid_months = [
            m for m in all_months 
            if pd.to_datetime(m, format='%Y-%m') >= pd.to_datetime('2023-01', format='%Y-%m')
        ]
        # 按时间顺序排序
        valid_months_sorted = sorted(valid_months, key=lambda x: pd.to_datetime(x, format='%Y-%m'))
    else:
        valid_months_sorted = []  # 无有效数据时为空

    # 3. 生成透视表（动态适配所有有效月份）
    pivot_table = merged_data.pivot_table(
        index=['board_code', 'material_desc'],
        columns='month_str',
        values='count',
        aggfunc='sum',
        fill_value=0  # 空值填0
    ).reset_index()

    # 4. 强制按动态排序的月份列显示
    target_columns = ['board_code', 'material_desc'] + valid_months_sorted
    pivot_table = pivot_table.reindex(columns=target_columns, fill_value=0)

    # 5. 添加累计行（仅统计有效月份）
    if not pivot_table.empty and valid_months_sorted:
        total_row = pivot_table[valid_months_sorted].sum().to_dict()
        total_row.update({'board_code': '', 'material_desc': '累计'})
        pivot_table = pd.concat([pivot_table, pd.DataFrame([total_row])], ignore_index=True)

    return pivot_table

def export_to_desktop(report_df):
    """
    将报表保存到桌面（固定文件名为「月返修率返修统计.xlsx」，不含时间戳）

    参数:
        report_df (pd.DataFrame): 待导出的报表数据
    """
    if report_df is None or report_df.empty:
        print("无有效数据，无法保存")
        return
    
    # 固定文件名，移除时间戳
    file_name = "月返修率返修统计.xlsx"  
    desktop = get_windows_desktop()
    save_location = os.path.join(desktop, file_name)
    
    try:
        report_df.to_excel(save_location, index=False)
        print(f"报表已保存至桌面：\n{save_location}")
    except Exception as e:
        print(f"保存失败: {e}")

def main():
    """报表生成主函数"""
    material_data, repair_data = load_database_data()
    report = generate_pivot_report(material_data, repair_data)
    export_to_desktop(report)

if __name__ == "__main__":
    main()
