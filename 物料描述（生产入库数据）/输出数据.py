#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2025.07.30
# @Author : 王沁桐(3636617336@qq.com)
# @File : 输出数据.py
# @Description : 


"""入库数据分析程序
从数据库读取物料和入库信息，生成月度入库数据透视表并导出到Excel
"""
import pandas as pd
import numpy as np
import os
from datetime import datetime
from db_utils import get_db_connection, close_db_connection, execute_query
from utils import get_desktop_path

# 数据库配置（根据实际环境修改）
DB_CONFIG = {
    'host': 'localhost',    
    'user': 'root',         
    'password': '123456',   
    'database': '三江'      
}
TABLE_MATERIAL = "material_info"  # 物料信息表
TABLE_STOCK = "material_stock"    # 入库信息表
OUTPUT_FILE = "入库数据分析.xlsx" # 输出文件名


def main():
    """
    入库数据分析程序
    """
    conn, _ = get_db_connection(DB_CONFIG)
    if not conn:
        return

    try:
        # 1. 查询数据（关联物料表和入库表）
        query = f"""
        SELECT 
            mi.material_code,   -- 物料编码
            mi.material_desc,   -- 物料描述
            ms.date,            -- 入库日期
            ms.quantity         -- 入库数量
        FROM `{TABLE_STOCK}` ms
        JOIN `{TABLE_MATERIAL}` mi 
            ON ms.material_code = mi.material_code;
        """
        cursor = execute_query(conn, query)
        if not cursor:
            return
            
        # 转换为DataFrame
        df = pd.DataFrame(cursor.fetchall())
        print(f"成功读取 {len(df)} 条入库数据")

        # 2. 数据预处理：日期格式化 + 提取月份（2023-01 格式）
        df['date'] = pd.to_datetime(df['date'])  
        df['month'] = df['date'].dt.strftime('%Y-%m')  

        # 3. 按月汇总（供透视表使用）
        monthly_summary = df.groupby(
            ['material_code', 'material_desc', 'month']
        )['quantity'].sum().reset_index()

        # 4. 构建透视表：物料为行，月份为列
        pivot_table = pd.pivot_table(
            monthly_summary,
            values='quantity',
            index=['material_code', 'material_desc'],
            columns='month',
            aggfunc=np.sum,
            fill_value=0
        )

        # 5. 添加月度合计行
        if not pivot_table.empty:
            # 计算每月总和
            monthly_totals = pivot_table.sum(axis=0)  
            # 构造合计行
            total_row = pd.DataFrame(monthly_totals).T  
            total_row.index = pd.MultiIndex.from_tuples(
                [("月度合计", "")],
                names=pivot_table.index.names
            )
            pivot_table = pd.concat([pivot_table, total_row])

            # 6. 月份列按时间排序
            sorted_months = sorted(
                pivot_table.columns, 
                key=lambda x: pd.to_datetime(x, format='%Y-%m')
            )
            pivot_table = pivot_table[sorted_months]

        # 7. 生成Excel
        desktop = get_desktop_path()
        output_path = os.path.join(desktop, OUTPUT_FILE)
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            pivot_table.to_excel(writer, sheet_name='透视表（物料×月份）')  

        print(f"\n分析完成！透视表已保存至：\n{output_path}")
        print(f"透视表包含：\n- {len(pivot_table)-1} 个物料行 + 1 个月度合计行\n- {len(pivot_table.columns)} 个月份列")

    except Exception as e:
        print(f"程序异常：{str(e)}")
    finally:
        close_db_connection(conn, None)


if __name__ == "__main__":
    main()