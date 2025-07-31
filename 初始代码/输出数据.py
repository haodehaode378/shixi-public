import pymysql
import pandas as pd
import os
from datetime import datetime
import numpy as np

# 数据库配置（根据实际环境修改）
DB_CONFIG = {
    'host': 'localhost',    
    'user': 'root',         
    'password': '123456',   # 数据库密码
    'database': '三江'      # 数据库名
}
TABLE_MATERIAL = "material_info"  # 物料信息表
TABLE_STOCK = "material_stock"    # 入库信息表
OUTPUT_FILE = "入库数据分析.xlsx" # 输出文件名

def get_desktop_path():
    """获取桌面路径"""
    return os.path.join(os.path.expanduser("~"), "Desktop")

def main():
    try:
        # 1. 连接数据库
        conn = pymysql.connect(**DB_CONFIG, charset="utf8")
        print("数据库连接成功")

        # 2. 查询数据（关联物料表和入库表）
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
        df = pd.read_sql(query, conn)
        print(f"成功读取 {len(df)} 条入库数据")

        # 3. 数据预处理：日期格式化 + 提取月份（2023-01 格式）
        df['date'] = pd.to_datetime(df['date'])  
        df['month'] = df['date'].dt.strftime('%Y-%m')  

        # 4. 按月汇总（供透视表使用）
        monthly_summary = df.groupby(
            ['material_code', 'material_desc', 'month']
        )['quantity'].sum().reset_index()

        # 5. 构建透视表：物料为行，月份为列
        pivot_table = pd.pivot_table(
            monthly_summary,
            values='quantity',
            index=['material_code', 'material_desc'],  # 行：物料标识
            columns='month',                           # 列：月份
            aggfunc=np.sum,                            # 汇总方式：求和
            fill_value=0                               # 空值填充为0
        )

        # 6. 添加【月度合计行】（每月所有物料的入库和，放表格最下方）
        if not pivot_table.empty:
            # 计算每月总和（列方向求和）
            monthly_totals = pivot_table.sum(axis=0)  
            # 构造合计行（匹配行索引层级）
            total_row = pd.DataFrame(monthly_totals).T  
            total_row.index = pd.MultiIndex.from_tuples(
                [("月度合计", "")],  # 与行索引的两层结构对齐
                names=pivot_table.index.names
            )
            # 合并到透视表
            pivot_table = pd.concat([pivot_table, total_row])

            # 7. 月份列按时间排序（2023-01 → 2023-02 顺序）
            sorted_months = sorted(
                pivot_table.columns, 
                key=lambda x: pd.to_datetime(x, format='%Y-%m')
            )
            pivot_table = pivot_table[sorted_months]

        # 8. 生成Excel（仅保留透视表）
        desktop = get_desktop_path()
        output_path = os.path.join(desktop, OUTPUT_FILE)
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # 仅写入透视表，移除原始数据Sheet
            pivot_table.to_excel(writer, sheet_name='透视表（物料×月份）')  

        print(f"\n分析完成！透视表已保存至：\n{output_path}")
        print(f"透视表包含：\n- {len(pivot_table)-1} 个物料行 + 1 个月度合计行\n- {len(pivot_table.columns)} 个月份列（2023-01 格式）")

    except Exception as e:
        print(f"\n程序异常：{str(e)}")
    finally:
        # 确保数据库连接关闭
        if 'conn' in locals() and conn:
            conn.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    main()