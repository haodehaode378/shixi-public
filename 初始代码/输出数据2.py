import pandas as pd
import pymysql
import os
from datetime import datetime

# 数据库配置（需修改为实际信息）
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "三江"
}

def get_windows_desktop():
    """获取Windows桌面路径"""
    return os.path.join(os.environ["USERPROFILE"], "Desktop")

def load_database_data():
    """从数据库加载物料和返修数据"""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        # 读取物料主表（material_stats）
        material_sql = "SELECT material_code, material_desc, board_code FROM material_stats"
        material_df = pd.read_sql(material_sql, conn)
        
        # 读取返修数据（repair_stats）
        repair_sql = "SELECT board_code, count, year, month FROM repair_stats"
        repair_df = pd.read_sql(repair_sql, conn)
        
        # 转换月份格式为 "Jan-23" 格式（2023年1月）
        repair_df['month_str'] = pd.to_datetime(repair_df[['year', 'month']].assign(DAY=1)) \
            .dt.strftime('%b-%y')  # 格式：Jan-23（2023年1月）
        
        conn.close()
        return material_df, repair_df
    except Exception as e:
        print(f"数据库读取失败: {e}")
        return None, None

def generate_pivot_report(material_df, repair_df):
    """生成数据透视表（仅含2023年及以后数据）"""
    if material_df is None or repair_df is None:
        return None
    
    # 合并物料与返修数据
    merged_data = pd.merge(material_df, repair_df, on='board_code', how='left')
    
    # 创建数据透视表
    pivot_table = merged_data.pivot_table(
        index=['material_code', 'material_desc', 'board_code'],
        columns='month_str',
        values='count',
        aggfunc='sum',
        fill_value=0  # 空值填充为0
    ).reset_index()
    
    # 过滤2023年以前的月份（仅保留2023年及以后）
    if len(pivot_table.columns) > 3:
        material_cols = ['material_code', 'material_desc', 'board_code']
        month_cols = pivot_table.columns[3:]
        
        # 筛选逻辑：解析月份字符串，保留2023年及以后的列
        filtered_months = []
        for col in month_cols:
            try:
                month_year = pd.to_datetime(col, format='%b-%y')  # 转为日期对象
                if month_year.year >= 2023:  # 保留2023年及以后数据
                    filtered_months.append(col)
            except:
                continue  # 忽略格式错误的列
        
        if not filtered_months:
            print("无2023年及以后的有效月份数据")
            return None
        
        # 重构数据框（仅保留筛选后的月份列）
        pivot_table = pivot_table[material_cols + filtered_months]
    
    # 添加累计行（仅统计2023年及以后的数据）
    if not pivot_table.empty and filtered_months:
        total_row = pivot_table[filtered_months].sum().to_dict()  # 仅对筛选后的月份求和
        total_row.update({
            'material_code': '',
            'material_desc': '累计',
            'board_code': '累计'
        })
        pivot_table = pd.concat([pivot_table, pd.DataFrame([total_row])], ignore_index=True)
    
    # 按时间排序月份列（确保顺序正确）
    if len(filtered_months) > 0:
        sorted_months = sorted(filtered_months, key=lambda x: pd.to_datetime(x, format='%b-%y'))
        pivot_table = pivot_table[material_cols + sorted_months]
    
    return pivot_table

def export_to_desktop(report_df):
    """保存报表到Windows桌面（文件名带时间戳）"""
    if report_df is None or report_df.empty:
        print("无有效数据，无法保存")
        return
    
    # 生成唯一文件名（含时间戳）
    time_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"返修统计_2023及以后_{time_tag}.xlsx"
    desktop = get_windows_desktop()
    save_location = os.path.join(desktop, file_name)
    
    try:
        report_df.to_excel(save_location, index=False)
        print(f"报表已保存至桌面：\n{save_location}")
    except Exception as e:
        print(f"保存失败: {e}")

if __name__ == "__main__":
    # 主流程执行
    material_data, repair_data = load_database_data()
    report = generate_pivot_report(material_data, repair_data)
    export_to_desktop(report)