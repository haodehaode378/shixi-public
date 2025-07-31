#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2025.07.30
# @Author : 王沁桐(3636617336@qq.com)
# @File : utils.py
# @Description : 


"""通用工具函数模块
包含路径处理、数据清洗等通用功能
"""
import os
import re


def get_desktop_path() -> str:
    """
    获取系统桌面路径
    
    返回:
        桌面路径字符串
    """
    return os.path.join(os.path.expanduser("~"), "Desktop")


def clean_description(desc: str) -> str:
    """
    清洗描述文本：仅保留中文、英文、数字
    
    参数:
        desc: 原始描述文本
        
    返回:
        清洗后的文本
    """
    if not desc:
        return ""
    # 正则匹配：仅保留中文、英文、数字
    return re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9]', '', str(desc))