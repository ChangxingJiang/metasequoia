"""
时间相关工具类
"""

import datetime


def get_current_date(fmt: str = "%Y-%m-%d") -> str:
    """获取当前日期"""
    return datetime.date.today().strftime(fmt)
