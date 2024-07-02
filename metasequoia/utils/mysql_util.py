"""
MySQL 相关工具类
"""

from typing import Tuple, Dict, Any

import pymysql
import pymysql.cursors


def conn_select_sql_as_dict(conn: pymysql.Connection, sql: str) -> Tuple[Dict[str, Any], ...]:
    """通过 MySQL 根据 WHERE 条件抽取数据

    Parameters
    ----------
    conn : pymysql.Connection
        Mysql 连接
    sql : str
        SQL 语句

    Returns
    -------
    List[Dict[str, Any]]
        根据 sql 和 query_data 读取的数据
    """
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(sql)
        return cursor.fetchall()
