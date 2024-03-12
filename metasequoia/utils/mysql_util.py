"""
MySQL 相关工具类
"""

import functools
from typing import Optional

import pymysql
import pymysql.cursors

from metasequoia.connector.rds_connector import RdsInstance, MysqlConn
from metasequoia.connector.ssh_tunnel import SshTunnel


@functools.lru_cache(maxsize=32)
def show_databases(rds_instance: RdsInstance, ssh_tunnel: Optional[SshTunnel] = None):
    """执行：SHOW DATABASES"""
    with MysqlConn.create_by_rds_instance(rds_instance=rds_instance, ssh_tunnel_info=ssh_tunnel) as conn:
        return conn_show_databases(conn)


@functools.lru_cache(maxsize=32)
def show_tables(rds_instance: RdsInstance, schema: str, ssh_tunnel: Optional[SshTunnel] = None):
    """执行：SHOW TABLES"""
    with MysqlConn.create_by_rds_instance(rds_instance=rds_instance, schema=schema, ssh_tunnel_info=ssh_tunnel) as conn:
        return conn_show_tables(conn, schema)


@functools.lru_cache(maxsize=32)
def show_create_table(rds_instance: RdsInstance, schema: str, table: str, ssh_tunnel: Optional[SshTunnel] = None):
    """执行：SHOW CREATE TABLE"""
    with MysqlConn.create_by_rds_instance(rds_instance=rds_instance, schema=schema, ssh_tunnel_info=ssh_tunnel) as conn:
        return conn_show_create_table(conn, table)


def conn_select_sql(conn: pymysql.Connection, sql: str):
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


def conn_show_databases(conn: pymysql.Connection):
    """执行 SHOW DATABASES 语句"""
    return [row["Database"] for row in conn_select_sql(conn, "SHOW DATABASES")]


def conn_show_tables(conn: pymysql.Connection, schema: str):
    """执行 SHOW TABLES 语句"""
    return [row[f"Tables_in_{schema}"] for row in conn_select_sql(conn, "SHOW TABLES")]


def conn_show_create_table(conn: pymysql.Connection, table: str) -> Optional[str]:
    """执行 SHOW CREATE TABLE 语句

    无法获取时返回 None，在以下场景下无法获取：
    1. 账号没有权限
    2. 表名不存在

    实现说明：
    1. 为使名称完全为数字的表执行正常，所以在表名外添加了引号
    """
    result = conn_select_sql(conn, f"SHOW CREATE TABLE `{table}`")[0]

    if "Create Table" in result:
        return result["Create Table"]
    else:  # 当表名不存在或没有权限时，没有 Create Table 列，只有 Error 列
        return None
