from pyhive import hive

from metasequoia.connector.hive_connector import HiveInstance, HiveConn


def execute(hive_instance: HiveInstance, sql: str):
    """执行 Hive 语句"""
    with HiveConn(hive_instance) as conn:
        with conn.cursor() as cursor:
            result = cursor.execute(sql)
            return result


def execute_and_fetch_result(hive_instance: HiveInstance, sql: str):
    """执行 Hive 语句"""
    with HiveConn(hive_instance) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            return result


def conn_execute(hive_conn: hive.Connection, sql: str):
    """执行 Hive 语句"""
    with hive_conn.cursor() as cursor:
        result = cursor.execute(sql)
        return result


def conn_execute_and_fetch_result(hive_conn: hive.Connection, sql: str):
    """执行 Hive 语句"""
    with hive_conn.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
        return result


def conn_get_create_table(hive_conn: hive.Connection, table_name: str):
    """获取建表语句"""
    with hive_conn.cursor() as cursor:
        cursor.execute(f"SHOW CREATE TABLE {table_name}")
        result = []
        for row in cursor.fetchall():
            result.append(row[0])
        return "\n".join(result)
