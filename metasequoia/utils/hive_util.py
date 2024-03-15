from metasequoia.connector.hive_connector import HiveInstance, HiveConn


def execute(hive_instance: HiveInstance, sql: str):
    """执行 Hive 语句"""
    with HiveConn(hive_instance) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            return result
