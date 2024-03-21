"""
海豚调度工具类

依赖：mysql_util
"""

from typing import List, Dict, Any, Tuple

import pymysql

from metasequoia.connector.dolphin_meta_connector import DolphinMetaInstance, DolphinMetaConnector
from metasequoia.utils import mysql_util


def list_projects(instance: DolphinMetaInstance) -> Tuple[Dict[str, Any], ...]:
    """获取海豚调度的项目列表"""
    with DolphinMetaConnector(instance) as conn:
        return conn_list_projects(conn)


def list_processes(instance: DolphinMetaInstance, project_code: str) -> Tuple[Dict[str, Any], ...]:
    """获取海豚调度指定项目的工作流列表"""
    with DolphinMetaConnector(instance) as conn:
        return conn_list_processes(conn, project_code)


def conn_list_projects(conn: pymysql.Connection) -> Tuple[Dict[str, Any], ...]:
    """获取海豚调度的项目列表"""
    return mysql_util.conn_select_sql_as_dict(
        conn, "SELECT `code`, `name` "
              "FROM t_ds_project"
    )


def conn_list_processes(conn: pymysql.Connection, project_code: str) -> Tuple[Dict[str, Any], ...]:
    """获取海豚调度指定项目的工作流列表"""
    return mysql_util.conn_select_sql_as_dict(
        conn, f"SELECT `code`, `name` "
              f"FROM t_ds_process_definition "
              f"WHERE `project_code` = '{project_code}'"
    )


def conn_get_process_name(conn: pymysql.Connection, project_code: str, process_code: str) -> str:
    """获取海豚调度指定工作流的名称"""
    return mysql_util.conn_select_sql_as_dict(
        conn, f"SELECT `name` "
              f"FROM t_ds_process_definition "
              f"WHERE `project_code` = '{project_code}' AND `code` = '{process_code}'"
    )[0].get("name")


def conn_get_tasks_of_processes(conn: pymysql.Connection,
                                project_code: str,
                                process_code: str
                                ) -> Tuple[Dict[str, Any], ...]:
    """获取海豚调度指定项目、工作流的任务列表"""
    return mysql_util.conn_select_sql_as_dict(
        conn, f"SELECT DISTINCT `post_task_code` "
              f"FROM t_ds_process_task_relation "
              f"WHERE `project_code` = '{project_code}' "
              f"  AND `process_definition_code`= '{process_code}'"
    )


def conn_get_task_params_of_need_dependent_task(conn: pymysql.Connection,
                                                project_code: str,
                                                task_code_list: List[str]) -> Tuple[Dict[str, Any], ...]:
    """获取海豚指定任务列表中依赖的其他工作流任务的任务参数"""
    task_code_str = ", ".join(f"'{task_code}'" for task_code in task_code_list)
    return mysql_util.conn_select_sql_as_dict(
        conn, f"SELECT `task_params` "
              f"FROM t_ds_task_definition "
              f"WHERE `project_code` = '{project_code}' "
              f"  AND `code` IN ({task_code_str}) "
              f"  AND `task_type` ='DEPENDENT'"
    )
