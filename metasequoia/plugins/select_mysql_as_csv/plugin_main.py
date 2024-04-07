"""
【MySQL】查询MySQL并作为csv文件下载
"""

from typing import Optional

import streamlit as st

from metasequoia.components.input_component import input_rds_instance, input_rds_schema
from metasequoia.connector.rds_connector import RdsInstance, MysqlConnector
from metasequoia.core import PluginBase
from metasequoia.utils.mysql_util import conn_select_sql_as_dict


class PluginSelectMysqlAsCsv(PluginBase):
    @staticmethod
    def page_name() -> str:
        return "【MySQL】查询MySQL并作为csv文件下载"

    def draw_page(self) -> None:
        rds_instance = input_rds_instance()
        rds_schema = input_rds_schema(rds_instance)
        select_sql = st.text_input(label="查询语句", value=None)

        if rds_instance is not None and rds_schema is not None and select_sql is not None:
            data = self.download_data_build_csv(rds_instance, rds_schema, select_sql)
        else:
            data = ""

        st.download_button(
            label="下载",
            data=data,
            file_name="download.csv",
            mime="text/csv"
        )

    @staticmethod
    def download_data_build_csv(rds_instance: Optional[RdsInstance],
                                rds_schema: Optional[str],
                                select_sql: Optional[str]):
        if rds_instance is None:
            st.error("请输入 RDS 实例")
            return
        if rds_schema is None:
            st.error("请输入 RDS 数据库")
            return
        if select_sql is None:
            st.error("请输入查询语句")
            return
        if not select_sql.startswith("SELECT"):
            st.error("不支持 SELECT 以外的其他语法")
            return
        if select_sql.count(";") > 1:
            st.error("不支持超过一个 SQL 语句")
            return
        if select_sql.count(";") == 1 and not select_sql.endswith(";"):
            st.error("不支持超过一个 SQL 语句")
            return

        columns = []
        results = []
        with MysqlConnector.create_by_rds_instance(rds_instance=rds_instance, schema=rds_schema) as conn:
            records = conn_select_sql_as_dict(conn, select_sql)
            if len(records) == 0:
                st.warning("查询到 0 条记录")
                return

            # 获取列名
            for column in records[0]:
                columns.append(column)
            results.append(",".join(columns))

            for record in records:
                values = [str(record[column]) for column in columns]
                results.append(",".join(values))

        return "\n".join(results)
