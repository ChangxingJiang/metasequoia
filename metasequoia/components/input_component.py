"""
用于输入的组件
"""

from typing import Optional

import streamlit as st

import metasequoia_connector as ms_conn
from metasequoia.components import cache_data
from metasequoia.components.cache_data import kafka_list_topics, kafka_list_consumer_groups
from metasequoia_streamlit.common import get_streamlit_default_key

__all__ = [
    "input_rds_name", "input_rds_schema", "input_rds_table_name", "input_rds_instance", "input_rds_table",
    "input_kafka_servers_name", "input_kafka_server", "input_kafka_topic", "input_kafka_group",
    "input_ssh_tunnel",
]


# ---------- RDS 相关输入组件 ----------


def input_rds_name() -> str:
    """【输入】内置 RDS 实例名称"""
    configuration = cache_data.load_configuration()
    return st.selectbox(label="请选择内置RDS实例",
                        options=configuration.get_rds_list(),
                        placeholder="请选择实例",
                        index=None,
                        format_func=configuration.get_rds_name,
                        key=get_streamlit_default_key())


def input_rds_schema(rds_instance: ms_conn.MysqlInstance,
                     default_schema: Optional[str] = None) -> Optional[str]:
    """【输入】RDS 数据库名"""
    databases = cache_data.show_databases(rds_instance) if rds_instance is not None else []
    index = databases.index(default_schema) if default_schema is not None and default_schema in databases else None
    return st.selectbox(label="数据库",
                        options=databases,
                        placeholder="请选择数据库",
                        index=index,
                        key=get_streamlit_default_key())


def input_rds_table_name(rds_instance: ms_conn.MysqlInstance,
                         schema: Optional[str],
                         default_table: Optional[str] = None) -> Optional[str]:
    """【输入】RDS 表名"""
    tables = cache_data.show_tables(rds_instance, schema) if rds_instance is not None and schema is not None else []
    index = tables.index(default_table) if default_table is not None and default_table in tables else None
    return st.selectbox(label="表",
                        options=tables,
                        placeholder="请选择表",
                        index=index,
                        key=get_streamlit_default_key())


def input_rds_instance(use_ssh: bool = False) -> Optional[ms_conn.MysqlInstance]:
    """【输入】RDS 实例"""
    configuration = cache_data.load_configuration()
    mode = st.radio(label="是否使用内置RDS实例",
                    options=["使用内置RDS实例", "自定义RDS实例"],
                    index=0,
                    key=get_streamlit_default_key())
    if mode == "使用内置RDS实例":
        # 使用内置 RDS 实例
        name = input_rds_name()
        if name is not None:
            return configuration.get_rds_instance(name)
    else:
        # 使用自定义 RDS 实例
        host = st.text_input(label="host", value=None)
        port = int(st.text_input(label="port", value="3306"))
        user = st.text_input(label="user", value=None)
        passwd = st.text_input(label="passwd", value=None)
        if use_ssh is True:
            ssh_tunnel = input_ssh_tunnel()
        else:
            ssh_tunnel = None
        if host is not None and port is not None and user is not None and passwd is not None:
            return ms_conn.MysqlInstance(host=host, port=port, user=user, passwd=passwd, ssh_tunnel=ssh_tunnel)
    return None


def input_rds_table(use_ssh: bool = False,
                    default_schema: Optional[str] = None,
                    default_table: Optional[str] = None) -> Optional[ms_conn.MysqlTable]:
    """【输入】RDS 表"""
    rds_instance = input_rds_instance(use_ssh=use_ssh)
    rds_schema = input_rds_schema(rds_instance, default_schema=default_schema)
    rds_table_name = input_rds_table_name(rds_instance, rds_schema, default_table=default_table)
    if rds_instance is not None and rds_schema is not None and rds_table_name is not None:
        rds_table = ms_conn.MysqlTable(instance=rds_instance, schema=rds_schema, table=rds_table_name)
        return rds_table
    else:
        return None


# ---------- Kafka 相关输入组件 ----------


def input_kafka_servers_name() -> str:
    """【输入】内置 Kafka 集群名称"""
    configuration = cache_data.load_configuration()
    return st.selectbox(label="请选择内置Kafka集群",
                        options=configuration.get_kafka_list(),
                        placeholder="请选择集群",
                        index=None,
                        key=get_streamlit_default_key())


def input_kafka_server(use_ssh: bool = False) -> Optional[ms_conn.KafkaServer]:
    """【输入】RDS 实例"""
    configuration = cache_data.load_configuration()
    mode = st.radio(label="是否使用内置Kafka集群",
                    options=["使用内置Kafka集群", "使用自定义Kafka集群"],
                    index=0,
                    key=get_streamlit_default_key())
    if mode == "使用内置Kafka集群":
        # 使用内置 RDS 实例
        name = input_kafka_servers_name()
        if name is not None:
            return configuration.get_kafka_server(name)
    else:
        bootstrap_servers = st.text_input(label="kafka集群",
                                          value=None,
                                          placeholder="例如：server1:9092,server2:9092")
        if use_ssh is True:
            ssh_tunnel = input_ssh_tunnel()
        else:
            ssh_tunnel = None
        if bootstrap_servers is not None:
            return ms_conn.KafkaServer(bootstrap_servers=bootstrap_servers.split(","), ssh_tunnel=ssh_tunnel)
    return None


def input_kafka_topic(use_ssh: bool = False) -> Optional[ms_conn.KafkaTopic]:
    """【输入】Kafka Topic"""
    kafka_server = input_kafka_server(use_ssh=use_ssh)
    topic_list = kafka_list_topics(kafka_server) if kafka_server is not None else []
    topic = st.selectbox(label="TOPIC",
                         options=topic_list,
                         placeholder="请选择TOPIC",
                         index=None,
                         key=get_streamlit_default_key())

    if kafka_server is not None and topic is not None:
        return ms_conn.KafkaTopic(kafka_server=kafka_server, topic=topic)
    else:
        return None


def input_kafka_group(use_ssh: bool = False) -> Optional[ms_conn.KafkaGroup]:
    """【输入】Kafka Group"""
    kafka_server = input_kafka_server(use_ssh=use_ssh)
    group_list = kafka_list_consumer_groups(kafka_server) if kafka_server is not None else []
    group = st.selectbox(label="消费者组",
                         options=group_list,
                         placeholder="请选择消费者组",
                         index=None,
                         key=get_streamlit_default_key())

    if kafka_server is not None and group is not None:
        return ms_conn.KafkaGroup(kafka_server=kafka_server, group=group)
    else:
        return None


# ---------- SSH 相关输入组件 ----------


def input_ssh_tunnel() -> Optional[ms_conn.DSMetaInstance]:
    """【输入】SSH 隧道"""
    configuration = cache_data.load_configuration()
    ssh_tunnel_name = st.selectbox(label="请选择SSH隧道",
                                   options=["不使用SSH隧道"] + configuration.get_ssh_list(),
                                   index=0,
                                   key=get_streamlit_default_key())
    if ssh_tunnel_name != "不使用SSH隧道":
        return configuration.get_ssh_tunnel(ssh_tunnel_name)
    else:
        return None
