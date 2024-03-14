"""
用于输入的组件
"""

from typing import Optional

import streamlit as st

from metasequoia.components import cache_data
from metasequoia.components.cache_data import kafka_list_topics, kafka_list_consumer_groups
from metasequoia.connector.hive_connector import HiveInstance, HiveTable
from metasequoia.connector.kafka_connector import KafkaServer, KafkaTopic, KafkaGroup
from metasequoia.connector.rds_connector import RdsInstance, RdsTable
from metasequoia.connector.ssh_tunnel import SshTunnel
from streamlit_app import StreamlitPage

__all__ = ["input_rds_name", "input_rds_schema", "input_rds_table_name", "input_rds_instance", "input_rds_table",
           "input_kafka_servers_name", "input_kafka_server", "input_kafka_topic", "input_kafka_group",
           "input_ssh_tunnel"]


# ---------- RDS 相关输入组件 ----------


def input_rds_name() -> str:
    """【输入】内置 RDS 实例名称"""
    configuration = cache_data.load_configuration()
    return st.selectbox(label="请选择内置RDS实例",
                        options=configuration.get_rds_list(),
                        placeholder="请选择实例",
                        index=None,
                        format_func=configuration.get_rds_name,
                        key=StreamlitPage.get_streamlit_default_key())


def input_rds_schema(rds_instance: RdsInstance, ssh_tunnel: Optional[SshTunnel]) -> Optional[str]:
    """【输入】RDS 数据库名"""
    databases = cache_data.show_databases(rds_instance, ssh_tunnel) if rds_instance is not None else []
    return st.selectbox(label="数据库",
                        options=databases,
                        placeholder="请选择数据库",
                        index=None,
                        key=StreamlitPage.get_streamlit_default_key())


def input_rds_table_name(rds_instance: RdsInstance, schema: Optional[str], ssh_tunnel: Optional[SshTunnel]):
    """【输入】RDS 表名"""
    if rds_instance is not None and schema is not None:
        tables = cache_data.show_tables(rds_instance, schema, ssh_tunnel)
    else:
        tables = []
    return st.selectbox(label="表",
                        options=tables,
                        placeholder="请选择表",
                        index=None,
                        key=StreamlitPage.get_streamlit_default_key())


def input_rds_instance() -> Optional[RdsInstance]:
    """【输入】RDS 实例"""
    configuration = cache_data.load_configuration()
    mode = st.radio(label="是否使用内置RDS实例",
                    options=["使用内置RDS实例", "自定义RDS实例"],
                    index=0,
                    key=StreamlitPage.get_streamlit_default_key())
    if mode == "使用内置RDS实例":
        # 使用内置 RDS 实例
        name = input_rds_name()
        if name is not None:
            info = configuration.get_rds(name)
            return RdsInstance(host=info["host"], port=info["port"], user=info["user"], passwd=info["passwd"])
    else:
        # 使用自定义 RDS 实例
        host = st.text_input(label="host", value=None)
        port = int(st.text_input(label="port", value="3306"))
        user = st.text_input(label="user", value=None)
        passwd = st.text_input(label="passwd", value=None)
        if host is not None and port is not None and user is not None and passwd is not None:
            return RdsInstance(host=host, port=port, user=user, passwd=passwd)
    return None


def input_rds_table(use_ssh: bool = False) -> Optional[RdsTable]:
    """【输入】RDS 表"""
    rds_instance = input_rds_instance()
    if use_ssh is True:
        ssh_tunnel = input_ssh_tunnel()
    else:
        ssh_tunnel = None
    rds_schema = input_rds_schema(rds_instance, ssh_tunnel)
    rds_table_name = input_rds_table_name(rds_instance, rds_schema, ssh_tunnel)
    if rds_instance is not None and rds_schema is not None and rds_table_name is not None:
        rds_instance.set_ssh_tunnel(ssh_tunnel)
        rds_table = RdsTable(rds_instance, rds_schema, rds_table_name)
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
                        key=StreamlitPage.get_streamlit_default_key())


def input_kafka_server(use_ssh: bool = False) -> Optional[KafkaServer]:
    """【输入】RDS 实例"""
    configuration = cache_data.load_configuration()
    mode = st.radio(label="是否使用内置Kafka集群",
                    options=["使用内置Kafka集群", "使用自定义Kafka集群"],
                    index=0,
                    key=StreamlitPage.get_streamlit_default_key())
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
            return KafkaServer(bootstrap_servers.split(","), ssh_tunnel=ssh_tunnel)
    return None


def input_kafka_topic(use_ssh: bool = False) -> Optional[KafkaTopic]:
    """【输入】Kafka Topic"""
    kafka_server = input_kafka_server(use_ssh=use_ssh)
    topic_list = kafka_list_topics(kafka_server) if kafka_server is not None else []
    topic = st.selectbox(label="TOPIC",
                         options=topic_list,
                         placeholder="请选择TOPIC",
                         index=None,
                         key=StreamlitPage.get_streamlit_default_key())

    if kafka_server is not None and topic is not None:
        return KafkaTopic(kafka_server=kafka_server, topic=topic)
    else:
        return None


def input_kafka_group(use_ssh: bool = False) -> Optional[KafkaGroup]:
    """【输入】Kafka Group"""
    kafka_server = input_kafka_server(use_ssh=use_ssh)
    group_list = kafka_list_consumer_groups(kafka_server) if kafka_server is not None else []
    group = st.selectbox(label="消费者组",
                         options=group_list,
                         placeholder="请选择消费者组",
                         index=None,
                         key=StreamlitPage.get_streamlit_default_key())

    if kafka_server is not None and group is not None:
        return KafkaGroup(kafka_server=kafka_server, group=group)
    else:
        return None


# ---------- Hive 相关输入组件 ----------


def input_hive_instance_name() -> str:
    """【输入】内置 Hive 集群名称"""
    configuration = cache_data.load_configuration()
    return st.selectbox(label="请选择内置Hive集群",
                        options=configuration.get_hive_list(),
                        placeholder="请选择集群",
                        index=None,
                        key=StreamlitPage.get_streamlit_default_key())


def input_hive_instance(use_ssh: bool = False) -> Optional[HiveInstance]:
    """【输入】Hive 实例"""
    configuration = cache_data.load_configuration()
    mode = st.radio(label="是否使用内置Hive集群",
                    options=["使用内置Hive集群", "使用自定义Hive集群"],
                    index=0,
                    key=StreamlitPage.get_streamlit_default_key())
    if mode == "使用内置Hive集群":
        name = input_hive_instance_name()
        username = st.text_input(label="username", value=None)
        if name is not None and username is not None:
            hive_instance = configuration.get_hive_instance(name)
            hive_instance.set_hive_username(username)
            return hive_instance
    else:
        hosts = st.text_input(label="Hive集群", value=None)
        port = int(st.text_input(label="端口", value=None))
        if use_ssh is True:
            ssh_tunnel = input_ssh_tunnel()
        else:
            ssh_tunnel = None
        username = st.text_input(label="username", value=None)
        if hosts is not None:
            return HiveInstance(hosts=hosts.split(","), port=port, username=username, ssh_tunnel=ssh_tunnel)
    return None


def input_hive_table(use_ssh: bool = False) -> Optional[HiveTable]:
    """【输入】Hive 表"""
    hive_instance = input_hive_instance(use_ssh=use_ssh)
    hive_schema = st.text_input(label="schema", value=None)
    hive_table_name = st.text_input(label="table", value=None)
    if hive_instance is not None and hive_schema is not None and hive_table_name is not None:
        hive_table = HiveTable(hive_instance, hive_schema, hive_table_name)
        return hive_table
    else:
        return None


# ---------- SSH 相关输入组件 ----------


def input_ssh_tunnel() -> Optional[SshTunnel]:
    """【输入】SSH 隧道"""
    configuration = cache_data.load_configuration()
    ssh_tunnel_name = st.selectbox(label="请选择SSH隧道",
                                   options=["不使用SSH隧道"] + configuration.get_ssh_list(),
                                   index=0,
                                   key=StreamlitPage.get_streamlit_default_key())
    if ssh_tunnel_name != "不使用SSH隧道":
        return configuration.get_ssh_tunnel(ssh_tunnel_name)
    else:
        return None
