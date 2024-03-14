"""
使用 streamlit 缓存的工具函数
"""

import datetime
from typing import Optional

import streamlit as st

from metasequoia.connector.kafka_connector import KafkaServer, KafkaTopic
from metasequoia.connector.rds_connector import RdsInstance
from metasequoia.connector.ssh_tunnel import SshTunnel
from metasequoia.core.config import Configuration, PROPERTIES_PATH
from metasequoia.utils import kafka_util
from metasequoia.utils import mysql_util

__all__ = ["load_configuration", "show_databases", "show_tables", "show_create_table",
           "kafka_list_topics", "kafka_get_topic_configs"]


# ---------- 配置文件函数 ----------

@st.cache_resource
def load_configuration():
    return Configuration(PROPERTIES_PATH)  # 读取配置信息


# ---------- Mysql 工具函数 ----------

@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128, hash_funcs={RdsInstance: hash, SshTunnel: hash})
def show_databases(rds_instance: RdsInstance, ssh_tunnel: Optional[SshTunnel] = None):
    return mysql_util.show_databases(rds_instance, ssh_tunnel)


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128,
               hash_funcs={RdsInstance: hash, SshTunnel: hash, str: hash})
def show_tables(rds_instance: RdsInstance, schema: str, ssh_tunnel: Optional[SshTunnel] = None):
    return mysql_util.show_tables(rds_instance, schema, ssh_tunnel)


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128,
               hash_funcs={RdsInstance: hash, SshTunnel: hash, str: hash})
def show_create_table(rds_instance: RdsInstance, schema: str, table: str, ssh_tunnel: Optional[SshTunnel] = None):
    return mysql_util.show_create_table(rds_instance, schema, table, ssh_tunnel)


# ---------- Kafka 工具函数 ----------

@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128, hash_funcs={KafkaServer: hash})
def kafka_list_topics(kafka_server: KafkaServer):
    return kafka_util.list_topics(kafka_server)


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128, hash_funcs={KafkaTopic: hash})
def kafka_get_topic_configs(kafka_topic: KafkaTopic):
    return kafka_util.get_topic_configs(kafka_topic)
