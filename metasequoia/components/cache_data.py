"""
使用 streamlit 缓存的工具函数
"""

import datetime
from typing import Optional, List

import streamlit as st

from metasequoia.connector.dolphin_meta_connector import DolphinMetaInstance
from metasequoia.connector.kafka_connector import KafkaServer, KafkaTopic
from metasequoia.connector.rds_connector import RdsInstance
from metasequoia.connector.ssh_tunnel import SshTunnel
from metasequoia.core.config import Configuration, PROPERTIES_PATH
from metasequoia.utils import dolphin_util
from metasequoia.utils import kafka_util
from metasequoia.utils import mysql_util

__all__ = ["load_configuration", "show_databases", "show_tables", "show_create_table",
           "kafka_list_topics", "kafka_list_consumer_groups", "kafka_get_topic_configs"]


# ---------- 配置文件函数 ----------

@st.cache_resource
def load_configuration():
    return Configuration(PROPERTIES_PATH)  # 读取配置信息


# ---------- Mysql 工具函数 ----------

@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128, hash_funcs={RdsInstance: hash, SshTunnel: hash})
def list_database_and_table(rds_instance: RdsInstance, ignore_schema: List[str] = None):
    if ignore_schema is not None:
        ignore_schema_set = set(ignore_schema)
    else:
        ignore_schema_set = {"information_schema", "performance_schema", "sys"}

    result = []
    for schema in show_databases(rds_instance):
        if schema in ignore_schema_set:
            continue
        for table in show_tables(rds_instance, schema=schema):
            result.append({
                "schema": schema,
                "table": table
            })
    return result


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128, hash_funcs={RdsInstance: hash, SshTunnel: hash})
def show_databases(rds_instance: RdsInstance):
    return mysql_util.show_databases(rds_instance)


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128, hash_funcs={RdsInstance: hash, str: hash})
def show_tables(rds_instance: RdsInstance, schema: str):
    return mysql_util.show_tables(rds_instance, schema)


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128,
               hash_funcs={RdsInstance: hash, SshTunnel: hash, str: hash})
def show_create_table(rds_instance: RdsInstance, schema: str, table: str, ssh_tunnel: Optional[SshTunnel] = None):
    return mysql_util.show_create_table(rds_instance, schema, table, ssh_tunnel)


# ---------- Kafka 工具函数 ----------

@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128, hash_funcs={KafkaServer: hash})
def kafka_list_topics(kafka_server: KafkaServer):
    return kafka_util.list_topics(kafka_server)


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128, hash_funcs={KafkaServer: hash})
def kafka_list_consumer_groups(kafka_server: KafkaServer):
    return kafka_util.list_consumer_groups(kafka_server)


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128, hash_funcs={KafkaTopic: hash})
def kafka_get_topic_configs(kafka_topic: KafkaTopic):
    return kafka_util.get_topic_configs(kafka_topic)


# ---------- 海豚调度工具函数 ----------

@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128,
               hash_funcs={DolphinMetaInstance: hash, SshTunnel: hash})
def dolphin_meta_list_projects(instance: DolphinMetaInstance):
    return dolphin_util.list_projects(instance)


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128,
               hash_funcs={DolphinMetaInstance: hash, SshTunnel: hash})
def dolphin_meta_list_processes(instance: DolphinMetaInstance, project_code: str):
    return dolphin_util.list_processes(instance, project_code)
