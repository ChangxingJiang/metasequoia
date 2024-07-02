"""
使用 streamlit 缓存的工具函数
"""

import datetime
from typing import Optional, List

import streamlit as st

import metasequoia_connector as ms_conn

__all__ = ["load_configuration", "show_databases", "show_tables", "show_create_table",
           "kafka_list_topics", "kafka_list_consumer_groups", "kafka_get_topic_configs"]


# ---------- 配置文件函数 ----------

@st.cache_data(ttl=300)
def load_configuration():
    return ms_conn.from_environment()  # 读取配置信息


# ---------- Mysql 工具函数 ----------

@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128,
               hash_funcs={ms_conn.MysqlInstance: hash, ms_conn.SshTunnel: hash})
def list_database_and_table(rds_instance: ms_conn.MysqlInstance, ignore_schema: List[str] = None):
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


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128,
               hash_funcs={ms_conn.MysqlInstance: hash, ms_conn.SshTunnel: hash})
def show_databases(rds_instance: ms_conn.MysqlInstance):
    return ms_conn.mysql.show_databases(rds_instance)


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128, hash_funcs={ms_conn.MysqlInstance: hash, str: hash})
def show_tables(rds_instance: ms_conn.MysqlInstance, schema: str):
    return ms_conn.mysql.show_tables(rds_instance, schema)


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128,
               hash_funcs={ms_conn.MysqlInstance: hash, ms_conn.SshTunnel: hash, str: hash})
def show_create_table(rds_instance: ms_conn.MysqlInstance, schema: str, table: str,
                      ssh_tunnel: Optional[ms_conn.SshTunnel] = None):
    return ms_conn.mysql.show_create_table(rds_instance, schema, table, ssh_tunnel)


# ---------- Kafka 工具函数 ----------

@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128, hash_funcs={ms_conn.KafkaServer: hash})
def kafka_list_topics(kafka_server: ms_conn.KafkaServer):
    return ms_conn.kafka.list_topics(kafka_server)


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128, hash_funcs={ms_conn.KafkaServer: hash})
def kafka_list_consumer_groups(kafka_server: ms_conn.KafkaServer):
    return ms_conn.kafka.list_consumer_groups(kafka_server)


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128, hash_funcs={ms_conn.KafkaTopic: hash})
def kafka_get_topic_configs(kafka_topic: ms_conn.KafkaTopic):
    return ms_conn.kafka.get_topic_configs(kafka_topic)
