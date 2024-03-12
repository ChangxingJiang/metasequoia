"""
使用 streamlit 缓存的工具函数
"""

import datetime

import streamlit as st

from metasequoia.connector.kafka_connector import *
from metasequoia.utils import kafka_util


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128, hash_funcs={KafkaServer: hash})
def kafka_list_topics(kafka_server: KafkaServer):
    return kafka_util.list_topics(kafka_server)


@st.cache_data(ttl=datetime.timedelta(minutes=30), max_entries=128, hash_funcs={KafkaTopic: hash})
def kafka_get_topic_configs(kafka_topic: KafkaTopic):
    return kafka_util.get_topic_configs(kafka_topic)
