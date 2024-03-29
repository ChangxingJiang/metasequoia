"""
获取 Kafka 指定消费者组各分区的当前偏移量
"""

import streamlit as st
from kafka import KafkaConsumer, TopicPartition

from metasequoia.components import cache_data
from metasequoia.components.input_component import input_kafka_topic
from metasequoia.core import PluginBase


class PluginGetKafkaTopicInfo(PluginBase):
    @staticmethod
    def page_name() -> str:
        return "【Kafka】TOPIC信息查询工具"

    def draw_page(self) -> None:
        st.markdown("### Kafka TOPIC 信息查询工具\n"
                    "\n"
                    "本功能用于查询 Kafka TOPIC 配置信息，以及指定消费者组的各个分区当前消费指定 TOPIC 的偏移量。适用于定位个别分区消费中止或监控各分区消费速度是否平衡的场景。")

        st.divider()

        # 输入 KafkaTopic 对象
        kafka_topic = input_kafka_topic(use_ssh=self.mode.is_dev)

        st.divider()

        if st.button("查询 TOPIC 配置信息"):
            topic_configs = cache_data.kafka_get_topic_configs(kafka_topic)
            topic_config_frame = []
            for config_name, config_value in topic_configs.items():
                topic_config_frame.append({
                    "配置名": config_name,
                    "配置值": config_value
                })

            retention_ms = int(topic_configs["retention.ms"]) / 1000 / 3600
            st.markdown(f"##### 重要配置信息\n"
                        f"\n"
                        f"- 过期时间：{retention_ms}（小时）")

            st.markdown("##### 完整配置信息")
            st.table(topic_config_frame)

        st.divider()

        group_id = st.text_input(label="消费者组", value=None)

        # 创建 Kafka 连接
        if st.button("查询各分区偏移量"):
            self.check_is_not_none(kafka_topic, "未输入完整的 Kafka 集群、TOPIC 和消费者组信息")

            consumer = KafkaConsumer(group_id=group_id,
                                     bootstrap_servers=kafka_topic.kafka_server,
                                     api_version=(0, 11))

            # 查询并打印每个 TOPIC 的偏移量
            tp_lst = [TopicPartition(kafka_topic.topic, int(partition))
                      for partition in consumer.partitions_for_topic(kafka_topic.topic)]
            tp_begin_dict = consumer.beginning_offsets(tp_lst)
            tp_end_dict = consumer.end_offsets(tp_lst)
            consumer.assign(tp_lst)
            data_list = []
            for tp in tp_lst:
                data_list.append({
                    "partition(分区)": tp.partition,
                    "start_offset(最小偏移量)": tp_begin_dict[tp],
                    "current_offset(当前偏移量)": consumer.position(tp),
                    "end_offset(最大偏移量)": tp_end_dict[tp],
                    "lag(延迟偏移量)": tp_end_dict[tp] - consumer.position(tp)
                })
            consumer.close()

            # 展示各分区偏移量
            st.table(data_list)
