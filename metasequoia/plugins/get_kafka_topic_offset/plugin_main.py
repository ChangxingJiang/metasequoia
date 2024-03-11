"""
获取 Kafka 指定消费者组各分区的当前偏移量
"""

import streamlit as st
from kafka import KafkaConsumer, TopicPartition

from metasequoia.core import PluginBase


class PluginGetKafkaTopicOffset(PluginBase):
    @staticmethod
    def page_name() -> str:
        return "【Kafka】TOPIC信息查询工具"

    def draw_page(self) -> None:
        st.markdown("### Kafka TOPIC 信息查询工具\n"
                    "\n"
                    "本功能用于查询 Kafka 指定消费者组的各个分区当前消费指定 TOPIC 的偏移量。适用于定位个别分区消费中止或监控各分区消费速度是否平衡的场景。")

        st.divider()

        # 输入 KafkaTopic 对象
        kafka_topic = self.input_kafka_topic(is_need_group=True)

        st.divider()

        # 创建 Kafka 连接
        if st.button("查询各分区偏移量"):
            self.check_is_not_none(kafka_topic, "未输入完整的 Kafka 集群、TOPIC 和消费者组信息")

            consumer = KafkaConsumer(group_id=kafka_topic.group,
                                     bootstrap_servers=kafka_topic.bootstrap_servers,
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
