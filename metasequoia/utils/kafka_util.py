from typing import List, Dict, Any

from kafka.admin import ConfigResource, ConfigResourceType
from kafka.protocol.admin import DescribeConfigsResponse_v2

from metasequoia.connector.kafka_connector import KafkaServer, KafkaTopic, KafkaGroup, ConnKafkaAdminClient


def list_topics(kafka_server: KafkaServer) -> List[str]:
    """列出 Kafka 集群中所有的 TOPIC"""
    with ConnKafkaAdminClient(kafka_server) as kafka_admin_client:
        return kafka_admin_client.list_topics()


def list_consumer_groups(kafka_server: KafkaServer) -> List[str]:
    """列出 Kafka 集群中所有的消费者组"""
    with ConnKafkaAdminClient(kafka_server) as kafka_admin_client:
        return [group_tuple[0] for group_tuple in kafka_admin_client.list_consumer_groups()]


def get_topic_configs(kafka_topic: KafkaTopic) -> Dict[str, Any]:
    """获取 TOPIC 的配置信息"""
    with ConnKafkaAdminClient(kafka_topic.kafka_server) as kafka_admin_client:
        configs = kafka_admin_client.describe_configs(config_resources=[
            ConfigResource(resource_type=ConfigResourceType.TOPIC, name=kafka_topic.topic)
        ], include_synonyms=False)

        topic_configs = {}

        config: DescribeConfigsResponse_v2
        for config in configs:
            for resource in config.get_item("resources"):
                _, _, _, _, config_entries = resource
                for config_name, config_value, _, _, _, _ in config_entries:
                    topic_configs[config_name] = config_value
        return topic_configs


def get_consume_topic(kafka_group: KafkaGroup) -> List[str]:
    """获取消费者组消费的 TOPIC 信息"""
    with ConnKafkaAdminClient(kafka_group.kafka_server) as kafka_admin_client:
        topic_set = set()
        for topic_partition in kafka_admin_client.list_consumer_group_offsets(kafka_group.group):
            topic_set.add(topic_partition.topic)
        return list(topic_set)
