from kafka.admin import ConfigResource, ConfigResourceType
from kafka.protocol.admin import DescribeConfigsResponse_v2

from metasequoia.connector.kafka_connector import KafkaServer, KafkaTopic, ConnKafkaAdminClient


def list_topics(kafka_server: KafkaServer):
    with ConnKafkaAdminClient(kafka_server) as kafka_admin_client:
        return kafka_admin_client.list_topics()


def get_topic_configs(kafka_topic: KafkaTopic):
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
