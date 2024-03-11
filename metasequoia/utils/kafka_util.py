import functools

from metasequoia.connector.kafka import KafkaServer, ConnKafkaAdminClient


@functools.lru_cache(maxsize=8)
def list_topics(kafka_server: KafkaServer):
    with ConnKafkaAdminClient(kafka_server) as kafka_admin_client:
        return kafka_admin_client.list_topics()
