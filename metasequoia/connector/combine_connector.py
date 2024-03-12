"""
组合的连接对象
"""

from metasequoia.connector.kafka_connector import KafkaTopic
from metasequoia.connector.rds_connector import RdsTable


class RdsTableWithMonitorKafkaTopic:
    """有 Kafka TOPIC 监听的 RDS 表"""

    def __init__(self, rds_table: RdsTable, kafka_topic: KafkaTopic):
        self._rds_table = rds_table
        self._kafka_topic = kafka_topic

    @property
    def rds_table(self) -> RdsTable:
        return self._rds_table

    @property
    def kafka_topic(self) -> KafkaTopic:
        return self._kafka_topic
