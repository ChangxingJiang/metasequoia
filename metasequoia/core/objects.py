from typing import List, Optional

__all__ = ["KafkaTopic", "RdsInstance", "RdsTable", "SshTunnel", "RdsTableWithMonitorKafkaTopic"]


# TODO 将各个属性修改为不可直接修改


class KafkaServer:
    def __init__(self, bootstrap_servers: List[str],
                 ssh_tunnel: Optional["SshTunnel"] = None):
        self.bootstrap_servers = bootstrap_servers
        self.ssh_tunnel = ssh_tunnel


class KafkaTopic:
    """Kafka TOPIC"""

    def __init__(self, kafka_server: "KafkaServer", topic: str, group_id: Optional[str] = None):
        self.bootstrap_servers = kafka_server
        self.topic = topic
        self.group_id = group_id


class RdsInstance:
    """RDS 实例"""

    def __init__(self, host: str, port: int, user: str, passwd: str, ssh_tunnel: Optional["SshTunnel"] = None):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.ssh_tunnel = ssh_tunnel


class RdsTable:
    """RDS 表"""

    def __init__(self, instance: "RdsInstance", schema: str, table: str):
        self.instance = instance
        self.schema = schema
        self.table = table


class SshTunnel:
    """SSH 隧道"""

    def __init__(self, host: str, port: int, username: str, pkey: str):
        self.host = host
        self.port = port
        self.username = username
        self.pkey = pkey


class RdsTableWithMonitorKafkaTopic:
    """有 Kafka TOPIC 监听的 RDS 表"""

    def __init__(self, rds_table: "RdsTable", kafka_topic: "KafkaTopic"):
        self.rds_table = rds_table
        self.kafka_topic = kafka_topic
