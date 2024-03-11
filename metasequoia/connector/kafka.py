"""
包含对象：
- KafkaServer
- KafkaTopic

包含连接器：
"""

from typing import List, Optional

from metasequoia.connector.base import HostPort
from metasequoia.core.objects import SshTunnel

__all__ = ["KafkaServer", "KafkaTopic"]


class KafkaServer:
    """Kafka 集群对象"""

    def __init__(self,
                 bootstrap_servers: List[str],
                 ssh_tunnel: Optional["SshTunnel"] = None):
        self._bootstrap_servers = [server.strip() for server in bootstrap_servers]
        self._ssh_tunnel = ssh_tunnel

    @property
    def bootstrap_servers(self) -> List[str]:
        return self._bootstrap_servers

    @property
    def ssh_tunnel(self) -> SshTunnel:
        return self._ssh_tunnel

    def get_host_list(self) -> List[HostPort]:
        return [HostPort.create_by_url(server) for server in self._bootstrap_servers]


class KafkaTopic:
    """Kafka TOPIC"""

    def __init__(self, kafka_server: "KafkaServer", topic: str, group_id: Optional[str] = None):
        self.bootstrap_servers = kafka_server
        self.topic = topic
        self.group_id = group_id
