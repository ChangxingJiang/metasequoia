"""
包含对象：
- KafkaServer
- KafkaTopic
- ConnKafkaAdminClient

包含连接器：
"""

from typing import List, Optional

import sshtunnel
from kafka.admin import KafkaAdminClient

from metasequoia.connector.base import HostPort
from metasequoia.connector.ssh_tunnel import SshTunnel

__all__ = ["KafkaServer", "KafkaTopic", "KafkaGroup", "ConnKafkaAdminClient"]


class KafkaServer:
    """Kafka 集群对象"""

    def __init__(self,
                 bootstrap_servers: List[str],
                 ssh_tunnel: Optional["SshTunnel"] = None
                 ):
        """

        Parameters
        ----------
        bootstrap_servers : List[str]
            Kafka 集群地址
        ssh_tunnel : Optional["SshTunnel"], default = None
            SSH 隧道
        """
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

    def __hash__(self):
        return hash((tuple(self._bootstrap_servers), self._ssh_tunnel))

    def __eq__(self, other):
        return (isinstance(other, KafkaServer) and
                self._bootstrap_servers == other._bootstrap_servers and
                self._ssh_tunnel == other._ssh_tunnel)


class KafkaTopic:
    """Kafka TOPIC"""

    def __init__(self, kafka_server: "KafkaServer", topic: str):
        self._kafka_server = kafka_server
        self._topic = topic

    @property
    def kafka_server(self) -> "KafkaServer":
        return self._kafka_server

    @property
    def topic(self) -> str:
        return self._topic

    def __hash__(self):
        return hash((self._kafka_server, self._topic))

    def __eq__(self, other):
        return (isinstance(other, KafkaTopic) and
                self._kafka_server == other._kafka_server and
                self._topic == other._topic)


class KafkaGroup:
    """Kafka 消费者组"""

    def __init__(self, kafka_server: "KafkaServer", group: str):
        self._kafka_server = kafka_server
        self._group = group

    @property
    def kafka_server(self) -> "KafkaServer":
        return self._kafka_server

    @property
    def group(self) -> str:
        return self._group

    def __hash__(self):
        return hash((self._kafka_server, self._group))

    def __eq__(self, other):
        return (isinstance(other, KafkaGroup) and
                self._kafka_server == other._kafka_server and
                self._group == other._group)


class ConnKafkaAdminClient:
    """根据 KafkaServer 对象创建 kafka-python 的 KafkaAdminClient 对象"""

    def __init__(self, kafka_server: KafkaServer):
        self.kafka_server = kafka_server  # MySQl 实例的配置

        # 初始化 MySQL 连接和 SSH 隧道连接
        self.kafka_admin_client = None
        self.ssh_tunnel = None

    def __enter__(self):
        """在进入 with as 语句的时候被 with 调用，返回值作为 as 后面的变量"""
        if self.kafka_server.ssh_tunnel is not None:
            # 启动 SSH 隧道
            self.ssh_tunnel = sshtunnel.SSHTunnelForwarder(
                ssh_address_or_host=self.kafka_server.ssh_tunnel.address,
                ssh_username=self.kafka_server.ssh_tunnel.username,
                ssh_pkey=self.kafka_server.ssh_tunnel.pkey,
                remote_bind_addresses=[(host_port.host, host_port.port)
                                       for host_port in self.kafka_server.get_host_list()]
            )
            self.ssh_tunnel.start()

            # 更新 Kafka 集群连接信息，令 Kafka 集群连接到 SSH 隧道
            addresses = [f"127.0.0.1:{self.ssh_tunnel.local_bind_port}"]
        else:
            addresses = [f"{host_port.host}:{host_port.port}" for host_port in self.kafka_server.get_host_list()]

        # 启动 Kafka 集群连接
        print(addresses)
        self.kafka_admin_client = KafkaAdminClient(bootstrap_servers=addresses)

        return self.kafka_admin_client

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.kafka_admin_client is not None:
            self.kafka_admin_client.close()
        if self.ssh_tunnel is not None:
            self.ssh_tunnel.close()
