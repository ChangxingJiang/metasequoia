"""
Hive 连接器
"""

import random
from typing import List, Optional

import sshtunnel
from pyhive import hive

from metasequoia.connector.ssh_tunnel import SshTunnel

__all__ = ["HiveInstance", "HiveTable", "HiveConn"]


class HiveInstance:
    """Hive 实例"""

    def __init__(self, hosts: List[str], port: int, username: Optional[str] = None,
                 ssh_tunnel: Optional[SshTunnel] = None):
        self._hosts = hosts
        self._port = port
        self._username = username
        self._ssh_tunnel = ssh_tunnel

    @property
    def hosts(self) -> List[str]:
        return self._hosts

    @property
    def port(self) -> int:
        return self._port

    @property
    def username(self) -> str:
        return self._username

    @property
    def ssh_tunnel(self) -> SshTunnel:
        return self._ssh_tunnel

    def set_hive_username(self, username: str) -> None:
        self._username = username


class HiveTable:
    """Hive 表"""

    def __init__(self, instance: "HiveInstance", schema: str, table: str):
        self._instance = instance
        self._schema = schema
        self._table = table

    @property
    def instance(self) -> "HiveInstance":
        return self._instance

    @property
    def schema(self) -> str:
        return self._schema

    @property
    def table(self) -> str:
        return self._table


class HiveConn:
    def __init__(self,
                 hive_instance: "HiveInstance",
                 schema: Optional[str] = None,
                 ssh_tunnel_info: Optional[SshTunnel] = None) -> None:
        """MySQL 连接的构造方法

        Parameters
        ----------
        hive_instance : RdsInstance
            MySQL 实例的配置
        schema : Optional[str], default = None
            数据库名称
        ssh_tunnel_info : Optional[SshTunnel], default = None
            SSH 隧道的配置，如果为 None 则不需要 SSH 隧道
        """
        self.hive_instance_info = hive_instance  # MySQl 实例的配置
        self.ssh_tunnel_info = ssh_tunnel_info  # SSH 隧道的配置
        self.schema = schema  # 数据库

        # 初始化 Hive 连接和 SSH 隧道连接
        self.hive_conn = None
        self.ssh_tunnel = None

    @staticmethod
    def create_by_hive_instance(hive_instance: HiveInstance,
                                schema: Optional[str] = None,
                                ssh_tunnel_info: Optional[SshTunnel] = None) -> "HiveConn":
        """根据 HiveInstance 构造"""
        return HiveConn(hive_instance=hive_instance, schema=schema, ssh_tunnel_info=ssh_tunnel_info)

    def __enter__(self):
        """在进入 with as 语句的时候被 with 调用，返回值作为 as 后面的变量

        因为 pyhive 不支持连接多个 Hive Client 的集群，因此在其中随机选择
        """

        choose_host = random.choice(self.hive_instance_info.hosts)

        if self.ssh_tunnel_info is not None:
            # 启动 SSH 隧道
            self.ssh_tunnel = sshtunnel.SSHTunnelForwarder(
                ssh_address_or_host=(self.ssh_tunnel_info.host, self.ssh_tunnel_info.port),
                ssh_username=self.ssh_tunnel_info.username,
                ssh_pkey=self.ssh_tunnel_info.pkey,
                remote_bind_address=(choose_host, self.hive_instance_info.port)
            )
            self.ssh_tunnel.start()

            # 更新 MySQL 连接信息，令 MySQL 连接到 SSH 隧道
            host = "127.0.0.1"
            port = self.ssh_tunnel.local_bind_port
        else:
            host = choose_host
            port = self.hive_instance_info.port

        self.hive_conn = hive.Connection(host=host, port=port, username=self.hive_instance_info.username)

        return self.hive_conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.hive_conn is not None:
            self.hive_conn.close()
        if self.ssh_tunnel is not None:
            self.ssh_tunnel.close()
