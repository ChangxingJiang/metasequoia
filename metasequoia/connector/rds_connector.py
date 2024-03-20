"""
包含对象：
- RdsInstance
- RdsTable

包含连接器：
- MysqlConn
"""

from typing import Optional

import pymysql
import sshtunnel

from metasequoia.connector.ssh_tunnel import SshTunnel

# from metasequoia.core.config import Configuration  # TODO 移除反向引用

__all__ = ["RdsInstance", "RdsTable", "MysqlConn"]


class RdsInstance:
    """RDS 实例"""

    def __init__(self, host: str, port: int, user: str, passwd: str, ssh_tunnel: Optional[SshTunnel] = None):
        self._host = host
        self._port = port
        self._user = user
        self._passwd = passwd
        self._ssh_tunnel = ssh_tunnel

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def user(self) -> str:
        return self._user

    @property
    def passwd(self) -> str:
        return self._passwd

    @property
    def ssh_tunnel(self) -> SshTunnel:
        return self._ssh_tunnel

    def set_ssh_tunnel(self, ssh_tunnel: Optional[SshTunnel]):
        self._ssh_tunnel = ssh_tunnel

    def __repr__(self) -> str:
        return f"<RdsTable host={self.host}, port={self.port}, user={self.user}, passwd={self.passwd}, ssh_tunnel={self.ssh_tunnel}>"


class RdsTable:
    """RDS 表"""

    def __init__(self, instance: "RdsInstance", schema: str, table: str):
        self._instance = instance
        self._schema = schema
        self._table = table

    @property
    def instance(self) -> "RdsInstance":
        return self._instance

    @property
    def schema(self) -> str:
        return self._schema

    @property
    def table(self) -> str:
        return self._table

    def __repr__(self) -> str:
        return f"<RdsTable instance={self.instance}, schema={self.schema}, table={self.table}>"


class MysqlConn:
    def __init__(self,
                 rds_instance: RdsInstance,
                 schema: Optional[str] = None,
                 ssh_tunnel_info: Optional[SshTunnel] = None,
                 connect_timeout: int = 5,
                 read_timeout: int = 10) -> None:
        """MySQL 连接的构造方法

        Parameters
        ----------
        rds_instance : RdsInstance
            MySQL 实例的配置
        schema : Optional[str], default = None
            数据库名称
        ssh_tunnel_info : Optional[SshTunnel], default = None
            SSH 隧道的配置，如果为 None 则不需要 SSH 隧道
        connect_timeout : int, default = 10
            连接超时时间
        read_timeout : int, default = 30
            读取超时时间
        """
        self.rds_info = rds_instance  # MySQl 实例的配置
        self.ssh_tunnel_info = ssh_tunnel_info  # SSH 隧道的配置
        self.schema = schema  # 数据库
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout

        # 初始化 MySQL 连接和 SSH 隧道连接
        self.mysql_conn = None
        self.ssh_tunnel = None

    @staticmethod
    def create_by_rds_instance(rds_instance: RdsInstance, schema: Optional[str] = None) -> "MysqlConn":
        """根据 RdsInstance 构造"""
        return MysqlConn(rds_instance=rds_instance, schema=schema, ssh_tunnel_info=rds_instance.ssh_tunnel)

    @staticmethod
    # TODO 待移除反向引用
    # def create_by_rds_name(configuration: Configuration, name: str, schema: Optional[str] = None) -> "MysqlConn":
    def create_by_rds_name(configuration, name: str, schema: Optional[str] = None) -> "MysqlConn":
        # 读取 MySQL 实例的配置
        rds_info_conf = configuration.get_rds(name)
        rds_info = RdsInstance(host=rds_info_conf["host"],
                               port=rds_info_conf["port"],
                               user=rds_info_conf["user"],
                               passwd=rds_info_conf["passwd"])

        # 读取 SSH 隧道的配置
        if "use_ssh" in rds_info_conf:
            ssh_tunnel_info_conf = configuration.get_ssh(rds_info_conf["use_ssh"])
            ssh_tunnel_info = SshTunnel(host=ssh_tunnel_info_conf["host"],
                                        port=ssh_tunnel_info_conf["port"],
                                        username=ssh_tunnel_info_conf["username"],
                                        pkey=ssh_tunnel_info_conf["pkey"])
        else:
            ssh_tunnel_info = None

        return MysqlConn(rds_instance=rds_info, schema=schema, ssh_tunnel_info=ssh_tunnel_info)

    def __enter__(self):
        """在进入 with as 语句的时候被 with 调用，返回值作为 as 后面的变量"""
        if self.ssh_tunnel_info is not None:
            # 启动 SSH 隧道
            self.ssh_tunnel = sshtunnel.SSHTunnelForwarder(
                ssh_address_or_host=(self.ssh_tunnel_info.host, self.ssh_tunnel_info.port),
                ssh_username=self.ssh_tunnel_info.username,
                ssh_pkey=self.ssh_tunnel_info.pkey,
                remote_bind_address=(self.rds_info.host, self.rds_info.port)
            )
            self.ssh_tunnel.start()

            # 更新 MySQL 连接信息，令 MySQL 连接到 SSH 隧道
            host = "127.0.0.1"
            port = self.ssh_tunnel.local_bind_port
        else:
            host = self.rds_info.host
            port = self.rds_info.port

        # 启动 MySQL 连接
        self.mysql_conn = pymysql.connect(
            host=host,
            port=port,
            user=self.rds_info.user,
            passwd=self.rds_info.passwd,
            db=self.schema,
            connect_timeout=self.connect_timeout,
            read_timeout=self.read_timeout
        )

        return self.mysql_conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.mysql_conn is not None:
            self.mysql_conn.close()
        if self.ssh_tunnel is not None:
            self.ssh_tunnel.close()
