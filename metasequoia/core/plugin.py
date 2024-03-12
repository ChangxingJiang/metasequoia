import abc
import inspect
from typing import Optional, Any

import streamlit as st

from metasequoia.connector.kafka_connector import KafkaServer, KafkaTopic
from metasequoia.connector.rds_connector import RdsInstance, RdsTable
from metasequoia.connector.ssh_tunnel import SshTunnel
from metasequoia.core import streamlit_cache_util
from metasequoia.core.config import configuration
from metasequoia.utils.mysql_util import show_databases, show_tables
from streamlit_app import StreamlitPage

__all__ = ["PluginBase"]


class PluginBase(StreamlitPage, abc.ABC):
    """插件的抽象基类"""

    def _input_kafka_servers_name(self) -> str:
        """【输入】内置 Kafka 集群名称"""
        return st.selectbox(label="请选择内置Kafka集群",
                            options=configuration.get_kafka_list(),
                            placeholder="请选择集群",
                            index=None,
                            key=self.get_streamlit_key())

    def input_kafka_server(self) -> Optional[KafkaServer]:
        """【输入】RDS 实例"""
        mode = st.radio(label="是否使用内置Kafka集群",
                        options=["使用内置Kafka集群", "使用自定义Kafka集群"],
                        index=0,
                        key=self.get_streamlit_key())
        if mode == "使用内置Kafka集群":
            # 使用内置 RDS 实例
            name = self._input_kafka_servers_name()
            if name is not None:
                return configuration.get_kafka_server(name)
        else:
            bootstrap_servers = st.text_input(label="kafka集群",
                                              value=None,
                                              placeholder="例如：server1:9092,server2:9092")
            ssh_tunnel = self.input_ssh_tunnel()
            if bootstrap_servers is not None:
                return KafkaServer(bootstrap_servers.split(","), ssh_tunnel=ssh_tunnel)
        return None

    def input_kafka_topic(self, is_need_group: bool = False) -> Optional[KafkaTopic]:
        """【输入】Kafka Topic

        Parameters
        ----------
        is_need_group : bool, default = False
            用户是否必须输入消费者组
        """
        kafka_server = self.input_kafka_server()
        topic_list = streamlit_cache_util.kafka_list_topics(kafka_server) if kafka_server is not None else []
        topic = st.selectbox(label="TOPIC",
                             options=topic_list,
                             placeholder="请选择TOPIC",
                             index=None,
                             key=self.get_streamlit_key())
        group_id = st.text_input(label="消费者组", value=None)

        if (kafka_server is not None and topic is not None and
                (is_need_group is False or (group_id is not None and group_id != ""))):
            return KafkaTopic(kafka_server=kafka_server,
                              topic=topic,
                              group_id=group_id)
        else:
            return None

    def input_rds_table(self) -> Optional[RdsTable]:
        """【输入】RDS 表"""
        rds_instance = self.input_rds_instance()
        ssh_tunnel = self.input_ssh_tunnel()
        rds_schema = self.input_rds_schema(rds_instance, ssh_tunnel)
        rds_table_name = self.input_rds_table_name(rds_instance, rds_schema, ssh_tunnel)
        if rds_instance is not None and rds_schema is not None and rds_table_name is not None:
            rds_instance.set_ssh_tunnel(ssh_tunnel)
            rds_table = RdsTable(rds_instance, rds_schema, rds_table_name)
            return rds_table
        else:
            return None

    def input_rds_instance(self) -> Optional[RdsInstance]:
        """【输入】RDS 实例"""
        mode = st.radio(label="是否使用内置RDS实例",
                        options=["使用内置RDS实例", "自定义RDS实例"],
                        index=0,
                        key=self.get_streamlit_key())
        if mode == "使用内置RDS实例":
            # 使用内置 RDS 实例
            name = self._input_rds_name()
            if name is not None:
                info = configuration.get_rds(name)
                return RdsInstance(host=info["host"], port=info["port"], user=info["user"], passwd=info["passwd"])
        else:
            # 使用自定义 RDS 实例
            host = st.text_input(label="host", value=None)
            port = int(st.text_input(label="port", value="3306"))
            user = st.text_input(label="user", value=None)
            passwd = st.text_input(label="passwd", value=None)
            if host is not None and port is not None and user is not None and passwd is not None:
                return RdsInstance(host=host, port=port, user=user, passwd=passwd)
        return None

    def _input_rds_name(self) -> str:
        """【输入】内置 RDS 实例名称"""
        return st.selectbox(label="请选择内置RDS实例",
                            options=configuration.get_rds_list(),
                            placeholder="请选择实例",
                            index=None,
                            format_func=configuration.get_rds_name,
                            key=self.get_streamlit_key())

    def input_ssh_tunnel(self) -> Optional[SshTunnel]:
        """【输入】SSH 隧道"""
        ssh_tunnel_name = st.selectbox(label="请选择SSH隧道",
                                       options=["不使用SSH隧道"] + configuration.get_ssh_list(),
                                       index=0,
                                       key=self.get_streamlit_key())
        if ssh_tunnel_name != "不使用SSH隧道":
            return configuration.get_ssh_tunnel(ssh_tunnel_name)
        else:
            return None

    def input_rds_schema(self, rds_instance: RdsInstance, ssh_tunnel: Optional[SshTunnel]) -> Optional[str]:
        """【输入】RDS 数据库名"""
        databases = show_databases(rds_instance, ssh_tunnel) if rds_instance is not None else []
        return st.selectbox(label="数据库",
                            options=databases,
                            placeholder="请选择数据库",
                            index=None,
                            key=self.get_streamlit_key())

    def input_rds_table_name(self, rds_instance: RdsInstance, schema: Optional[str], ssh_tunnel: Optional[SshTunnel]):
        """【输入】RDS 表名"""
        if rds_instance is not None and schema is not None:
            tables = show_tables(rds_instance, schema, ssh_tunnel)
        else:
            tables = []
        return st.selectbox(label="表",
                            options=tables,
                            placeholder="请选择表",
                            index=None,
                            key=self.get_streamlit_key())

    @staticmethod
    def check_is_not_none(obj: Optional[Any], prompt_text: str) -> None:
        """检查 obj 对象是否为 None，如果为 None 则输出提示文本

        Parameters
        ----------
        obj : Optional[Any]
            被检查的对象
        prompt_text : str
            提示文本
        """
        if obj is None:
            st.error(prompt_text)
            st.stop()

    def get_streamlit_key(self) -> str:
        """Streamlit 的 key 自动分配器：根据调用路径构造，理论上不存在同名的情况"""
        stack_list = []
        frame = inspect.currentframe()
        while frame.f_back:
            stack_list.append(f"{frame.f_back.f_code.co_name}:{frame.f_back.f_lineno}")
            if frame.f_back.f_code.co_name == "draw_page":  # 如果已经追溯到 draw_page，则不再继续递归栈信息
                break
            frame = frame.f_back
        stack_key = "-".join(reversed(stack_list))  # 根据调用链路，获取 draw_page 函数之后的唯一键
        return f"[{self.__class__.__name__}]{stack_key}"
