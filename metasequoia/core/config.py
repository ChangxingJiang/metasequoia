import json
import os
from typing import Dict, Any, List

from metasequoia.connector.dolphin_meta_connector import DolphinMetaInstance
from metasequoia.connector.hive_connector import HiveInstance
from metasequoia.connector.kafka_connector import KafkaServer
from metasequoia.connector.rds_connector import RdsInstance
from metasequoia.connector.ssh_tunnel import SshTunnel

__all__ = ["configuration", "Configuration", "MODE"]

PROPERTIES_PATH = os.environ.get("PINALE_CONFIG_PATH")
MODE = "dev"


# TODO 将配置输出到文件：https://www.cnblogs.com/wengzx/p/18019494


class Configuration:
    ENCODING = "UTF-8"  # 默认编码格式

    def __init__(self, path: str):
        """配置文件路径"""
        self.path = path
        self._configuration = None
        self.load()

    def load(self):
        with open(self.path, "r", encoding=self.ENCODING) as file:
            self._configuration = json.load(file)

    # ---------- 读取 RDS 相关配置 ----------

    def get_rds_list(self) -> List[str]:
        """获取 RDS 列表"""
        return list(self._configuration.get("RDS", {}).keys())

    def get_rds(self, name: str, mode: str = MODE) -> Dict[str, Any]:
        """获取 RDS 信息"""
        return self._confirm_params("RDS", self._get_section("RDS", name, mode), ["host", "port", "user"])

    def get_rds_instance(self, name: str) -> RdsInstance:
        """获取 RdsInstance 对象"""
        rds_info = self.get_rds(name)
        ssh_tunnel = self.get_ssh_tunnel(rds_info["use_ssh"]) if rds_info.get("use_ssh") else None
        return RdsInstance(host=rds_info["host"],
                           port=rds_info["port"],
                           user=rds_info["user"],
                           passwd=rds_info["passwd"],
                           ssh_tunnel=ssh_tunnel)

    def get_rds_name(self, name: str) -> str:
        """获取 RDS 的名称"""
        return self._configuration["RDS"][name].get("_name", "")

    # ---------- 读取 SSH 相关配置 ----------

    def get_ssh(self, name: str, mode: str = MODE) -> Dict[str, Any]:
        """获取 SSH 信息"""
        return self._confirm_params("SSH", self._get_section("SSH", name, mode), ["host", "port"])

    def get_ssh_tunnel(self, name: str) -> SshTunnel:
        """获取 SshTunnel 对象"""
        ssh_info = self.get_ssh(name)
        return SshTunnel(host=ssh_info["host"],
                         port=ssh_info["port"],
                         username=ssh_info["username"],
                         pkey=ssh_info["pkey"])

    def get_ssh_list(self):
        """获取 SSH 列表"""
        return list(self._configuration["SSH"].keys())

    # ---------- 读取 Kafka 相关配置 ----------

    def get_kafka_list(self) -> List[str]:
        """获取 Kafka 列表"""
        return list(self._configuration.get("Kafka", {}).keys())

    def get_kafka_info(self, name: str, mode: str = MODE) -> Dict[str, Any]:
        return self._confirm_params("Kafka", self._get_section("Kafka", name, mode), ["bootstrap_servers"])

    def get_kafka_server(self, name: str) -> KafkaServer:
        """获取 Kafka Servers 对象"""
        kafka_info = self.get_kafka_info(name)
        ssh_tunnel = self.get_ssh_tunnel(kafka_info["use_ssh"]) if kafka_info.get("use_ssh") else None
        return KafkaServer(bootstrap_servers=kafka_info["bootstrap_servers"],
                           ssh_tunnel=ssh_tunnel)

    # ---------- 读取 Hive 相关配置 ----------

    def get_hive_list(self) -> List[str]:
        """获取 Hive 列表"""
        return list(self._configuration.get("Hive", {}).keys())

    def get_hive_info(self, name: str, mode: str = MODE) -> Dict[str, Any]:
        return self._confirm_params("Hive", self._get_section("Hive", name, mode), ["hosts", "port"])

    def get_hive_instance(self, name: str) -> HiveInstance:
        """获取 Hive 列表"""
        hive_info = self.get_hive_info(name)
        ssh_tunnel = self.get_ssh_tunnel(hive_info["use_ssh"]) if hive_info.get("use_ssh") else None
        return HiveInstance(hosts=hive_info["hosts"], port=hive_info["port"],
                            ssh_tunnel=ssh_tunnel)

    # ---------- 读取 DolphinScheduler 相关配置 ----------

    def get_dolphin_meta_list(self) -> List[str]:
        """获取海豚调度元数据清单"""
        return list(self._configuration["DolphinMeta"])

    def get_dolphin_meta_info(self, name: str, mode: str = MODE) -> Dict[str, Any]:
        """获取海豚调度元数据信息"""
        return self._get_section("DolphinMeta", name, mode)

    def get_dolphin_meta_instance(self, name: str) -> DolphinMetaInstance:
        """获取海豚调度元数据的 DolphinMetaInstance 对象"""
        dolphin_meta_info = self.get_dolphin_meta_info(name)
        ssh_tunnel = self.get_ssh_tunnel(dolphin_meta_info["use_ssh"]) if dolphin_meta_info.get("use_ssh") else None
        return DolphinMetaInstance(
            host=dolphin_meta_info["host"],
            port=dolphin_meta_info["port"],
            user=dolphin_meta_info["user"],
            passwd=dolphin_meta_info["passwd"],
            db=dolphin_meta_info["db"],
            ssh_tunnel=ssh_tunnel
        )

    # ---------- 其他工具方法 ----------

    def _get_section(self, section: str, name: str, mode: str) -> Dict[str, Any]:
        """获取每种类型的配置信息数据"""
        if section not in self._configuration or name not in self._configuration[section]:
            return {}
        config = self._configuration[section][name].get("mode:common", {}).copy()  # 先加载通用配置
        config.update(self._configuration[section][name].get(f"mode:{mode}", {}))  # 然后再加对应模式的配置
        print(self._configuration[section][name])
        return config

    @staticmethod
    def _confirm_params(section: str, config: Dict[str, Any], params: List[str]) -> Dict[str, Any]:
        """检查参数是否满足"""
        for param in params:
            assert param in config, f"param {param} not in {section} config"
        return config

    @classmethod
    def make_templates(cls, path: str):
        """在指定路径下生成模板配置文件"""
        if os.path.exists(path):
            raise ValueError(f"配置文件已存在({path}),创建模板文件失败!")
        with open(path, "w", encoding=cls.ENCODING) as file:
            file.write(json.dumps(
                {"RDS": {"localhost": {"host": "localhost", "port": 3306, "user": "root", "passwd": "123456"}},
                 "SSH": {"demo": {"host": "...", "port": "...", "username": "...", "pkey": "..."}}}))
            print("配置文件模板生成完成")


configuration = Configuration(PROPERTIES_PATH)  # 实现配置信息的单例
