import sys

from metasequoia.application.application import MetaSequoiaApplication
from metasequoia.plugins import PluginGetKafkaTopicInfo


def main():
    """部署默认 Metasequoia 工具箱"""
    if len(sys.argv) < 2:
        raise KeyError("metasequoia 需要如下参数: \n"
                       "mode: 启动模式，需在 DEV、TEST、PRE、PROD 中选择其一\n"
                       "deploy_path: 部署地址（可选）")

    # 读取参数信息
    application_deploy_dir = sys.argv[2] if len(sys.argv) >= 3 else None

    # 构造默认服务并部署
    application = MetaSequoiaApplication(sys.argv[1], application_deploy_dir)
    application.set_name("Metasequoia Demo")
    application.add_plugin("Kafka 工具", PluginGetKafkaTopicInfo)
    application.deploy_and_start()


if __name__ == "__main__":
    main()
