# Metasequoia

## 安装方法

```bash
pip install metasequoia
```

## 使用方法

启动工具箱的方法：

```python
import sys

from metasequoia.application.application import MetaSequoiaApplication
from metasequoia.plugins import PluginGetKafkaTopicInfo


def main():
    """启动工具箱"""
    application_deploy_dir = sys.argv[2] if len(sys.argv) >= 3 else None

    application = MetaSequoiaApplication(sys.argv[1], application_deploy_dir)
    application.set_name("Metasequoia Demo")
    application.add_plugin("Kafka 工具", PluginGetKafkaTopicInfo)
    application.deploy_and_start()


if __name__ == "__main__":
    main()
```

如果需要自定义工具，可以参考 `PluginGetKafkaTopicOffset` 的逻辑，并使用 `add_plugin` 方法添加到 `MetaSequoiaApplication`
即可。

## 项目结构

- `application`：部署和应用
- `components`：组件
- `connector`：连接器
- `core`：核心逻辑
- `plugins`：内置插件
- `utils`：工具方法

# 变更历史

#### 0.1.2

- 新增：
    - 海豚调度元数据连接器，streamlit 的海豚输入组件
    - 新增查询 MySQL 并下载为 csv 文件的工具
- 优化：RDS 输入组件的 SSH 配置实现方法
- 修复：Hive 连接器的 Bug

#### 0.1.1

- 新增：
    - Kafka 连接器，streamlit 的 Kafka 输入组件
    - Hive 连接器，streamlit 的 Hive 输入组件
- 重构：
    - connector 模块，优化连接信息对象
    - components 模块，将 streamlit 组件从 PluginBase 中拆分出来
- 优化：
    - get_kafka_topic_info 插件，增加查询 TOPIC 配置功能
    - 非 DEV 模式默认关闭 SSH 通道

##### 0.1.0 初始化