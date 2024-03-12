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
from metasequoia.plugins import PluginGetKafkaTopicOffset


def main():
    """启动工具箱"""
    application_deploy_dir = sys.argv[2] if len(sys.argv) >= 3 else None

    application = MetaSequoiaApplication(sys.argv[1], application_deploy_dir)
    application.set_name("Metasequoia Demo")
    application.add_plugin("Kafka 工具", PluginGetKafkaTopicOffset)
    application.deploy_and_start()


if __name__ == "__main__":
    main()
```

如果需要自定义工具，可以参考 `PluginGetKafkaTopicOffset` 的逻辑，并使用 `add_plugin` 方法添加到 `MetaSequoiaApplication`
即可。

## 项目结构

- `application`：部署和应用
- `connector`：连接器
- `core`：核心逻辑
- `plugins`：内置插件
- `utils`：工具方法
