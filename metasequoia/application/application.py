"""
Metasequoia 启动主逻辑

具有如下功能：
1. 根据插件清单自动生成主页中的链接部分
2. 为插件自动生成标题和路径
3. 接受、处理启动模式
4. TODO 提供数据源管理器
5. TODO 提供配置和日志管理器
"""

__all__ = ["MetaSequoiaApplication"]

import collections
from typing import Optional, Union, Type, List, Dict

import streamlit_app

from metasequoia.application.main_page import MainPage
from metasequoia.application.mode import ApplicationMode
from metasequoia.core.plugin import PluginBase


class MetaSequoiaApplication(streamlit_app.Application):
    """Metasequoia 的启动主逻辑"""

    def __init__(self,
                 mode: Union[str, ApplicationMode],
                 deploy_dir: Optional[str] = None,
                 auto_clear_deploy_dir: bool = True):
        super().__init__(deploy_dir=deploy_dir, auto_clear_deploy_dir=auto_clear_deploy_dir)

        if isinstance(mode, str):
            mode = ApplicationMode(mode.upper())

        self._mode: ApplicationMode = mode

        self._application_name: Optional[str] = None
        self._sections: Dict[str, List[Type[PluginBase]]] = collections.defaultdict(list)  # 分组列表

    @property
    def mode(self) -> ApplicationMode:
        return self._mode

    def set_name(self, application_name: str):
        """设置应用名称"""
        self._application_name = application_name

    def add_plugin(self, section: str, plugin: Type[PluginBase]) -> None:
        """添加插件

        Parameters
        ----------
        section : str
            组件分组名称
        plugin : str
            组件类
        """
        self._sections[section].append(plugin)

    def create_main_page(self):
        """初始化主页"""
        print("self._sections:", self._sections)
        main_page_params = {
            "application_name": self._application_name,
            "sections": {
                section_name: [plugin.page_name() for plugin in plugin_list]
                for section_name, plugin_list in self._sections.items()
            }
        }
        print("初始化主页:", main_page_params)
        self.set_main_page(MainPage, params=main_page_params)

    def deploy(self):
        # 将插件中的页面添加到 Streamlit-app 中
        for plugin_list in self._sections.values():
            for plugin in plugin_list:
                self.append_page(plugin)

        # 设置主页
        self.create_main_page()

        # 执行部署逻辑
        super().deploy()



