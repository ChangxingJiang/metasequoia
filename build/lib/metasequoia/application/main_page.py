from typing import Dict, Any

import streamlit as st
from streamlit_app import StreamlitPage


class MainPage(StreamlitPage):
    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self._application_name = params.get("application_name")
        self._sections = params.get("sections")

    def page_name(self) -> str:
        return self._application_name

    def draw_page(self) -> None:
        self.draw_before_plugin_list()
        self.draw_plugin_list()
        self.draw_after_plugin_list()
        self.draw_copyright()

    def draw_before_plugin_list(self) -> None:
        """在插件列表之前打印的信息"""

    def draw_plugin_list(self) -> None:
        """打印插件列表"""
        if self._sections is not None:  # 有配置任何工具
            st.title(self._application_name)
            for section_name, plugin_name_list in self._sections.items():
                st.markdown(f"### {section_name}")
                for plugin_name in plugin_name_list:
                    st.markdown(f"- [{plugin_name}]({plugin_name})")

    def draw_after_plugin_list(self) -> None:
        """在插件列表之后打印的信息"""

    def draw_copyright(self) -> None:
        st.markdown(f"---\n"
                    f"\n"
                    f"{self._application_name}: power by metasequoia")
