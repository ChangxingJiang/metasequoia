import abc
from typing import Optional, Any, Dict

import streamlit as st

from metasequoia.application.mode import ApplicationMode
from streamlit_app import StreamlitPage

__all__ = ["PluginBase"]


class PluginBase(StreamlitPage, abc.ABC):
    """插件的抽象基类"""

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self._mode: ApplicationMode = ApplicationMode(self.params["mode"])
        print("mode:", self._mode)

    @property
    def mode(self) -> ApplicationMode:
        return self._mode

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
