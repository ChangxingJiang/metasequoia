"""
各个连接器的基础通用对象
"""

from typing import Optional

__all__ = ["HostPort"]


class HostPort:
    """服务器地址和端口"""

    def __init__(self, host: str, port: Optional[int]):
        self._host = host
        self._port = port

    @staticmethod
    def create_by_url(url: str) -> "HostPort":
        if ":" not in url:
            return HostPort(url.strip(), None)
        else:
            host, port = url.split(":")
            return HostPort(url.strip(), int(port))

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> Optional[int]:
        return self._port
