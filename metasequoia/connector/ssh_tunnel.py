"""
包含对象：
- SshTunnel
"""

from typing import Tuple

__all__ = ["SshTunnel"]


class SshTunnel:
    """SSH 隧道"""

    def __init__(self, host: str, port: int, username: str, pkey: str):
        self.host = host
        self.port = port
        self.username = username
        self.pkey = pkey

    @property
    def address(self) -> Tuple[str, int]:
        return self.host, self.port
