"""
包含对象：
- SshTunnel
"""

from typing import Tuple

__all__ = ["SshTunnel"]


class SshTunnel:
    """SSH 隧道"""

    def __init__(self, host: str, port: int, username: str, pkey: str):
        self._host = host
        self._port = port
        self._username = username
        self._pkey = pkey

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def username(self) -> str:
        return self._username

    @property
    def pkey(self) -> str:
        return self._pkey

    @property
    def address(self) -> Tuple[str, int]:
        return self.host, self.port

    def __hash__(self):
        return hash((self._host, self._port, self._username, self._pkey))

    def __eq__(self, other):
        return (isinstance(other, SshTunnel) and
                self._host == other._host and
                self._port == other._port and
                self._username == other._username and
                self._pkey == other._pkey)
