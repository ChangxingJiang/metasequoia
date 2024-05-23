"""
包含对象：
- SshTunnel
"""

import dataclasses
from typing import Tuple

__all__ = ["SshTunnel"]


@dataclasses.dataclass(slots=True, frozen=True, eq=True)
class SshTunnel:
    """SSH 隧道"""

    host: str = dataclasses.field(kw_only=True)
    port: int = dataclasses.field(kw_only=True)
    username: str = dataclasses.field(kw_only=True)
    pkey: str = dataclasses.field(kw_only=True)

    @property
    def address(self) -> Tuple[str, int]:
        return self.host, self.port
