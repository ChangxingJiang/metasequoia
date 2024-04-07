from typing import Optional

from metasequoia.connector.rds_connector import RdsInstance, MysqlConnector
from metasequoia.connector.ssh_tunnel import SshTunnel


class DolphinMetaInstance(RdsInstance):
    """海豚调度元数据实例"""

    def __init__(self, host: str, port: int, user: str, passwd: str, db: str, ssh_tunnel: Optional[SshTunnel] = None):
        super().__init__(host, port, user, passwd, ssh_tunnel)
        self._db = db

    @property
    def db(self) -> str:
        return self._db

    def __repr__(self) -> str:
        return f"<DolphinMetaInstance host={self.host}, port={self.port}, user={self.user}, passwd={self.passwd}, ssh_tunnel={self.ssh_tunnel}, db={self.db}>"

    def __hash__(self):
        return hash((self.host, self.port, self.user, self.passwd, self.db, self.ssh_tunnel))

    def __eq__(self, other):
        return (isinstance(other, DolphinMetaInstance) and
                self.host == other.host and
                self.port == other.port and
                self.user == other.user and
                self.passwd == other.passwd and
                self.db == other.db and
                self.ssh_tunnel == other.ssh_tunnel)


class DolphinMetaConnector(MysqlConnector):
    """海豚调度元数据连接器"""

    def __init__(self, dolphin_scheduler_meta_info: DolphinMetaInstance):
        print(dolphin_scheduler_meta_info)
        super().__init__(dolphin_scheduler_meta_info,
                         schema=dolphin_scheduler_meta_info.db,
                         ssh_tunnel_info=dolphin_scheduler_meta_info.ssh_tunnel)
        self._dolphin_scheduler_meta_info = dolphin_scheduler_meta_info


if __name__ == "__main__":
    from metasequoia.core.config import configuration

    with DolphinMetaConnector(configuration.get_dolphin_meta_instance("demo")) as dolphin_conn:
        with dolphin_conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            print(cursor.fetchall())
