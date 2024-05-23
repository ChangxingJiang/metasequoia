from metasequoia_connector.connector import MysqlConnector
from metasequoia_connector.node import DSMetaInstance


class DolphinMetaConnector(MysqlConnector):
    """海豚调度元数据连接器"""

    def __init__(self, dolphin_scheduler_meta_info: DSMetaInstance):
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
