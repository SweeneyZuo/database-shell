from cmd.sql_cmd import SqlCmd
from core.core import Query


class ScanCmd(SqlCmd):
    def exe(self):
        server = self.get_server()
        tab_name = self.params['tab_name']
        sql = server.get_scan_table_sql(tab_name)
        dc, conn = server.db_conf, server.get_connection()
        self._run_sql(
            Query(dc.server_type, dc.database, sql, tab_name, self.fold, self.columns, self.limit_rows, self.human),
            conn)
        conn.close()
