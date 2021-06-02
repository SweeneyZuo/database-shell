from cmd.sql_cmd import SqlCmd
from core.core import Query


class CountCmd(SqlCmd):
    name = 'count'

    def exe(self):
        tab_name, server = self.params['option_val'], self.get_server()
        sql = server.get_count_table_sql(tab_name)
        dc, conn = server.db_conf, server.get_connection()
        self._run_sql(Query(dc.server_type, dc.database, sql, tab_name, False), conn)
        conn.close()
