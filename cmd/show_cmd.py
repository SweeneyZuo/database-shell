from cmd.sql_cmd import SqlCmd
from core.core import DatabaseType, Query


class ShowCmd(SqlCmd):

    def get_sql(self, server, show_obj):
        if show_obj in {'database', 'databases'}:
            return server.get_list_databases_sql()
        elif show_obj in {'table', 'tables'}:
            return server.get_list_tables_sql(server.db_conf.database)
        elif show_obj in {'view', 'views'}:
            return server.get_list_views_sql(server.db_conf.database)
        return None

    def exe(self):
        server = self.get_server()
        dc = server.db_conf
        show_obj = self.params.get('show_obj', 'table')
        sql = self.get_sql(server, show_obj)
        if sql is None:
            self.printer.print_error_msg(f'invalid obj "{show_obj}"!')
            self.write_error_history(show_obj)
            return
        conn = server.get_connection()
        if dc.server_type is DatabaseType.MONGO:
            db = conn[dc.database]
            header, res = eval(sql)
            self.print_result_set(header, res, Query(dc.server_type, dc.database, sql, None, False))
        else:
            self._run_sql(Query(dc.server_type, dc.database, sql, fold=False), conn)
        conn.close()
