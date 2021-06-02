from cmd.sql_cmd import SqlCmd
from core.core import DatabaseType, Query


class ShellCmd(SqlCmd):
    def exe(self):
        server = self.get_server()
        dc = server.db_conf
        if dc.server_type is DatabaseType.MONGO:
            self.printer.print_error_msg("MongoDB does not support shell option!")
            self.write_error_history("not support MongoDB")
            return
        conn = server.get_connection()
        val = input('db>').strip()
        while val not in {'quit', '!q', 'exit'}:
            if not (val == '' or val.strip() == ''):
                self._run_sql(Query(dc.server_type, None, val), conn)
            val = input('db>')
        conn.close()
        self.printer.output('Bye')
