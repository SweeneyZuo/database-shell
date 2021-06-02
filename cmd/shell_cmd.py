from cmd.sql_cmd import SqlCmd
from core.core import DatabaseType, Query


class ShellCmd(SqlCmd):
    name = 'shell'

    def exe(self):
        server = self.get_server()
        dc = server.db_conf
        if dc.server_type is DatabaseType.MONGO:
            self.printer.print_error_msg("Not Support MongoDB!")
            self.write_error_history("Not Support MongoDB")
            return
        conn = server.get_connection()
        val = input('db>').strip()
        while val not in {'quit', '!q', 'exit'}:
            if not (val == '' or val.strip() == ''):
                self._run_sql(Query(dc.server_type, None, val), conn)
            val = input('db>')
        conn.close()
        self.printer.output('Bye')
