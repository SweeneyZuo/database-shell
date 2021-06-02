import os

from cmd.sql_cmd import SqlCmd
from core.core import Query, DatabaseType


class LoadCmd(SqlCmd):
    name = 'load'

    def exe_sql(self, _cur, _query: Query):
        try:
            if not _query.sql:
                return 0, 0
            effect_rows = _cur.execute(_query.sql)
            description, res = [], []
            try:
                description.append(_cur.description)
                res.append(_cur.fetchall())
                while _cur.nextset():
                    description.append(_cur.description)
                    res.append(_cur.fetchall())
            except Exception:
                if effect_rows:
                    self.printer.print_warn_msg(f'Effect rows:{effect_rows}')
            for r, d in zip(res, description):
                self.print_result_set(self._get_table_head_from_description(d), r, _query)
            return 1, 0
        except Exception as be:
            self.printer.print_error_msg(f"SQL:{_query.sql}, ERROR MESSAGE:{be}")
            return 0, 1

    def exe(self):
        path = self.params['option_val']
        success_num, fail_num = 0, 0
        try:
            if os.path.exists(path):
                server = self.get_server()
                dc = server.db_conf
                if dc.server_type is DatabaseType.MONGO:
                    self.printer.print_error_msg("Not Support MongoDB!")
                    self.write_error_history('Not Support MongoDB')
                    return
                conn = server.get_connection()
                cur = conn.cursor()
                with open(path, mode='r', encoding='UTF8') as sql_file:
                    sql = ''
                    for line in sql_file.readlines():
                        t_sql = line.strip()
                        if not t_sql or t_sql.startswith('--'):
                            continue
                        elif t_sql.lower().startswith(('insert', 'create', 'update', 'select', 'delete', 'alter', 'set',
                                                       'drop', 'go', 'use', 'if', 'with', 'show', 'desc', 'grant',
                                                       'exec')):
                            if sql == '':
                                sql = line
                                continue
                            success, fail = self.exe_sql(cur, Query(dc.server_type, None, sql, fold=False))
                            success_num += success
                            fail_num += fail
                            sql = line
                        else:
                            sql += line
                    if sql != '':
                        success, fail = self.exe_sql(cur, Query(dc.server_type, None, sql, fold=False))
                        success_num += success
                        fail_num += fail
                if success_num > 0:
                    conn.commit()
                cur.close()
                conn.close()
                self.printer.print_info_msg(f'end load. {success_num} successfully executed, {fail_num} failed.')
                self.write_ok_history(path)
            else:
                self.printer.print_error_msg(f"path:{path} not exist!")
                self.write_error_history(path)
        except Exception as be:
            self.printer.print_error_msg(be)
            self.write_error_history(path)
