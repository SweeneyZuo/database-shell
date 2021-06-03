import sys
from collections.abc import Iterable

from cmd.conf_cmd import ConfCmd
from core.core import DatabaseType, Query, DatabaseConf, ServerFactory


class SqlCmd(ConfCmd):
    name = 'sql'

    def check_conf(self, db_conf: dict):
        if db_conf['use']['conf'] == ConfCmd.DEFAULT_CONF:
            self.printer.print_error_msg('Please Set conf!')
            return False
        conf_keys = db_conf['conf'][db_conf['use']['env']][db_conf['use']['conf']].keys()
        for conf in ConfCmd.CONFIG[2:8]:
            if conf not in conf_keys:
                self.printer.print_error_msg(f'Please Set {conf}!')
                return False
        return True

    def get_server(self):
        info = self._read_info()
        if self.out_format == 'table':
            self.show_database_info(info)
        if not self.check_conf(info):
            self.write_error_history('Conf Error')
            sys.exit(-1)
        dc = DatabaseConf(info['conf'][info['use']['env']].get(info['use']['conf'], {}))
        try:
            return ServerFactory.get_server(dc)
        except BaseException as e:
            self.printer.print_error_msg(e)
            self.write_error_history("Can't Create Server")
            sys.exit(-1)

    def _deal_mongo_result(self, mongo_result):
        if mongo_result is None:
            return None, None
        header_list, result_list = [], []
        if isinstance(mongo_result, dict):
            result_list.append([])
            for (k, value) in mongo_result.items():
                header_list.append(k)
                result_list[0].append(value)
        elif isinstance(mongo_result, Iterable) and not isinstance(mongo_result, str):
            t_set, result_dict_list = set(), []
            for d in mongo_result:
                if isinstance(d, dict):
                    result_dict_list.append(d)
                    for (k, value) in d.items():
                        if k not in t_set:
                            t_set.add(k)
                            header_list.append(k)
                else:
                    result_list.append([d])
            if result_list:
                header_list.append(f"result({type(result_list[0][0]).__name__})")
            for d in result_dict_list:
                result_list.append([d.get(h, None) for h in header_list])
        else:
            header_list.append(f'result({type(mongo_result).__name__})')
            result_list.append([mongo_result])
        return header_list, result_list

    def _get_table_head_from_description(self, description):
        return [desc[0] for desc in description] if description else []

    def exe_mongo(self, conn, query):
        if conn is None:
            return
        db = conn[query.database]
        try:
            res = eval(query.sql)
            header_list, result_list = self._deal_mongo_result(res)
            self.print_result_set(header_list, result_list, query)
            self.write_ok_history(query.sql)
        except Exception as e:
            self.write_error_history(query.sql)
            self.printer.print_error_msg(e)

    def _exe_query(self, sql, conn):
        res_list, description = [], []
        try:
            cur = conn.cursor()
            cur.execute(sql)
            description.append(cur.description)
            res_list.append(cur.fetchall())
            while cur.nextset():
                description.append(cur.description)
                res_list.append(cur.fetchall())
            self.write_ok_history(sql)
        except BaseException as be:
            self.write_error_history(sql)
            self.printer.print_error_msg(be)
        return description, res_list

    def _exe_no_query(self, sql, conn):
        effect_rows, description, res, success = None, [], [], False
        try:
            if sql.lower().startswith(('create', 'drop')):
                conn.autocommit(True)
            cur = conn.cursor()
            effect_rows = cur.execute(sql)
            description.append(cur.description)
            try:
                res.append(cur.fetchall())
                while cur.nextset():
                    description.append(cur.description)
                    res.append(cur.fetchall())
                conn.commit()
            except Exception:
                if not effect_rows and self.out_format == 'table':
                    self.printer.print_warn_msg('Empty Sets!')
            success = True
            self.write_ok_history(sql)
        except BaseException as be:
            self.write_error_history(sql)
            self.printer.print_error_msg(be)
        return effect_rows, description, res, success

    def _run_sql(self, query, conn):
        sql, description, res, effect_rows = query.sql.strip(), None, None, None
        if query.server_type is DatabaseType.MONGO:
            self.exe_mongo(conn, query)
            return
        if sql.lower().startswith(('select', 'show')):
            description, res = self._exe_query(sql, conn)
        else:
            effect_rows, description, res, success = self._exe_no_query(sql, conn)
        if description and res:
            for index, (d, r) in enumerate(zip(description, res)):
                self.print_result_set(self._get_table_head_from_description(d), r, query, index)
                self.printer.output('\n', end='')
        if effect_rows and self.out_format == 'table':
            self.printer.print_info_msg(f'Effect Rows:{effect_rows}')

    def exe(self):
        server = self.get_server()
        dc, conn = server.db_conf, server.get_connection()
        self._run_sql(Query(dc.server_type, dc.database, self.params['option_val'], None,
                            self.fold, self.columns, self.limit_rows, self.human), conn)
        conn.close()
