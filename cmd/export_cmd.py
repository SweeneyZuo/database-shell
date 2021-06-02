from cmd.desc_cmd import DescCmd
from core.core import DatabaseType, Query


def get_print_template(out_format):
    if out_format == 'markdown':
        return '>\n> {}\n>\n\n'
    elif out_format in {'html', 'html2', 'html3', 'html4'}:
        return '<h1>{}</h1>\n'
    else:
        return '--\n-- {}\n--\n\n'


class ExportCmd(DescCmd):

    @property
    def export_type(self):
        return self.params['export_type']

    def exe(self):
        out_format, fold, export_type, printer = self.out_format, self.fold, self.export_type, self.printer
        server = self.get_server()
        dc = server.db_conf
        if dc.server_type is DatabaseType.MONGO:
            printer.print_error_msg("MongoDB does not support export option!")
            return
        conn = server.get_connection()
        try:
            tab_list = self._exe_query(server.get_list_tables_sql(server.db_conf.database), conn)[1]
            split_line, print_template = '\n---\n' if out_format == 'markdown' else '\n', get_print_template(out_format)
            attach_sql = export_type == 'ddl'
            for tab in tab_list[0]:
                if export_type in {'all', 'ddl'}:
                    printer.output(print_template.format(f'Table structure for {tab[0]}'))
                    self._print_table_schema(conn, Query(dc.server_type, dc.database, None, tab[0], fold), attach_sql)
                    printer.output(split_line)
                if export_type in {'all', 'data'}:
                    q = Query(dc.server_type, dc.database, server.get_scan_table_sql(tab[0]), tab[0], fold)
                    printer.output(print_template.format(f'Dumping data for {tab[0]}'), end='')
                    if dc.server_type is DatabaseType.SQL_SERVER and out_format == 'sql':
                        headers, results = self._exe_query(f'sp_columns [{tab[0]}];{q.sql}', conn)
                        set_identity_insert = len([1 for row in results[0] if row[5].endswith("identity")]) > 0
                        if set_identity_insert:
                            printer.output(f'SET IDENTITY_INSERT [{tab[0]}] ON;')
                        self.print_result_set(self._get_table_head_from_description(headers[1]), results[1], q)
                        if set_identity_insert:
                            printer.output(f'SET IDENTITY_INSERT [{tab[0]}] OFF;')
                    else:
                        self._run_sql(q, conn)
                    printer.output(split_line)
        except BaseException as be:
            self.write_error_history(export_type)
            printer.print_error_msg(be)
        finally:
            conn.close()
