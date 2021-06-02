import abc
import re
from datetime import datetime, timedelta

import core.print_utils as pu
from cmd.cmd import Cmd


def get_tab_name_from_sql(src_sql):
    """
     提取sql中的表名，sql语句不能有嵌套结构。
    """

    def deal_tab_name(tab_name):
        if '.' in tab_name:
            tab_name = tab_name.split('.')[-1]
        tab_name = tab_name.replace('[', '').replace(']', '')
        b_index = sql.index(tab_name)
        return src_sql[b_index:b_index + len(tab_name)]

    sql = src_sql.strip().lower()
    if sql.startswith(('select', 'delete')) and 'from' in sql:
        return deal_tab_name(re.split('\\s+', sql[sql.index('from') + 4:].strip())[0])
    elif sql.startswith('update'):
        return deal_tab_name(re.split('\\s+', sql[sql.index('update') + 6:].strip())[0])
    elif sql.startswith(('create', 'alter',)) or (sql.startswith('drop') and re.split('\\s+', sql)[1] == 'table'):
        return deal_tab_name(re.split('\\s+', sql[sql.index('table') + 5:].strip())[0])
    elif sql.startswith(('desc', 'describe')):
        return deal_tab_name(re.split('\\s+', sql)[1].strip())
    else:
        return None


class PrintCmd(Cmd):

    @property
    def printer(self):
        return self._params['printer']

    @property
    def out_format(self):
        return self.params['out_format']

    @property
    def human(self):
        return self.params['human']

    @property
    def limit_rows(self):
        return self.params['limit_rows']

    @property
    def columns(self):
        return self.params['columns']

    @property
    def fold(self):
        return self.params['fold']

    def deal_human(self, rows):
        def can_human(field_name):
            return 'time' in field_name

        human_time_cols = set(i for i in range(len(rows[0])) if can_human(str(rows[0][i]).lower()))
        for rdx, row in enumerate(rows):
            if rdx == 0:
                continue
            for cdx, ele in enumerate(row):
                if cdx in human_time_cols and isinstance(ele, int):
                    # 注意：时间戳被当成毫秒级时间戳处理，秒级时间戳格式化完是错误的时间
                    rows[rdx][cdx] = (datetime(1970, 1, 1) + timedelta(milliseconds=ele)).strftime("%Y-%m-%d %H:%M:%S")
        return rows

    def deal_res(self, res):
        for row in res:
            for cdx, e in enumerate(row):
                if isinstance(e, (bytearray, bytes)):
                    try:
                        row[cdx] = str(e, 'utf8')
                    except Exception:
                        row[cdx] = pu.HexObj(e.hex())
                elif isinstance(e, bool):
                    row[cdx] = 1 if e else 0

    def before_print(self, header, res, query):
        """
            需要注意不能改变原本数据的类型，除了需要替换成其它数据的情况
        """
        if res is None:
            return []
        if query.limit_rows:
            res = res[query.limit_rows[0]:query.limit_rows[1]]
        res = [row if isinstance(row, list) else list(row) for row in res]
        res.insert(0, header if isinstance(header, list) else list(header))
        res = [[line[i] for i in query.columns] for line in res] if query.columns else res
        self.deal_res(res)
        res = self.deal_human(res) if query.human else res
        fold_limit = self.params['fold_limit']
        print_conf = self.params['print_conf']
        res = [[e if len(str(e)) < fold_limit else
                f'{str(e)[:fold_limit - 3]}{print_conf.fold_replace_str_with_color}' for e in row] for row in
               res] if query.fold else res
        return res.pop(0), res

    def print_result_set(self, header, res, query, res_index=0):
        if not header:
            return

        printer = self.printer
        print_conf = self.params['print_conf']
        if not res and self.out_format == 'table':
            printer.print_warn_msg('Empty Sets!')
            return
        header, res = self.before_print(header, res, query)
        if self.out_format == 'table':
            pu.print_table(header, res, print_conf, printer,
                           start_func=lambda tw, mp: mp.print_info_msg(f'Result Sets [{res_index}]:'))
        elif self.out_format == 'sql' and query and query.server_type and query.sql:
            pu.print_insert_sql(header, res, query.table_name if query.table_name else get_tab_name_from_sql(query.sql),
                                query.server_type, printer)
        elif self.out_format == 'json':
            pu.print_json(header, res, printer)
        elif self.out_format == 'html':
            pu.print_html(header, res, printer)
        elif self.out_format == 'html2':
            pu.print_html2(header, res, printer)
        elif self.out_format == 'html3':
            pu.print_html3(header, res, printer)
        elif self.out_format == 'html4':
            pu.print_html4(header, res, printer)
        elif self.out_format == 'markdown':
            pu.print_markdown(header, res, print_conf, printer)
        elif self.out_format == 'xml':
            pu.print_xml(header, res, printer)
        elif self.out_format == 'csv':
            pu.print_csv(header, res, printer)
        elif self.out_format == 'text':
            pu.print_table(header, res, print_conf, printer, lambda a, b: a, lambda a, b, c, d, e, f: a,
                           lambda a, b, c: a)
        else:
            printer.print_error_msg(f'Invalid out format : "{self.out_format}"!')

    @abc.abstractmethod
    def exe(self):
        pass
