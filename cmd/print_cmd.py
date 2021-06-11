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


def deal_res(res):
    for row in res:
        for cdx, e in enumerate(row):
            if isinstance(e, (bytearray, bytes)):
                try:
                    row[cdx] = str(e, 'utf8')
                except Exception:
                    row[cdx] = pu.HexObj(e.hex())
            elif isinstance(e, bool):
                row[cdx] = 1 if e else 0


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

    def before_print(self, header, res, query):
        """
            需要注意不能改变原本数据的类型，除了需要替换成其它数据的情况
        """
        if res is None:
            return []
        new_res = []
        fold_limit = self.params['fold_limit']
        fold_str, end_pos = self.params['print_conf'].fold_replace_str_with_color, fold_limit - 3
        header = header if isinstance(header, list) else list(header)
        header = [header[i] for i in query.columns] if query.columns else header
        human_time_cols = set(
            i for i in range(len(header)) if 'time' in str(header[i]).lower()) if query.human else set()
        for rdx, row in enumerate(res):
            if query.limit_rows and (query.limit_rows[0] > rdx or rdx >= query.limit_rows[1]):
                continue
            row = row if isinstance(row, list) or query.columns else list(row)
            row = [row[i] for i in query.columns] if query.columns else row
            for cdx, e in enumerate(row):
                if isinstance(e, (bytearray, bytes)):
                    try:
                        e = str(e, 'utf8')
                    except Exception:
                        e = pu.HexObj(e.hex())
                elif isinstance(e, bool):
                    e = 1 if e else 0
                elif isinstance(e, int) and cdx in human_time_cols:
                    # 注意：时间戳被当成毫秒级时间戳处理，秒级时间戳格式化完是错误的时间
                    e = (datetime(1970, 1, 1) + timedelta(milliseconds=e)).strftime("%Y-%m-%d %H:%M:%S")
                if query.fold:
                    str_e = str(e)
                    e = f'{str_e[:end_pos]}{fold_str}' if len(str_e) > fold_limit else e
                row[cdx] = e
            new_res.append(row)
        return header, new_res

    def print_result_set(self, header, res, query, res_index=0):
        mp, pc, out_format = self.printer, self.params['print_conf'], self.out_format
        if not res and out_format == 'table':
            mp.print_warn_msg('Empty Sets!')
            return
        header, res = self.before_print(header, res, query)
        if out_format == 'table':
            pu.print_table(header, res, pc, mp, lambda t, p: p.print_info_msg(f'Result Sets [{res_index}]:'))
        elif out_format == 'sql' and query and query.server_type and query.sql:
            pu.print_insert_sql(header, res, query.table_name if query.table_name else get_tab_name_from_sql(query.sql),
                                query.server_type, mp)
        elif out_format == 'json':
            pu.print_json(header, res, mp)
        elif out_format == 'html':
            pu.print_html(header, res, mp)
        elif out_format == 'html2':
            pu.print_html2(header, res, mp)
        elif out_format == 'html3':
            pu.print_html3(header, res, mp)
        elif out_format == 'html4':
            pu.print_html4(header, res, mp)
        elif out_format == 'markdown':
            pu.print_markdown(header, res, pc, mp)
        elif out_format == 'xml':
            pu.print_xml(header, res, mp)
        elif out_format == 'csv':
            pu.print_csv(header, res, mp)
        elif out_format == 'text':
            pu.print_table(header, res, pc, mp, lambda a, b: a, lambda a, b, c, d, e, f: a, lambda a, b, c: a)
        else:
            mp.print_error_msg(f'Invalid Out Format : "{out_format}"!')

    @abc.abstractmethod
    def exe(self):
        pass
