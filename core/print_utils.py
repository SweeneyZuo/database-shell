import os
import sys
import html
import json
from enum import Enum
from core.core import DatabaseType

__all__ = [
    'print_table', 'print_markdown', 'print_insert_sql', 'print_json', 'print_config',
    'print_html', 'print_html2', 'print_html3', 'print_html4', 'print_xml', 'print_csv'
]


class Color(Enum):
    OFF_WHITE = "\033[29;1m{}\033[0m"
    WHITE = "\033[30;1m{}\033[0m"
    RED = "\033[31;1m{}\033[0m"
    YELLOW_GREEN = "\033[32;1m{}\033[0m"
    # 土黄色
    KHAKI = "\033[33;1m{}\033[0m"
    BLUE = "\033[34;1m{}\033[0m"
    PURPLE = "\033[35;1m{}\033[0m"
    GREEN = "\033[36;1m{}\033[0m"
    GRAY = "\033[37;1m{}\033[0m"
    # 黑白闪烁
    BLACK_WHITE_TWINKLE = "\033[5;30;47m{}\033[0m"
    # 无颜色
    NO_COLOR = "{}"

    def wrap(self, v):
        if self is Color.NO_COLOR:
            return v
        return self.value.format(v)


class Align(Enum):
    ALIGN_LEFT = 1
    ALIGN_RIGHT = 2
    ALIGN_CENTER = 3


class HexObj:
    def __init__(self, hex_str):
        self.hex_str = f'0x{hex_str}'

    def __str__(self):
        return self.hex_str


class PrintConf:
    __slots__ = (
        '__info_color',
        '__show_bottom_threshold',
        '__split_row_char',
        '__table_head_color',
        '__data_color',
        '__null_str',
        '__null_str_color',
        '__null_str_with_color',
        '__null_str_with_color_len',
        '__fold_replace_str',
        '__fold_replace_str_with_color',
        '__fold_color',
        '__fold_color_len')

    def __init__(self,
                 show_bottom_threshold=150,
                 split_row_char='-',
                 table_head_color=Color.RED,
                 data_color=Color.NO_COLOR,
                 null_str='NULL',
                 null_str_color=Color.BLACK_WHITE_TWINKLE,
                 fold_replace_str='...',
                 fold_color=Color.BLACK_WHITE_TWINKLE):
        self.__show_bottom_threshold = show_bottom_threshold
        self.__split_row_char = split_row_char
        self.__table_head_color = table_head_color
        self.__data_color = data_color
        self.__null_str = null_str
        self.__null_str_color = null_str_color
        self.__null_str_with_color = self.__null_str_color.wrap(self.__null_str)
        self.__null_str_with_color_len = len(self.null_str)
        self.__fold_replace_str = fold_replace_str
        self.__fold_color = fold_color
        self.__fold_replace_str_with_color = fold_color.wrap(fold_replace_str)
        self.__fold_color_len = len(self.__fold_color.wrap(""))

    @property
    def split_row_char(self):
        return self.__split_row_char

    @property
    def table_head_color(self):
        return self.__table_head_color

    @property
    def data_color(self):
        return self.__data_color

    @property
    def null_str(self):
        return self.__null_str

    @property
    def null_str_with_color(self):
        return self.__null_str_with_color

    @property
    def null_str_with_color_len(self):
        return self.__null_str_with_color_len

    @property
    def fold_replace_str_with_color(self):
        return self.__fold_replace_str_with_color

    @property
    def fold_color_len(self):
        return self.__fold_color_len

    @property
    def show_bottom_threshold(self):
        return self.__show_bottom_threshold

    def disable_color(self):
        self.__data_color = Color.NO_COLOR
        self.__table_head_color = Color.NO_COLOR
        self.__null_str_color = Color.NO_COLOR
        self.__null_str_with_color = self.__null_str_color.wrap(self.__null_str)
        self.__fold_color = Color.NO_COLOR
        self.__fold_replace_str_with_color = self.__fold_color.wrap(self.__fold_replace_str)
        self.__fold_color_len = len(self.__fold_color.wrap(""))


widths = [
    (126, 1), (159, 0), (687, 1), (710, 0), (711, 1), (727, 0), (733, 1), (879, 0), (1154, 1), (1161, 0), (4347, 1),
    (4447, 2), (7467, 1), (7521, 0), (8369, 1), (8426, 0), (9000, 1), (9002, 2), (11021, 1), (12350, 2), (12351, 1),
    (12438, 2), (12442, 0), (19893, 2), (19967, 1), (55203, 2), (63743, 1), (64106, 2), (65039, 1), (65059, 0),
    (65131, 2), (65279, 1), (65376, 2), (65500, 1), (65510, 2), (120831, 1), (262141, 2), (1114109, 1),
]


class MsgPrinter:
    def __init__(self, info_color=Color.GREEN, warn_color=Color.KHAKI, error_color=Color.RED, normal_out=sys.stdout):
        self.__info_color = info_color
        self.__warn_color = warn_color
        self.__error_color = error_color
        self.__normal_out = normal_out

    def print_error_msg(self, msg, end='\n'):
        sys.stderr.write(f'{self.__error_color.wrap(msg)}{end}')

    def print_info_msg(self, msg, end='\n'):
        sys.stdout.write(f'{self.__info_color.wrap(msg)}{end}')

    def print_warn_msg(self, msg, end='\n'):
        sys.stdout.write(f'{self.__warn_color.wrap(msg)}{end}')

    def output(self, data, end='\n'):
        self.__normal_out.write(f'{data}{end}')

    def disable_info_color(self):
        self.__info_color = Color.NO_COLOR

    def disable_warn_color(self):
        self.__warn_color = Color.NO_COLOR

    def disable_error_color(self):
        self.__error_color = Color.NO_COLOR


def _calc_char_width(char):
    char_ord = ord(char)
    if char_ord == 0xe or char_ord == 0xf:
        return 0
    global widths
    for num, wid in widths:
        if char_ord <= num:
            return wid
    return 1


def _str_width(pc: PrintConf):
    def _inner_calc_width(any_str):
        if any_str == pc.null_str_with_color:
            return pc.null_str_with_color_len
        ln = -pc.fold_color_len if any_str.endswith(pc.fold_replace_str_with_color) else 0
        for char in any_str:
            ln += _calc_char_width(char)
        return ln

    return _inner_calc_width


def _get_max_length_each_fields(rows, before_calc_width, pc: PrintConf):
    length_head = len(rows[0]) * [0]
    calc_width_func = _str_width(pc)
    for row in rows:
        for cdx, e in enumerate(row):
            e = before_calc_width(e)
            row[cdx] = (e, calc_width_func(e))
            length_head[cdx] = max(row[cdx][1], length_head[cdx])
    return length_head


def _table_row_str(head_length, align_list, color=Color.NO_COLOR, split_char='|'):
    def _table_row_str_lazy(row):
        end_str = f' {split_char} '
        yield f'{split_char} '
        for e, width, align_type in zip(row, head_length, align_list):
            space_num = width - e[1]
            if space_num == 0:
                yield color.wrap(e[0])
            elif align_type == Align.ALIGN_RIGHT:
                yield f"{' ' * space_num}{color.wrap(e[0])}"
            elif align_type == Align.ALIGN_LEFT:
                yield f"{color.wrap(e[0])}{' ' * space_num}"
            else:
                half_space_num = space_num >> 1
                yield ' ' * half_space_num
                yield color.wrap(e[0])
                yield ' ' * (space_num - half_space_num)
            yield end_str

    return _table_row_str_lazy


def _default_after_print_row(row_num, row_total_num, max_row_length, table_width, pc: PrintConf, mp: MsgPrinter):
    if max_row_length > pc.show_bottom_threshold and row_num < row_total_num - 1:
        mp.output(pc.split_row_char * table_width)


def print_table(header, res, pc: PrintConf, mp: MsgPrinter,
                start_func=lambda table_width, mp: mp.print_info_msg('Result Sets:'),
                after_print_row_func=_default_after_print_row,
                end_func=lambda table_width, total, mp: mp.print_info_msg(f'Total Records: {total}')):
    def _deal_res():
        calc_width_func = _str_width(pc)
        for _row in res:
            for cdx, e in enumerate(_row):
                if isinstance(e, str):
                    e = e.replace('\r', '\\r').replace('\n', '\\n').replace('\t', '\\t') \
                        .replace('\0', '\\0').replace('\b', '\\b')
                elif isinstance(e, (int, float)):
                    # 数字采用右对齐
                    align_list[cdx], e = Align.ALIGN_RIGHT, str(e)
                elif e is None:
                    e = pc.null_str_with_color
                else:
                    e = str(e)
                _row[cdx] = (e, calc_width_func(e))
                max_length_each_fields[cdx] = max(_row[cdx][1], max_length_each_fields[cdx])

    row_total_num, col_total_num = len(res), len(header)
    res.insert(0, header)
    max_length_each_fields = col_total_num * [0]
    default_align = col_total_num * [Align.ALIGN_LEFT]
    align_list = default_align.copy()
    _deal_res()
    header = res.pop(0)
    # 数据的总长度
    max_row_length = sum(max_length_each_fields)
    # 表格的宽度(数据长度加上分割线)
    table_width = 1 + max_row_length + 3 * col_total_num
    space_list_down = [(pc.split_row_char * i, i) for i in max_length_each_fields]
    start_func(table_width, mp)
    print_data_func = _table_row_str(max_length_each_fields, align_list, pc.data_color)
    # 打印表格的上顶线
    s_line = ''.join(_table_row_str(max_length_each_fields, default_align, Color.NO_COLOR, '+')(space_list_down))
    mp.output(s_line)
    # 打印HEADER部分，和数据的对其方式保持一致
    mp.output(''.join(_table_row_str(max_length_each_fields, align_list, pc.table_head_color)(header)))
    # 打印HEADER和DATA之间的分割线
    mp.output(s_line)
    for row_num, row in enumerate(res):
        # 打印DATA部分
        mp.output(''.join(print_data_func(row)))
        after_print_row_func(row_num, row_total_num, max_row_length, table_width, pc, mp)
    if row_total_num > 0:
        # 有数据时，打印表格的下底线
        mp.output(s_line)
    end_func(table_width, row_total_num, mp)


def _deal_html_elem(e):
    if e is None:
        return "NULL"
    return html.escape(e).replace('\r\n', '<br>').replace('\0', '\\0').replace('\b', '\\b') \
        .replace('\n', '<br>').replace('\t', '&ensp;' * 4) if isinstance(e, str) else str(e)


def print_markdown(header, res, pc, mp):
    res.insert(0, header)
    max_length_each_fields = _get_max_length_each_fields(res, _deal_html_elem, pc)
    res.insert(1, map(lambda l: ('-' * l, l), max_length_each_fields))
    print_row_func = _table_row_str(max_length_each_fields, len(header) * [Align.ALIGN_LEFT], Color.NO_COLOR)
    for row in res:
        mp.output(''.join(print_row_func(row)))


def print_insert_sql(header, res, tab_name, server_type: DatabaseType, mp):
    def _case_for_sql(_row):
        for cdx, e in enumerate(_row):
            if e is None:
                yield "NULL"
            elif isinstance(e, (int, float, HexObj)):
                yield str(e)
            elif server_type is DatabaseType.MYSQL and isinstance(e, str):
                yield "'{}'".format(e.replace("'", "''").replace('\r', '\\r').replace('\n', '\\n')
                                    .replace('\t', '\\t').replace('\0', '\\0').replace('\b', '\\b'))
            else:
                yield f"'{e}'"

    if tab_name is None:
        mp.print_error_msg("Can't Get Table Name!")
        return
    if res:
        header = map(lambda x: server_type.escape_value(str(x)), header)
        insert_prefix = f"INSERT INTO {server_type.escape_value(tab_name)} ({','.join(header)}) VALUES"
        for row in res:
            mp.output(f"""{insert_prefix} ({','.join(_case_for_sql(row))});""")


def print_json(header, res, mp, human=False):
    indent = 2 if human else None
    for row in res:
        row = map(lambda e: e if isinstance(e, (str, int, float, list, dict, bool)) or e is None else str(e), row)
        mp.output(json.dumps({k: v for (k, v) in zip(header, row) if v}, indent=indent, ensure_ascii=False))


def print_config(path, mp):
    with open(os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), 'config/', path)) as html_head:
        mp.output(''.join(html_head.readlines()), end='')


def _print_header_with_html(header, mp):
    mp.output(f"""<tr>{''.join(map(lambda head: f"<th>{'' if head is None else head}</th>", header))}</tr>""")


def print_html3(header, res, mp):
    print_config('html/html1.head', mp)
    _print_header_with_html(header, mp)
    for row in res:
        mp.output(f"""<tr>{''.join(map(lambda e: f"<td>{_deal_html_elem(e)}</td>", row))}</tr>""")
    mp.output("</table>\n</body>\n</html>")


def print_html2(header, res, mp):
    print_config('html/html2.head', mp)
    _print_header_with_html(header, mp)
    for rdx, row in enumerate(res):
        mp.output(
            f"""<tr{'' if rdx % 2 == 0 else ' class="alt"'}>{''.join(map(lambda o: f"<td>{_deal_html_elem(o)}</td>", row))}</tr>""")
    mp.output("</table>\n</body>\n</html>")


def print_html(header, res, mp):
    print_config('html/html3.head', mp)
    _print_header_with_html(header, mp)
    s = '<tr onmouseover="this.style.backgroundColor=\'#ffff66\';"onmouseout="this.style.backgroundColor=\'#d4e3e5\';">'
    for row in res:
        mp.output(f"""{s}{''.join(map(lambda e: f"<td>{_deal_html_elem(e)}</td>", row))}</tr>""")
    mp.output("</table>")


def print_html4(header, res, mp):
    print_config('html/html4.head', mp)
    mp.output(
        f"""<thead><tr>{''.join(map(lambda head: f"<th>{'' if head is None else head}</th>", header))}</tr></thead>""")
    mp.output('<tbody>')
    for row in res:
        mp.output(f"""<tr>{''.join(map(lambda e: f'<td>{_deal_html_elem(e)}</td>', row))}</tr>""")
    mp.output("</tbody>\n</table>")


def print_xml(hd, res, mp, human=False):
    end, d = ('\n', " " * 4) if human else ('', '')
    record_template = f'<RECORD>{end}{{}}{end}</RECORD>'
    for row in res:
        mp.output(record_template.format(
            f"""{end.join([f"{d}<{h}/>" if e is None else f"{d}<{h}>{e}</{h}>" for h, e in zip(hd, row)])}"""))


def print_csv(header, res, mp, split_char=','):
    res.insert(0, header)
    for row in res:
        new_row = []
        for data in row:
            print_data = "" if data is None else str(data)
            if split_char in print_data or '\n' in print_data or '\r' in print_data:
                print_data = f'''"{print_data.replace('"', '""')}"'''
            new_row.append(print_data)
        mp.output(split_char.join(new_row))
