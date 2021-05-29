import os
import sys
import html
import json
from enum import Enum
from databasecore import DatabaseType


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


class HexObj(object):
    def __init__(self, hex_str):
        self.hex_str = f'0x{hex_str}'

    def __str__(self):
        return self.hex_str


class PrintConf():
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
                 info_color=Color.GREEN,
                 show_bottom_threshold=150,
                 split_row_char='-',
                 table_head_color=Color.RED,
                 data_color=Color.NO_COLOR,
                 null_str='NULL',
                 null_str_color=Color.BLACK_WHITE_TWINKLE,
                 fold_replace_str='...',
                 fold_color=Color.BLACK_WHITE_TWINKLE):
        self.__info_color = info_color
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
    def info_color(self):
        return self.__info_color

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
        self.__info_color = Color.NO_COLOR
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


def print_error_msg(msg, end='\n'):
    sys.stderr.write(f'{Color.RED.wrap(msg)}{end}')


def calc_char_width(char):
    char_ord = ord(char)
    if char_ord == 0xe or char_ord == 0xf:
        return 0
    global widths
    for num, wid in widths:
        if char_ord <= num:
            return wid
    return 1


def str_width(any_str, pc: PrintConf):
    if any_str == pc.null_str_with_color:
        return pc.null_str_with_color_len
    l = -pc.fold_color_len if any_str.endswith(pc.fold_replace_str_with_color) else 0
    for char in any_str:
        l += calc_char_width(char)
    return l


def get_max_length_each_fields(rows, func, pc: PrintConf):
    length_head = len(rows[0]) * [0]
    for row in rows:
        for cdx, e in enumerate(row):
            row[cdx] = (e, func(e, pc))
            length_head[cdx] = max(row[cdx][1], length_head[cdx])
    return length_head


def table_row_str(row, head_length, align_list, color=Color.NO_COLOR, split_char='|'):
    def _table_row_str():
        end_str = f' {split_char} '
        yield f'{split_char} '
        for e, width, align_type in zip(row, head_length, align_list):
            space_num = abs(e[1] - width)
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

    return ''.join(_table_row_str())


def default_after_print_row(row_num, row_total_num, max_row_length, table_width, pc: PrintConf):
    if max_row_length > pc.show_bottom_threshold and row_num < row_total_num - 1:
        print(pc.split_row_char * table_width)


def print_table(header, res, pc: PrintConf,
                start_func=lambda table_width, pc: print(pc.info_color.wrap('Result Sets:')),
                after_print_row_func=default_after_print_row,
                end_func=lambda table_width, total, pc: print(pc.info_color.wrap(f'Total Records: {total}'))):
    def _deal_res(_res, _align_list):
        for row in _res:
            for cdx, e in enumerate(row):
                if isinstance(e, str):
                    row[cdx] = e.replace('\r', '\\r').replace('\n', '\\n').replace('\t', '\\t') \
                        .replace('\0', '\\0').replace('\b', '\\b')
                elif isinstance(e, (int, float)):
                    # 数字采用右对齐
                    _align_list[cdx], row[cdx] = Align.ALIGN_RIGHT, str(e)
                elif e is None:
                    row[cdx] = pc.null_str_with_color
                else:
                    row[cdx] = str(e)
        return _res, _align_list

    row_total_num, col_total_num = len(res), len(header)
    res.insert(0, header)
    default_align = col_total_num * [Align.ALIGN_LEFT]
    res, align_list = _deal_res(res, default_align.copy())
    max_length_each_fields = get_max_length_each_fields(res, str_width, pc)
    header = res.pop(0)
    # 数据的总长度
    max_row_length = sum(max_length_each_fields)
    # 表格的宽度(数据长度加上分割线)
    table_width = 1 + max_row_length + 3 * col_total_num
    space_list_down = [(pc.split_row_char * i, i) for i in max_length_each_fields]
    start_func(table_width, pc)
    # 打印表格的上顶线
    print(table_row_str(space_list_down, max_length_each_fields, default_align, Color.NO_COLOR, '+'))
    # 打印HEADER部分，和数据的对其方式保持一致
    print(table_row_str(header, max_length_each_fields, align_list, pc.table_head_color))
    # 打印HEADER和DATA之间的分割线
    print(table_row_str(space_list_down, max_length_each_fields, default_align, Color.NO_COLOR, '+'))
    for row_num, row in enumerate(res):
        # 打印DATA部分
        print(table_row_str(row, max_length_each_fields, align_list, pc.data_color))
        after_print_row_func(row_num, row_total_num, max_row_length, table_width, pc)
    if res:
        # 有数据时，打印表格的下底线
        print(table_row_str(space_list_down, max_length_each_fields, default_align, Color.NO_COLOR, '+'))
    end_func(table_width, row_total_num, pc)


def deal_html_elem(e):
    if e is None:
        return "NULL"
    return html.escape(e).replace('\r\n', '<br>').replace('\0', '\\0').replace('\b', '\\b') \
        .replace('\n', '<br>').replace('\t', '&ensp;' * 4) if isinstance(e, str) else str(e)


def print_markdown(header, res, pc: PrintConf):
    res.insert(0, header)
    res = [[deal_html_elem(e) for e in row] for row in res]
    max_length_each_fields, align_list = get_max_length_each_fields(res, str_width, pc), len(header) * [
        Align.ALIGN_LEFT]
    res.insert(1, map(lambda l: ('-' * l, l), max_length_each_fields))
    for row in res:
        print(table_row_str(row, max_length_each_fields, align_list, Color.NO_COLOR))


def print_insert_sql(header, res, tab_name, server_type: DatabaseType):
    def _case_for_sql(row):
        for cdx, e in enumerate(row):
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
        print_error_msg("Can't get table name!")
        return
    if res:
        header = map(lambda x: server_type.value.escape_value(str(x)), header)
        insert_prefix = f"INSERT INTO {server_type.value.escape_value(tab_name)} ({','.join(header)}) VALUES"
        for row in res:
            print(f"""{insert_prefix} ({','.join(_case_for_sql(row))});""")


def print_json(header, res):
    for row in res:
        row = map(lambda e: e if isinstance(e, (str, int, float, list, dict, bool)) or e is None else str(e), row)
        print(json.dumps({k: v for (k, v) in zip(header, row) if v}, indent=2, ensure_ascii=False))


def print_config(path):
    with open(os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), 'config/', path)) as html_head:
        print(''.join(html_head.readlines()), end='')


def print_header_with_html(header):
    print(f"""<tr>{''.join(map(lambda head: f"<th>{'' if head is None else head}</th>", header))}</tr>""")


def print_html3(header, res):
    print_config('html/html1.head')
    print_header_with_html(header)
    for row in res:
        print(f"""<tr>{''.join(map(lambda e: f"<td>{deal_html_elem(e)}</td>", row))}</tr>""")
    print("</table>\n</body>\n</html>")


def print_html2(header, res):
    print_config('html/html2.head')
    print_header_with_html(header)
    for rdx, row in enumerate(res):
        print(
            f"""<tr{'' if rdx % 2 == 0 else ' class="alt"'}>{''.join(map(lambda o: f"<td>{deal_html_elem(o)}</td>", row))}</tr>""")
    print("</table>\n</body>\n</html>")


def print_html(header, res):
    print_config('html/html3.head')
    print_header_with_html(header)
    s = '<tr onmouseover="this.style.backgroundColor=\'#ffff66\';"onmouseout="this.style.backgroundColor=\'#d4e3e5\';">'
    for row in res:
        print(f"""{s}{''.join(map(lambda e: f"<td>{deal_html_elem(e)}</td>", row))}</tr>""")
    print("</table>")


def print_html4(header, res):
    print_config('html/html4.head')
    print(
        f"""<thead><tr>{''.join(map(lambda head: f"<th>{'' if head is None else head}</th>", header))}</tr></thead>""")
    print('<tbody>')
    for row in res:
        print(f"""<tr>{''.join(map(lambda e: f'<td>{deal_html_elem(e)}</td>', row))}</tr>""")
    print("</tbody>\n</table>")


def print_xml(header, res):
    end, intend = '\n', " " * 4
    for row in res:
        print(
            f"""<RECORD>{end}{end.join([f"{intend}<{h}/>" if e is None else f"{intend}<{h}>{e}</{h}>" for h, e in zip(header, row)])}{end}</RECORD>""")


def print_csv(header, res, split_char=','):
    res.insert(0, header)
    for row in res:
        new_row = []
        for data in row:
            print_data = "" if data is None else str(data)
            if split_char in print_data or '\n' in print_data or '\r' in print_data:
                print_data = '"{}"'.format(print_data.replace('"', '""'))
            new_row.append(print_data)
        print(split_char.join(new_row))