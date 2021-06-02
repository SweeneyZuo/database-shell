import platform
import re
import sys

from cmd.cmd import Cmd
from core import print_utils as pu

PRINT_CONF = pu.PrintConf()
PRINTER = pu.MsgPrinter()
PRINT_FORMAT_SET = {'table', 'text', 'json', 'sql', 'html', 'html2', 'html3', 'html4', 'markdown', 'xml', 'csv'}
FOLD_LIMIT = 50
params = {
    'printer': PRINTER,
    'print_conf': PRINT_CONF,
    'fold_limit': FOLD_LIMIT,
    'fold': True,
    'human': False,
    'columns': None,
    'limit_rows': None,
    'export_type': 'all',
    'out_format': 'table'
}


def disable_color():
    global PRINT_CONF, PRINTER
    PRINT_CONF.disable_color()
    PRINTER.disable_info_color()
    PRINTER.disable_warn_color()


def error_param_exit(param):
    PRINTER.print_error_msg(f'Invalid Param : "{param}"!')
    sys.exit(-1)


def parse_columns(limit_column_re):
    group2 = limit_column_re.group(2).strip()
    if ',' in group2 or group2.isdigit():
        return [int(i.strip()) for i in group2.split(',')]
    else:
        t = [int(i.strip()) for i in group2.split('-')]
        return [i for i in range(int(t[0]), int(t[1]) + 1)] if int(t[0]) <= int(t[1]) \
            else [i for i in range(int(t[0]), int(t[1]) - 1, -1)]


def parse_args(args):
    set_format = set_human = set_export_type = set_columns = set_fold = set_raw = set_row_limit = set_show_obj = False
    option = args[1].strip().lower() if len(args) > 1 else 'info'
    option = {'hist': 'history'}.get(option, option)
    params['option_val'], parse_start_pos = args[2] if len(args) > 2 else '', 3 if option == 'sql' else 2
    if len(args) > parse_start_pos:
        for index in range(parse_start_pos, len(args)):
            p = args[index].strip().lower()
            limit_row_re = re.match("^(row)\[\s*(-?\d+)\s*:\s*(-?\d+)\s*(\])$", p)
            limit_column_re = re.match("^(col)\[((\s*\d+\s*-\s*\d+\s*)|(\s*(\d+)\s*(,\s*(\d+)\s*)*))(\])$", p)
            if option in {'info', 'shell', 'help', 'test', 'version'}:
                error_param_exit(p)
            elif index == 2 and option in {'sql', 'scan', 'peek', 'count', 'desc', 'load', 'set', 'lock', 'unlock'}:
                continue  # 第3个参数可以自定义输入的操作
            elif option == 'show':
                if p not in {'database', 'table', 'databases', 'tables', 'view', 'views'} or set_show_obj:
                    error_param_exit(p)
                set_show_obj = set_fold = set_raw = set_row_limit = set_human = set_columns = set_format = True
            elif not set_fold and p == 'false':
                params['fold'], set_fold = False, True
            elif not set_row_limit and option != 'export' and limit_row_re:
                set_row_limit, params['limit_rows'] = True, (int(limit_row_re.group(2)), int(limit_row_re.group(3)))
            elif not set_columns and option != 'export' and limit_column_re:
                params['columns'], set_columns = parse_columns(limit_column_re), True
            elif not set_format and option in {'export', 'sql', 'scan', 'peek', 'desc', 'history'} and \
                    p in PRINT_FORMAT_SET:
                if option != 'table':
                    disable_color()
                else:
                    params['fold'] = False
                params['out_format'], set_format = p, True
            elif not set_raw and params['out_format'] == 'table' and p == 'raw':
                set_raw = True
                disable_color()
            elif not set_export_type and option == 'export' and p in {'ddl', 'data', 'all'}:
                params['export_type'], set_export_type = p, True
            elif not set_human and p == 'human':
                params['human'], set_human = True, True
            else:
                error_param_exit(p)
    return option


if __name__ == '__main__':
    if platform.system().lower() == 'windows':
        disable_color()
    try:
        opt = parse_args(sys.argv)
        cmd = Cmd.get(opt)
        if cmd:
            cmd(opt=opt, **params).exe()
        else:
            print("Invalid Operation!")
    except Exception as ex:
        PRINTER.print_error_msg(ex)
        import traceback

        traceback.print_exc(chain=ex)
