import fcntl
import os
from enum import Enum

from cmd.conf_cmd import ConfCmd
from core.core import DatabaseType


class Opt(Enum):
    UPDATE = 'update'
    WRITE = 'write'
    READ = 'read'


class SetCmd(ConfCmd):
    name = 'set'

    def parse_info_obj(self, _read_info, info_obj, opt=Opt.READ):
        if info_obj:
            for conf_key in ConfCmd.CONFIG:
                if conf_key not in info_obj.keys():
                    continue
                set_conf_value = info_obj[conf_key]
                if conf_key == 'env':
                    _read_info['use']['env'] = set_conf_value
                    if opt is Opt.UPDATE:
                        self.printer.print_info_msg(f"Set env={set_conf_value} OK.")
                    continue
                elif conf_key == 'conf':
                    if set_conf_value in _read_info['conf'][_read_info['use']['env']].keys():
                        if opt is Opt.UPDATE:
                            _read_info['use']['conf'] = set_conf_value
                            self.printer.print_info_msg(f"set conf={set_conf_value} ok.")
                    elif opt is Opt.UPDATE:
                        i = input("Add Configuration: {}? \nY/N:".format(set_conf_value)).lower()
                        if i in ('y', 'yes'):
                            _read_info['use']['conf'] = set_conf_value
                            _read_info['conf'][_read_info['use']['env']][_read_info['use']['conf']] = {}
                            self.printer.print_info_msg(
                                f'Add "{set_conf_value}" conf in env={_read_info["use"]["env"]}')
                    continue
                elif conf_key == 'servertype' and not DatabaseType.support(set_conf_value):
                    self.printer.print_error_msg(f'Server Type: "{set_conf_value}" Not Supported!')
                    continue
                elif conf_key == 'autocommit':
                    if set_conf_value.lower() not in {'true', 'false'}:
                        self.printer.print_error_msg(f'"{set_conf_value}" Incorrect Parameters!')
                        continue
                    set_conf_value = set_conf_value.lower() == 'true'
                elif conf_key == 'port':
                    if not set_conf_value.isdigit():
                        self.printer.print_error_msg(f'Set port={set_conf_value} Failed!')
                        continue
                    set_conf_value = int(set_conf_value)
                    self.printer.print_info_msg(f"Set {set_conf_value}={set_conf_value} OK.")
                _read_info['conf'][_read_info['use']['env']][_read_info['use']['conf']][conf_key] = set_conf_value
        return _read_info

    def exe(self):
        kv = self.params['option_val']
        info_obj = {}
        try:
            with open(os.path.join(self.get_proc_home(), 'config/.db.lock'), 'w') as lock:
                try:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    self.printer.print_error_msg("Set Failed, Please Retry!")
                    self.write_error_history(kv)
                    return
                if self.is_locked():
                    self.printer.print_error_msg("DB Is Locked!")
                    self.write_error_history(kv)
                    return
                kv_pair = kv.split('=', 1)
                info_obj[kv_pair[0].lower()] = kv_pair[1]
                self._write_info(self.parse_info_obj(self._read_info(), info_obj, Opt.UPDATE))
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                self.write_ok_history(kv)
        except Exception as be:
            self.write_error_history(kv)
            self.printer.print_error_msg(be)
