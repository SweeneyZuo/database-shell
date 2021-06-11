import fcntl
import os

from cmd.conf_cmd import ConfCmd
from core.core import DatabaseType


class SetCmd(ConfCmd):
    name = 'set'

    def set_info(self, _read_info, set_info_obj):
        if set_info_obj:
            for conf_key in set_info_obj.keys():
                if conf_key not in ConfCmd.CONFIG:
                    self.printer.print_error_msg(f'Invalid Key "{conf_key}"!')
                    continue
                set_conf_value = set_info_obj[conf_key]
                if conf_key == 'env':
                    _read_info['use']['env'] = set_conf_value
                    if set_conf_value not in _read_info['conf'].keys():
                        _read_info['conf'][set_conf_value] = {}
                    _read_info['use']['conf'] = ConfCmd.DEFAULT_CONF
                    self.printer.print_info_msg(f"Set env={set_conf_value} OK.")
                    continue
                elif conf_key == 'conf':
                    if set_conf_value in _read_info['conf'][_read_info['use']['env']].keys():
                        _read_info['use']['conf'] = set_conf_value
                        self.printer.print_info_msg(f"set conf={set_conf_value} OK.")
                    else:
                        i = input("Add Configuration: {}? \nY/N:".format(set_conf_value)).lower()
                        if i in ('y', 'yes'):
                            _read_info['use']['conf'] = set_conf_value
                            _read_info['conf'][_read_info['use']['env']][_read_info['use']['conf']] = {}
                            self.printer.print_info_msg(
                                f'Add "{set_conf_value}" conf in env={_read_info["use"]["env"]}')
                    continue
                if _read_info['use']['conf'] == ConfCmd.DEFAULT_CONF:
                    self.printer.print_error_msg('Please Set conf!')
                    continue
                if conf_key == 'servertype' and not DatabaseType.support(set_conf_value):
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
                _read_info['conf'][_read_info['use']['env']][_read_info['use']['conf']][conf_key] = set_conf_value
                self.printer.print_info_msg(f"Set {conf_key}={set_conf_value} OK.")
        return _read_info

    def exe(self):
        kv = self.params['option_val']
        if '=' not in kv:
            self.printer.print_error_msg(f'Invalid Params "{kv}"!')
            self.write_error_history(kv)
            return
        set_info_obj = {}
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
                set_info_obj[kv_pair[0].lower()] = kv_pair[1]
                self._write_info(self.set_info(self._read_info(), set_info_obj))
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                self.write_ok_history(kv)
        except Exception as be:
            self.write_error_history(kv)
            self.printer.print_error_msg(be)
