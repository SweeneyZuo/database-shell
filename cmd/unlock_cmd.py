import fcntl
import hashlib
import os

from cmd.conf_cmd import ConfCmd


class UnlockCmd(ConfCmd):
    name = 'unlock'

    def exe(self):
        key = self.params['option_val']
        with open(os.path.join(self.get_proc_home(), 'config/.db.lock'), 'w') as file_lock:
            try:
                fcntl.flock(file_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                self.printer.print_error_msg("Unlock Failed, Please Retry!")
                self.write_error_history('*' * 6)
                return
            lock_val = self.lock_value()
            if not lock_val:
                self.printer.print_error_msg('The DB Is Not Locked.')
                self.write_error_history('*' * 6)
                return
            key = key if key else ""
            m = hashlib.md5()
            m.update(key.encode('UTF-8'))
            if m.hexdigest() == lock_val:
                os.remove(os.path.join(self.get_proc_home(), 'config/.db.lock.value'))
                self.printer.print_info_msg('DB Unlocked!')
                self.write_ok_history('*' * 6)
            else:
                self.printer.print_error_msg('Incorrect Key!')
                self.write_error_history(key)
            fcntl.flock(file_lock.fileno(), fcntl.LOCK_UN)
