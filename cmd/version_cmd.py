import core.print_utils as pu
from cmd.history_cmd import HistoryCmd


class VersionCmd(HistoryCmd):
    name = 'version'

    def exe(self):
        pu.print_config('.db.version', self.printer)
        self.write_ok_history('')
