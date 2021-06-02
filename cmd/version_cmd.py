import core.print_utils as pu
from cmd.history_cmd import HistoryCmd


class VersionCmd(HistoryCmd):
    def exe(self):
        pu.print_config('.db.version', self.printer)
        self.write_ok_history('')