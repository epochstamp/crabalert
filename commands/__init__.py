from commands.reboot import Reboot
from .list_wallets import ListWallets
from .shutdown import Shutdown
from .reboot import Reboot
from .register import Register
from .command_err_handler import CommandErrHandler
from .duration_trial import DurationTrial
from .alive import Alive

commands = {
    "shutdown": Shutdown,
    "reboot": Reboot,
    "register": Register,
    "command_err_handler": CommandErrHandler,
    "list_wallets": ListWallets,
    "duration_trial": DurationTrial,
    "alive": Alive
}
