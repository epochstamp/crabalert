from .list_wallets import ListWallets
from .shutdown import Shutdown
from .register import Register
from .command_err_handler import CommandErrHandler

commands = {
    "shutdown": Shutdown,
    "register": Register,
    "command_err_handler": CommandErrHandler,
    "list_wallets": ListWallets
}
